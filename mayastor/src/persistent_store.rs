//! The persistent store is used to save information that is required by
//! Mayastor across restarts.
//!
//! etcd is used as the backing store and is interfaced with through the use of
//! the etcd-client crate. This crate has a dependency on the tokio async
//! runtime, therefore store operations need to be dispatched to a tokio thread.
use crate::{
    core::Reactors,
    store::{
        etcd::Etcd,
        store_defs::{
            DeleteWait,
            GetWait,
            PutWait,
            Store,
            StoreError,
            StoreKey,
            StoreValue,
        },
    },
};
use futures::{
    channel::{mpsc, mpsc::Receiver, oneshot},
    StreamExt,
};
use once_cell::sync::OnceCell;
use serde_json::Value;
use snafu::ResultExt;
use std::{thread::sleep, time::Duration};

static ETCD_ENDPOINT: &str = "0.0.0.0:2379";
static PERSISTENT_STORE: OnceCell<Option<PersistentStore>> = OnceCell::new();

type TaskReceiver = oneshot::Receiver<Result<Option<Value>, StoreError>>;

/// Persistent store
pub struct PersistentStore {
    /// Backing store used for persistence.
    store: Etcd,
    /// Sender channel used to send tasks to the worker thread.
    sender: mpsc::Sender<StoreTask>,
}

impl PersistentStore {
    /// Start the persistent store.
    /// This function must be called by the tokio runtime, because the work loop
    /// executes using the etcd-client which requires tokio.
    pub async fn run(endpoint: Option<String>) -> Result<(), ()> {
        if endpoint.is_none() {
            // No endpoint means no persistent store.
            warn!("Persistent store not initialised");
            PERSISTENT_STORE.get_or_init(|| None);
            return Ok(());
        }

        match PersistentStore::connect_to_backing_store(
            &endpoint.clone().unwrap(),
        )
        .await
        {
            Some(etcd) => {
                // Initialise the persistent store.
                let (sender, mut receiver) = mpsc::channel::<StoreTask>(0);
                PERSISTENT_STORE.get_or_init(|| {
                    Some(PersistentStore {
                        store: etcd,
                        sender,
                    })
                });
                // Run the work loop which executes store tasks.
                PersistentStore::work_loop(&mut receiver).await;
            }
            None => {
                // If the store cannot be connected to, we cannot run.
                panic!(
                    "Failed to connect to etcd on endpoint {}",
                    endpoint.unwrap()
                );
            }
        }
        Ok(())
    }

    async fn connect_to_backing_store(endpoint: &str) -> Option<Etcd> {
        let mut retries = 3;
        while retries > 0 {
            match Etcd::new(endpoint).await {
                Ok(store) => return Some(store),
                Err(_) => {
                    retries -= 1;
                    sleep(Duration::from_secs(1));
                }
            }
        }
        None
    }

    /// Put a key-value in the store.
    pub async fn put(
        key: &impl StoreKey,
        value: &impl StoreValue,
    ) -> Result<(), StoreError> {
        let put_value = serde_json::to_value(value)
            .expect("Failed to convert value to a serde_json value");
        let r = PersistentStore::dispatch_task(
            StoreOp::Put,
            key,
            Some(put_value.clone()),
        )
        .await;
        let result = r.await.context(PutWait {
            key: key.to_string(),
            value: put_value,
        })?;
        result.map(|_| ())
    }

    /// Retrieve a value, with the given key, from the store.
    pub async fn get(key: &impl StoreKey) -> Result<Value, StoreError> {
        let r = PersistentStore::dispatch_task(StoreOp::Get, key, None).await;
        r.await
            .context(GetWait {
                key: key.to_string(),
            })?
            .map(|r| r.unwrap())
    }

    /// Delete the entry in the store with the given key.
    pub async fn delete(key: &impl StoreKey) -> Result<(), StoreError> {
        let r =
            PersistentStore::dispatch_task(StoreOp::Delete, key, None).await;
        r.await
            .context(DeleteWait {
                key: key.to_string(),
            })?
            .map(|_| ())
    }

    /// Send task to the worker thread and return a receiver channel which can
    /// be waited on for task completion.
    async fn dispatch_task(
        op: StoreOp,
        key: &impl ToString,
        value: Option<Value>,
    ) -> TaskReceiver {
        assert!(PersistentStore::store().is_some());
        let (s, r) = oneshot::channel::<Result<Option<Value>, StoreError>>();
        PersistentStore::store()
            .as_ref()
            .unwrap()
            .sender
            .clone()
            .start_send(StoreTask {
                operation: op,
                key: key.to_string(),
                value,
                sender: s,
            })
            .expect("Failed to dispatch work");
        r
    }
    /// Determine if the persistent store has been enabled.
    pub fn enabled() -> bool {
        PERSISTENT_STORE.get().is_some()
    }

    /// Get an immutable reference to the store.
    fn store() -> &'static Option<PersistentStore> {
        PERSISTENT_STORE.get().unwrap()
    }

    /// Worker thread which executes the tasks sent to it.
    /// This MUST be executed on a tokio thread as that is what is required by
    /// the etcd-client crate.
    async fn work_loop(receiver: &mut Receiver<StoreTask>) {
        assert!(PersistentStore::store().is_some());
        loop {
            let task = receiver.next().await.unwrap();
            let mut store =
                PersistentStore::store().as_ref().unwrap().store.clone();

            // Once an operation has been performed on the store, send the
            // result back on the provided channel. This has to be performed by
            // sending a future to our reactor as trying to send it directly
            // from the tokio thread doesn't work - the receiver side never
            // receives the message.
            //
            // If the task sender cannot be notified of completion, the error is
            // logged and we continue to wait for the next task. We cannot do
            // anything else here.
            match task.operation {
                StoreOp::Put => {
                    let result = store.put_kv(&task.key, &task.value).await;
                    Reactors::master().send_future(async move {
                        if task.sender.send(result.map(|_| None)).is_err() {
                            tracing::error!(
                                "Failed to send completion for 'put' request."
                            );
                        }
                    });
                }
                StoreOp::Get => {
                    assert!(task.value.is_none());
                    let result = store.get_kv(&task.key).await;
                    Reactors::master().send_future(async move {
                        if task.sender.send(result.map(Some)).is_err() {
                            tracing::error!(
                                "Failed to send completion for 'get' request."
                            );
                        }
                    });
                }
                StoreOp::Delete => {
                    let result = store.delete_kv(&task.key).await;
                    Reactors::master().send_future(async move {
                        if task.sender
                            .send(result.map(|_| None)).is_err() {
                            tracing::error!(
                                "Failed to send completion for 'delete' request."
                            );
                        }
                    });
                }
            }
        }
    }

    /// Returns the default etcd endpoint.
    pub fn default_endpoint() -> String {
        ETCD_ENDPOINT.to_string()
    }
}

/// Operations that can be enacted on the store.
enum StoreOp {
    Put,
    Get,
    Delete,
}

/// A task that needs to be executed by the worker thread.
struct StoreTask {
    /// Type of operation.
    operation: StoreOp,
    /// Key
    key: String,
    /// Value
    value: Option<Value>,
    /// Sender channel used to signal completion.
    sender: oneshot::Sender<Result<Option<Value>, StoreError>>,
}
