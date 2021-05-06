use crate::{
    core::Reactors,
    store::{
        etcd::Etcd,
        store_defs::{Store, StoreError, StoreKey, StoreValue},
    },
};
use futures::{
    channel::{mpsc, mpsc::Receiver, oneshot},
    StreamExt,
};
use once_cell::sync::OnceCell;
use serde_json::Value;

static ETCD_ENDPOINT: &str = "0.0.0.0:2379";
static PERSISTENT_STORE: OnceCell<PersistentStore> = OnceCell::new();

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
    pub async fn run() -> Result<(), ()> {
        // Create an instance of the backing store.
        let etcd = Etcd::new(ETCD_ENDPOINT).await;
        if etcd.is_err() {
            // If the store cannot be connected to, we cannot run.
            panic!("Failed to connect to etcd");
        }

        // Initialise the persistent store.
        let (sender, mut receiver) = mpsc::channel::<StoreTask>(0);
        PERSISTENT_STORE.get_or_init(|| PersistentStore {
            store: etcd.unwrap(),
            sender,
        });

        // Run the work loop which executes store tasks.
        PersistentStore::do_work(&mut receiver).await;
        Ok(())
    }

    /// Put a key-value in the store.
    pub async fn put(
        key: &impl StoreKey,
        value: &impl StoreValue,
    ) -> Result<(), StoreError> {
        tracing::warn!("Putting key {:?}, value {:?}", key, value);
        let r = PersistentStore::dispatch_task(
            StoreOp::Put,
            key,
            Some(serde_json::to_value(value).unwrap()),
        )
        .await;
        let result = r.await.expect("Failed to wait for 'put' to complete");
        result.map(|_| ())
    }

    /// Retrieve a value, with the given key, from the store.
    pub async fn get(key: &impl StoreKey) -> Result<Value, StoreError> {
        let r = PersistentStore::dispatch_task(StoreOp::Get, key, None).await;
        r.await
            .expect("Failed to wait for 'get' to complete.")
            .map(|r| r.unwrap())
    }

    /// Delete the entry in the store with the given key.
    pub async fn delete(key: &impl StoreKey) -> Result<(), StoreError> {
        let r =
            PersistentStore::dispatch_task(StoreOp::Delete, key, None).await;
        r.await
            .expect("Failed to wait for 'get' to complete.")
            .map(|_| ())
    }

    /// Send task to the worker thread and return a receiver channel which can
    /// be waited on for task completion.
    async fn dispatch_task(
        op: StoreOp,
        key: &impl ToString,
        value: Option<Value>,
    ) -> TaskReceiver {
        let (s, r) = oneshot::channel::<Result<Option<Value>, StoreError>>();
        PersistentStore::store()
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

    fn store() -> &'static PersistentStore {
        PERSISTENT_STORE.get().unwrap()
    }

    /// Worker thread which executes the tasks sent to it.
    /// This MUST be executed on a tokio thread as that is what is required by
    /// the etcd-client crate.
    async fn do_work(receiver: &mut Receiver<StoreTask>) {
        loop {
            let task = receiver.next().await.unwrap();
            let mut store = PersistentStore::store().store.clone();

            // Once an operation has been performed on the store, send the
            // result back on the provided channel. This has to be performed by
            // sending a future to our reactor as trying to send it directly
            // from the tokio thread doesn't work - the receiver side never
            // receives the message.
            match task.operation {
                StoreOp::Put => {
                    let result = store.put_kv(&task.key, &task.value).await;
                    Reactors::master().send_future(async move {
                        task.sender
                            .send(result.map(|_| None))
                            .expect("Failed to send 'put' result");
                    });
                }
                StoreOp::Get => {
                    assert!(task.value.is_none());
                    let result = store.get_kv(&task.key).await;
                    Reactors::master().send_future(async move {
                        task.sender
                            .send(result.map(Some))
                            .expect("Failed to send 'get' result");
                    });
                }
                StoreOp::Delete => {
                    let result = store.delete_kv(&task.key).await;
                    Reactors::master().send_future(async move {
                        task.sender
                            .send(result.map(|_| None))
                            .expect("Failed to send 'delete' result");
                    });
                }
            }
        }
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
