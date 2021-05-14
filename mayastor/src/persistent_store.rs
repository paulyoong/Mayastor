//! The persistent store is used to save information that is required by
//! Mayastor across restarts.
//!
//! etcd is used as the backing store and is interfaced with through the use of
//! the etcd-client crate. This crate has a dependency on the tokio async
//! runtime.
use crate::{
    core::{runtime::spawn, Mthread},
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
use futures::channel::oneshot;
use once_cell::sync::OnceCell;
use serde_json::Value;
use snafu::ResultExt;
use std::{future::Future, thread::sleep, time::Duration};

static ETCD_ENDPOINT: &str = "0.0.0.0:2379";
static PERSISTENT_STORE: OnceCell<Option<PersistentStore>> = OnceCell::new();

type TaskReceiver = oneshot::Receiver<Result<Option<Value>, StoreError>>;

/// Persistent store
pub struct PersistentStore {
    /// Backing store used for persistence.
    store: Etcd,
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
                PERSISTENT_STORE.get_or_init(|| {
                    Some(PersistentStore {
                        store: etcd,
                    })
                });
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
        let key_clone = key.to_string();
        let value_clone = put_value.clone();
        let rx = PersistentStore::execute_store_op(async move {
            let mut store =
                PersistentStore::store().as_ref().unwrap().store.clone();
            let result = store.put_kv(&key_clone, &value_clone).await;
            result.map(|_| None)
        });

        let result = rx.await.context(PutWait {
            key: key.to_string(),
            value: put_value,
        })?;
        result.map(|_| ())
    }

    /// Retrieve a value, with the given key, from the store.
    pub async fn get(key: &impl StoreKey) -> Result<Value, StoreError> {
        let key_clone = key.to_string();
        let rx = PersistentStore::execute_store_op(async move {
            let mut store =
                PersistentStore::store().as_ref().unwrap().store.clone();
            let result = store.get_kv(&key_clone).await;
            result.map(Some)
        });
        rx.await
            .context(GetWait {
                key: key.to_string(),
            })?
            .map(|r| r.unwrap())
    }

    /// Delete the entry in the store with the given key.
    pub async fn delete(key: &impl StoreKey) -> Result<(), StoreError> {
        let key_clone = key.to_string();
        let rx = PersistentStore::execute_store_op(async move {
            let mut store =
                PersistentStore::store().as_ref().unwrap().store.clone();
            let result = store.delete_kv(&key_clone).await;
            result.map(|_| None)
        });
        rx.await
            .context(DeleteWait {
                key: key.to_string(),
            })?
            .map(|_| ())
    }

    /// Executes a future representing a store operation (i.e. put, get, delete)
    /// on the tokio runtime.
    /// A channel is returned which can be waited on for the operation to
    /// complete.
    fn execute_store_op(
        f: impl Future<Output = Result<Option<Value>, StoreError>> + Send + 'static,
    ) -> TaskReceiver {
        let (tx, rx) = oneshot::channel::<Result<Option<Value>, StoreError>>();
        spawn(async move {
            let result = f.await;
            let thread = Mthread::get_init();
            // Execute the sending of the result on a "Mayastor thread".
            let rx = thread
                .spawn_local(async move {
                    if tx.send(result).is_err() {
                        tracing::error!(
                            "Failed to send completion for 'put' request."
                        );
                    }
                })
                .unwrap();
            let _ = rx.await;
        });
        rx
    }

    /// Determine if the persistent store has been enabled.
    pub fn enabled() -> bool {
        PERSISTENT_STORE.get().is_some()
    }

    /// Get an immutable reference to the store.
    fn store() -> &'static Option<PersistentStore> {
        PERSISTENT_STORE.get().unwrap()
    }

    /// Returns the default etcd endpoint.
    pub fn default_endpoint() -> String {
        ETCD_ENDPOINT.to_string()
    }
}
