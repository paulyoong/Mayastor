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
use std::{future::Future, sync::Mutex, time::Duration};

static DEFAULT_PORT: &str = "2379";
static PERSISTENT_STORE: OnceCell<Option<Mutex<PersistentStore>>> =
    OnceCell::new();

type TaskReceiver = oneshot::Receiver<Result<Option<Value>, StoreError>>;

/// Persistent store
pub struct PersistentStore {
    /// Backing store used for persistence.
    store: Etcd,
    endpoint: String,
}

impl PersistentStore {
    /// Initialise the persistent store.
    /// This function must be called by the tokio runtime, because the
    /// etcd-client has a dependency on it.
    pub async fn init(endpoint: Option<String>) {
        let endpoint = match endpoint {
            Some(e) => match e.contains(':') {
                true => e,
                false => {
                    format!("{}:{}", e, DEFAULT_PORT)
                }
            },
            None => {
                // No endpoint means no persistent store.
                warn!("Persistent store not initialised");
                return;
            }
        };

        let store =
            PersistentStore::connect_to_backing_store(&endpoint.clone()).await;
        PERSISTENT_STORE.get_or_init(|| {
            Some(Mutex::new(PersistentStore {
                store,
                endpoint,
            }))
        });
    }

    /// Connect to etcd as the backing store.
    /// This must be called from a tokio thread context.
    async fn connect_to_backing_store(endpoint: &str) -> Etcd {
        let mut output_err = true;
        // Continually try to connect to etcd.
        loop {
            match Etcd::new(endpoint).await {
                Ok(store) => {
                    info!("Connected to etcd on endpoint {}", endpoint);
                    return store;
                }
                Err(_) => {
                    if output_err {
                        error!(
                            "Failed to connect to etcd on endpoint {}",
                            endpoint
                        );
                        output_err = false;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Put a key-value in the store.
    pub async fn put(
        key: &impl StoreKey,
        value: &impl StoreValue,
    ) -> Result<(), StoreError> {
        let put_value = serde_json::to_value(value)
            .expect("Failed to convert value to a serde_json value");
        let key_string = key.to_string();
        let value_clone = put_value.clone();
        let rx = PersistentStore::execute_store_op(async move {
            info!(
                "Putting key {}, value {} in store.",
                key_string,
                value_clone.to_string()
            );
            match PersistentStore::store()
                .put_kv(&key_string, &value_clone)
                .await
            {
                Ok(_) => {
                    info!(
                        "Successfully put key {}, value {} in store.",
                        key_string,
                        value_clone.to_string()
                    );
                    Ok(None)
                }
                Err(e) => Err(e),
            }
        });

        let result = rx.await.context(PutWait {
            key: key.to_string(),
            value: put_value.to_string(),
        })?;
        result.map(|_| ())
    }

    /// Retrieve a value, with the given key, from the store.
    pub async fn get(key: &impl StoreKey) -> Result<Value, StoreError> {
        let key_string = key.to_string();
        let rx = PersistentStore::execute_store_op(async move {
            info!("Getting key {} from store.", key_string);
            match PersistentStore::store().get_kv(&key_string).await {
                Ok(value) => {
                    info!("Successfully got key {}", key_string);
                    Ok(Some(value))
                }
                Err(e) => Err(e),
            }
        });
        rx.await
            .context(GetWait {
                key: key.to_string(),
            })?
            .map(|value| value.unwrap())
    }

    /// Delete the entry in the store with the given key.
    pub async fn delete(key: &impl StoreKey) -> Result<(), StoreError> {
        let key_string = key.to_string();
        let rx = PersistentStore::execute_store_op(async move {
            info!("Deleting key {} from store.", key_string);
            match PersistentStore::store().delete_kv(&key_string).await {
                Ok(_) => {
                    info!(
                        "Successfully deleted key {} from store.",
                        key_string
                    );
                    Ok(None)
                }
                Err(e) => Err(e),
            }
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
            let result =
                match tokio::time::timeout(Duration::from_secs(10), f).await {
                    Ok(result) => result,
                    Err(_) => {
                        PersistentStore::reconnect().await;
                        Err(StoreError::OpTimeout {})
                    }
                };

            // Execute the sending of the result on a "Mayastor thread".
            let thread = Mthread::get_init();
            let rx = thread
                .spawn_local(async move {
                    if tx.send(result).is_err() {
                        tracing::error!(
                            "Failed to send completion for 'put' request."
                        );
                    }
                })
                .expect("Failed to send future to Mayastor thread");
            let _ = rx.await;
        });
        rx
    }

    /// Determine if the persistent store has been enabled.
    pub fn enabled() -> bool {
        PERSISTENT_STORE.get().is_some()
    }

    /// Get an instance of the backing store.
    fn store() -> Etcd {
        PERSISTENT_STORE
            .get()
            .unwrap()
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .store
            .clone()

        // PERSISTENT_STORE
        //     .get()
        //     .expect("Persistent store should have been initialised")
        //     .as_ref()
        //     .expect("Failed to get a reference to the persistent store")
        //     .store
        //     .lock()
        //     .unwrap()
        //     .clone()
    }

    fn endpoint() -> String {
        PERSISTENT_STORE
            .get()
            .unwrap()
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .endpoint
            .clone()
    }

    async fn reconnect() {
        let store = PersistentStore::connect_to_backing_store(
            &PersistentStore::endpoint(),
        )
        .await;
        let opt_store = PERSISTENT_STORE.get().unwrap();
        let mut locked_store = opt_store.as_ref().unwrap().lock().unwrap();
        locked_store.store = store;
    }
}
