use crate::{
    bdev::{
        nexus::nexus_child::ChildState::Faulted,
        ChildState,
        Nexus,
        Reason,
    },
    persistent_store::PersistentStore,
    sleep::mayastor_sleep,
};
use rpc::persistence::{
    ChildInfo,
    ChildState as PersistentChildState,
    NexusInfo,
    Reason as PersistentReason,
};
use std::time::Duration;

type ChildUuid = String;

/// Defines the type of persist operations.
pub(crate) enum PersistOp {
    /// Create a persistent entry.
    Create,
    /// Update a persistent entry.
    Update((ChildUuid, ChildState)),
    /// Save the clean shutdown variable.
    Shutdown,
}

impl Nexus {
    /// Persist information to the store.
    pub(crate) async fn persist(&self, op: PersistOp) {
        if !PersistentStore::enabled() {
            return;
        }
        let mut nexus_info = self.nexus_info.lock().unwrap();
        match op {
            PersistOp::Create => {
                // Initialisation of the persistent info will overwrite any
                // existing entries.
                // This should only be called on nexus creation.
                nexus_info.clean_shutdown = false;
                self.children.iter().for_each(|c| {
                    let state: PersistentChildState = c.state().into();
                    let reason: PersistentReason = c.state().into();
                    nexus_info.children.push(ChildInfo {
                        uuid: c.uuid(),
                        state: state as i32,
                        reason: reason as i32,
                    });
                });
            }
            PersistOp::Update((uuid, state)) => {
                // Only update the state of the child that has changed. Do not
                // update the other children or "clean shutdown" information.
                // This should only be called on a child state change.
                nexus_info.children.iter_mut().for_each(|c| {
                    let persistent_state: PersistentChildState = state.into();
                    let persistent_reason: PersistentReason = state.into();
                    if c.uuid == uuid {
                        c.state = persistent_state as i32;
                        c.reason = persistent_reason as i32;
                    }
                });
            }
            PersistOp::Shutdown => {
                // Only update the clean shutdown variable. Do not update the
                // child state information.
                // This should only be called when destroying a nexus.
                nexus_info.clean_shutdown = true;
            }
        }
        self.save(&nexus_info).await;
    }

    // Save the nexus info to the store. This is integral to ensuring data
    // consistency across restarts of Mayastor. Therefore, keep retrying
    // until successful.
    // TODO: Should we give up retrying eventually?
    async fn save(&self, info: &NexusInfo) {
        let nexus_uuid = self.name.strip_prefix("nexus-").unwrap_or(&self.name);
        loop {
            match PersistentStore::put(&nexus_uuid, info).await {
                Ok(_) => {
                    // The state was saved successfully.
                    break;
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to persist info {:?} with error {}. Retrying...",
                        info,
                        e
                    );
                    // Allow some time for the connection to the persistent
                    // store to be re-established before retrying the operation.
                    let rx = mayastor_sleep(Duration::from_secs(1));
                    rx.await.expect("Failed to wait for Mayastor sleep");
                }
            }
        }
    }
}

impl From<ChildState> for rpc::persistence::ChildState {
    fn from(state: ChildState) -> Self {
        match state {
            ChildState::Init => PersistentChildState::Init,
            ChildState::ConfigInvalid => PersistentChildState::ConfigInvalid,
            ChildState::Open => PersistentChildState::Open,
            ChildState::Destroying => PersistentChildState::Destroying,
            ChildState::Closed => PersistentChildState::Closed,
            ChildState::Faulted(_) => PersistentChildState::Faulted,
        }
    }
}

impl From<ChildState> for rpc::persistence::Reason {
    fn from(state: ChildState) -> Self {
        match state {
            ChildState::Init
            | ChildState::ConfigInvalid
            | ChildState::Open
            | ChildState::Destroying
            | ChildState::Closed => PersistentReason::Unknown,
            Faulted(reason) => match reason {
                Reason::Unknown => PersistentReason::Unknown,
                Reason::OutOfSync => PersistentReason::OutOfSync,
                Reason::CantOpen => PersistentReason::CantOpen,
                Reason::RebuildFailed => PersistentReason::RebuildFailed,
                Reason::IoError => PersistentReason::IoError,
                Reason::Rpc => PersistentReason::Rpc,
            },
        }
    }
}
