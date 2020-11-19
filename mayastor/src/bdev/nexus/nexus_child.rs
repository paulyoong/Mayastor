use std::{convert::TryFrom, fmt::Display, sync::Arc};

use nix::errno::Errno;
use serde::{export::Formatter, Serialize};
use snafu::{ResultExt, Snafu};

use crate::{
    bdev::{
        nexus::{
            nexus_channel::DREvent,
            nexus_child::ChildState::Faulted,
            nexus_child_status_config::ChildStatusConfig,
        },
        nexus_lookup,
        NexusErrStore,
        VerboseError,
    },
    core::{Bdev, BdevHandle, CoreError, Descriptor, Reactor, Reactors},
    nexus_uri::{bdev_create, bdev_destroy, NexusBdevError},
    rebuild::{ClientOperations, RebuildJob},
    subsys::Config,
};
use crossbeam::atomic::AtomicCell;
use futures::{channel::mpsc, SinkExt};
use tokio::stream::StreamExt;

#[derive(Debug, Snafu)]
pub enum ChildError {
    #[snafu(display("Child is not offline"))]
    ChildNotOffline {},
    #[snafu(display("Child is not closed"))]
    ChildNotClosed {},
    #[snafu(display("Child is faulted, it cannot be reopened"))]
    ChildFaulted {},
    #[snafu(display(
        "Child is smaller than parent {} vs {}",
        child_size,
        parent_size
    ))]
    ChildTooSmall { child_size: u64, parent_size: u64 },
    #[snafu(display("Open child"))]
    OpenChild { source: CoreError },
    #[snafu(display("Claim child"))]
    ClaimChild { source: Errno },
    #[snafu(display("Child is inaccessible"))]
    ChildInaccessible {},
    #[snafu(display("Invalid state of child"))]
    ChildInvalid {},
    #[snafu(display("Opening child bdev without bdev pointer"))]
    OpenWithoutBdev {},
    #[snafu(display("Failed to create a BdevHandle for child"))]
    HandleCreate { source: CoreError },
    #[snafu(display("Failed to create a Bdev for child {}", child))]
    ChildBdevCreate {
        child: String,
        source: NexusBdevError,
    },
}

#[derive(Debug, Serialize, PartialEq, Deserialize, Eq, Copy, Clone)]
pub enum Reason {
    /// no particular reason for the child to be in this state
    /// this is typically the init state
    Unknown,
    /// out of sync - needs to be rebuilt
    OutOfSync,
    /// cannot open
    CantOpen,
    /// the child failed to rebuild successfully
    RebuildFailed,
    /// the child has been faulted due to I/O error(s)
    IoError,
    /// the child has been explicitly faulted due to a rpc call
    Rpc,
}

impl Display for Reason {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "Unknown"),
            Self::OutOfSync => {
                write!(f, "The child is out of sync and requires a rebuild")
            }
            Self::CantOpen => write!(f, "The child bdev could not be opened"),
            Self::RebuildFailed => {
                write!(f, "The child failed to rebuild successfully")
            }
            Self::IoError => write!(f, "The child had too many I/O errors"),
            Self::Rpc => write!(f, "The child is faulted due to a rpc call"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
pub enum ChildState {
    /// child has not been opened, but we are in the process of opening it
    Init,
    /// cannot add this bdev to the parent as its incompatible property wise
    ConfigInvalid,
    /// the child is open for RW
    Open,
    /// the child has been closed by the nexus
    Closed,
    /// the child is faulted
    Faulted(Reason),
}

impl Display for ChildState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Faulted(r) => write!(f, "Faulted with reason {}", r),
            Self::Init => write!(f, "Init"),
            Self::ConfigInvalid => write!(f, "Config parameters are invalid"),
            Self::Open => write!(f, "Child is open"),
            Self::Closed => write!(f, "Closed"),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct NexusChild {
    /// name of the parent this child belongs too
    pub(crate) parent: String,
    /// Name of the child is the URI used to create it.
    /// Note that bdev name can differ from it!
    pub(crate) name: String,
    #[serde(skip_serializing)]
    /// the bdev wrapped in Bdev
    pub(crate) bdev: Option<Bdev>,
    #[serde(skip_serializing)]
    pub(crate) desc: Option<Arc<Descriptor>>,
    /// current state of the child
    #[serde(skip_serializing)]
    pub state: AtomicCell<ChildState>,
    /// record of most-recent IO errors
    #[serde(skip_serializing)]
    pub(crate) err_store: Option<NexusErrStore>,
    #[serde(skip_serializing)]
    remove_channel: (mpsc::Sender<()>, mpsc::Receiver<()>),
}

impl Display for NexusChild {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        if self.bdev.is_some() {
            let bdev = self.bdev.as_ref().unwrap();
            writeln!(
                f,
                "{}: {:?}, blk_cnt: {}, blk_size: {}",
                self.name,
                self.state(),
                bdev.num_blocks(),
                bdev.block_len(),
            )
        } else {
            writeln!(f, "{}: state {:?}", self.name, self.state())
        }
    }
}

impl NexusChild {
    pub(crate) fn set_state(&self, state: ChildState) {
        trace!(
            "{}: child {}: state change from {} to {}",
            self.parent,
            self.name,
            self.state.load().to_string(),
            state.to_string(),
        );

        self.state.store(state);
    }

    /// Open the child in RW mode and claim the device to be ours. If the child
    /// is already opened by someone else (i.e one of the targets) it will
    /// error out.
    ///
    /// only devices in the closed or Init state can be opened.
    ///
    /// A child can only be opened if:
    ///  - it's not faulted
    ///  - it's not already opened
    pub(crate) fn open(
        &mut self,
        parent_size: u64,
    ) -> Result<String, ChildError> {
        trace!("{}: Opening child device {}", self.parent, self.name);

        // verify the state of the child before we open it
        match self.state() {
            ChildState::Faulted(reason) => {
                error!(
                    "{}: can not open child {} reason {}",
                    self.parent, self.name, reason
                );
                return Err(ChildError::ChildFaulted {});
            }
            ChildState::Open => {
                // the child (should) already be open
                assert_eq!(self.bdev.is_some(), true);
            }
            _ => {}
        }

        let bdev = self.bdev.as_ref().unwrap();

        let child_size = bdev.size_in_bytes();
        if parent_size > child_size {
            error!(
                "{}: child {} too small, parent size: {} child size: {}",
                self.parent, self.name, parent_size, child_size
            );

            self.set_state(ChildState::ConfigInvalid);
            return Err(ChildError::ChildTooSmall {
                parent_size,
                child_size,
            });
        }

        let desc = Arc::new(Bdev::open_by_name(&bdev.name(), true).map_err(
            |source| {
                self.set_state(Faulted(Reason::CantOpen));
                ChildError::OpenChild {
                    source,
                }
            },
        )?);

        self.desc = Some(desc);

        let cfg = Config::get();
        if cfg.err_store_opts.enable_err_store {
            self.err_store =
                Some(NexusErrStore::new(cfg.err_store_opts.err_store_size));
        };

        self.set_state(ChildState::Open);

        debug!("{}: child {} opened successfully", self.parent, self.name);
        Ok(self.name.clone())
    }

    /// Fault the child with a specific reason.
    /// We do not close the child if it is out-of-sync because it will
    /// subsequently be rebuilt.
    pub(crate) async fn fault(&mut self, reason: Reason) {
        match reason {
            Reason::OutOfSync => {
                self.set_state(ChildState::Faulted(reason));
            }
            _ => {
                if let Err(e) = self.close().await {
                    error!(
                        "{}: child {} failed to close with error {}",
                        self.parent,
                        self.name,
                        e.verbose()
                    );
                }
                self.set_state(ChildState::Faulted(reason));
            }
        }
        NexusChild::save_state_change();
    }

    /// Set the child as temporarily offline
    pub(crate) async fn offline(&mut self) {
        if let Err(e) = self.close().await {
            error!(
                "{}: child {} failed to close with error {}",
                self.parent,
                self.name,
                e.verbose()
            );
        }
        NexusChild::save_state_change();
    }

    /// Online a previously offlined child.
    /// The child is set out-of-sync so that it will be rebuilt.
    /// TODO: channels need to be updated when bdevs are opened
    pub(crate) async fn online(
        &mut self,
        parent_size: u64,
    ) -> Result<String, ChildError> {
        // Only online a child if it was previously set offline. Check for a
        // "Closed" state as that is what offlining a child will set it to.
        match self.state.load() {
            ChildState::Closed => {
                // Re-create the bdev as it will have been previously destroyed.
                let name =
                    bdev_create(&self.name).await.context(ChildBdevCreate {
                        child: self.name.clone(),
                    })?;
                self.bdev = Bdev::lookup_by_name(&name);
            }
            _ => return Err(ChildError::ChildNotClosed {}),
        }

        let result = self.open(parent_size);
        self.set_state(ChildState::Faulted(Reason::OutOfSync));
        NexusChild::save_state_change();
        result
    }

    /// Save the state of the children to the config file
    pub(crate) fn save_state_change() {
        if ChildStatusConfig::save().is_err() {
            error!("Failed to save child status information");
        }
    }

    /// returns the state of the child
    pub fn state(&self) -> ChildState {
        self.state.load()
    }

    pub(crate) fn rebuilding(&self) -> bool {
        match RebuildJob::lookup(&self.name) {
            Ok(_) => self.state() == ChildState::Faulted(Reason::OutOfSync),
            Err(_) => false,
        }
    }

    /// return a descriptor to this child
    pub fn get_descriptor(&self) -> Result<Arc<Descriptor>, CoreError> {
        if let Some(ref d) = self.desc {
            Ok(d.clone())
        } else {
            Err(CoreError::InvalidDescriptor {
                name: self.name.clone(),
            })
        }
    }

    /// Close the nexus child.
    pub(crate) async fn close(&mut self) -> Result<(), NexusBdevError> {
        info!("Closing child {}", self.name);
        if self.desc.is_some() && self.bdev.is_some() {
            self.desc.as_ref().unwrap().unclaim();
        }

        // Check if the child is local before calling destroy() as this will
        // invalidate the bdev (i.e. set the bdev to None).
        let local_child = self.is_local().unwrap_or_else(|| true);

        // Destruction raises an SPDK_BDEV_EVENT_REMOVE event.
        let result = self.destroy().await;

        // If a child is initialising or local to the nexus, the remove()
        // callback is not called, so don't wait on the remove event as it will
        // never be raised.
        if self.state.load() != ChildState::Init && !local_child {
            self.remove_channel.1.next().await;
        }

        info!("Child {} closed", self.name);
        result
    }

    /// Called in response to a SPDK_BDEV_EVENT_REMOVE event.
    /// All the necessary teardown should be performed here before the bdev is
    /// removed.
    ///
    /// Note: The descriptor *must* be dropped for the remove to complete.
    pub(crate) fn remove(&mut self) {
        info!("Removing child {}", self.name);

        // The bdev is being removed, so ensure we don't use it again.
        self.bdev = None;

        // Remove the child from the I/O path.
        self.set_state(ChildState::Closed);
        let nexus_name = self.parent.clone();
        Reactor::block_on(async move {
            match nexus_lookup(&nexus_name) {
                Some(n) => n.reconfigure(DREvent::ChildRemove).await,
                None => error!("Nexus {} not found", nexus_name),
            }
        });

        // Dropping the last descriptor results in the bdev being removed.
        // This must be performed in this function.
        let desc = self.desc.take();
        drop(desc);

        self.remove_complete();
        info!("Child {} removed", self.name);
    }

    /// Signal that the child removal has been completed.
    pub(crate) fn remove_complete(&self) {
        let mut sender = self.remove_channel.0.clone();
        let name = self.name.clone();
        Reactors::current().send_future(async move {
            if let Err(e) = sender.send(()).await {
                error!(
                    "Failed to send close complete for child {}, error {}",
                    name, e
                );
            }
        });
    }

    /// create a new nexus child
    pub fn new(name: String, parent: String, bdev: Option<Bdev>) -> Self {
        NexusChild {
            name,
            bdev,
            parent,
            desc: None,
            state: AtomicCell::new(ChildState::Init),
            err_store: None,
            remove_channel: mpsc::channel(0),
        }
    }

    /// destroy the child bdev
    pub(crate) async fn destroy(&self) -> Result<(), NexusBdevError> {
        trace!("destroying child {:?}", self);
        if let Some(_bdev) = &self.bdev {
            bdev_destroy(&self.name).await
        } else {
            warn!("Destroy child without bdev");
            Ok(())
        }
    }

    /// Check if the child is in a state that can service I/O.
    /// When out-of-sync, the child is still accessible (can accept I/O)
    /// because:
    /// 1. An added child starts in the out-of-sync state and may require its
    ///    label and metadata to be updated
    /// 2. It needs to be rebuilt
    fn is_accessible(&self) -> bool {
        self.state() == ChildState::Open
            || self.state() == ChildState::Faulted(Reason::OutOfSync)
    }

    /// return reference to child's bdev and a new BdevHandle
    /// both must be present - otherwise it is considered an error
    pub fn get_dev(&self) -> Result<(&Bdev, BdevHandle), ChildError> {
        if !self.is_accessible() {
            info!("{}: Child is inaccessible: {}", self.parent, self.name);
            return Err(ChildError::ChildInaccessible {});
        }

        if let Some(bdev) = &self.bdev {
            if let Ok(desc) = self.get_descriptor() {
                let hndl =
                    BdevHandle::try_from(desc).context(HandleCreate {})?;
                return Ok((bdev, hndl));
            }
        }
        Err(ChildError::ChildInvalid {})
    }

    /// Return the rebuild job which is rebuilding this child, if rebuilding
    fn get_rebuild_job(&self) -> Option<&mut RebuildJob> {
        let job = RebuildJob::lookup(&self.name).ok()?;
        assert_eq!(job.nexus, self.parent);
        Some(job)
    }

    /// Return the rebuild progress on this child, if rebuilding
    pub fn get_rebuild_progress(&self) -> i32 {
        self.get_rebuild_job()
            .map(|j| j.stats().progress as i32)
            .unwrap_or_else(|| -1)
    }

    /// Determines if a child is local to the nexus (i.e. on the same node)
    pub fn is_local(&self) -> Option<bool> {
        match &self.bdev {
            Some(bdev) => {
                // A local child is not exported over nvme or iscsi
                let local = bdev.driver() != "nvme" && bdev.driver() != "iscsi";
                Some(local)
            }
            None => None,
        }
    }
}
