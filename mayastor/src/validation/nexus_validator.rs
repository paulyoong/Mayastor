//! Module responsible for validating that an operation can be performed on a
//! nexus.

use crate::bdev::{
    nexus::{
        nexus_bdev::{Error, Error::ValidationFailed},
        nexus_child::{NexusChild, StatusReasons},
        nexus_child_status_config::ChildStatusConfig,
    },
    nexus_lookup,
    ChildStatus,
    Nexus,
    NexusStatus,
};
use std::future::Future;

/// Perform add child validation. If this passes, execute the future.
pub(crate) async fn add_child<F>(
    nexus_name: &str,
    child_name: &str,
    future: F,
) -> Result<NexusStatus, Error>
where
    F: Future<Output = Result<NexusStatus, Error>>,
{
    let nexus = nexus_exists(nexus_name)?;
    if is_nexus_faulted(&nexus) || !has_healthy_child(&nexus) {
        return Err(ValidationFailed);
    }

    // Add the child to the status configuration as out-of-sync.
    // The status configuration will be updated automatically when the newly
    // added child comes online.
    let mut status_reasons = StatusReasons::new();
    status_reasons.out_of_sync(true);
    if ChildStatusConfig::add(child_name, &status_reasons).is_err() {
        // TODO: different error message?
        return Err(ValidationFailed);
    }

    future.await
}

/// Perform remove child validation. If this passes, execute the future.
pub(crate) async fn remove_child<F>(
    nexus_name: &str,
    child_name: &str,
    future: F,
) -> Result<(), Error>
where
    F: Future<Output = Result<(), Error>>,
{
    let nexus = nexus_exists(nexus_name)?;
    if nexus.child_count == 1 {
        return Err(Error::DestroyLastChild {
            name: nexus_name.to_string(),
            child: child_name.to_string(),
        });
    } else if is_last_healthy_child(nexus, child_name) {
        return Err(ValidationFailed);
    }

    if ChildStatusConfig::remove(child_name).is_err() {
        // TODO: different error message?
        return Err(ValidationFailed);
    }

    future.await?;
    Ok(())
}

// Checks if a given nexus exists
fn nexus_exists(nexus_name: &str) -> Result<&Nexus, Error> {
    match nexus_lookup(nexus_name) {
        Some(nexus) => Ok(nexus),
        None => {
            error!("Failed to find nexus");
            Err(ValidationFailed)
        }
    }
}

/// Checks if a nexus is in the faulted state
fn is_nexus_faulted(nexus: &Nexus) -> bool {
    if nexus.status() == NexusStatus::Faulted {
        error!("Nexus is faulted");
        true
    } else {
        false
    }
}

/// Checks if a nexus has at least one healthy child
fn has_healthy_child(nexus: &Nexus) -> bool {
    if nexus
        .children
        .iter()
        .any(|c| c.status() == ChildStatus::Online)
    {
        true
    } else {
        error!("Nexus does not have any healthy children");
        false
    }
}

// Checks if the child is the last remaining healthy child
fn is_last_healthy_child(nexus: &Nexus, child_name: &str) -> bool {
    let healthy_children = nexus
        .children
        .iter()
        .filter(|child| child.status() == ChildStatus::Online)
        .collect::<Vec<&NexusChild>>();

    if healthy_children.len() == 1 && healthy_children[0].name == child_name {
        error!(
            "Child {} of nexus {} is the last healthy child",
            child_name, nexus.name
        );
        true
    } else {
        false
    }
}
