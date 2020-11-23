use std::{collections::HashMap, convert::TryFrom};

use async_trait::async_trait;
use snafu::ResultExt;
use url::Url;

use crate::{
    bdev::{nexus::instances, util::uri, CreateDestroy, GetName},
    core::Bdev,
    nexus_uri::{self, NexusBdevError},
};

#[derive(Debug)]
pub(super) struct Loopback {
    name: String,
    alias: String,
    uuid: Option<uuid::Uuid>,
}

impl TryFrom<&Url> for Loopback {
    type Error = NexusBdevError;

    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        let segments = uri::segments(url);

        if segments.is_empty() {
            return Err(NexusBdevError::UriInvalid {
                uri: url.to_string(),
                message: String::from("no path segments"),
            });
        }

        let mut parameters: HashMap<String, String> =
            url.query_pairs().into_owned().collect();

        let uuid = uri::uuid(parameters.remove("uuid")).context(
            nexus_uri::UuidParamParseError {
                uri: url.to_string(),
            },
        )?;

        if let Some(keys) = uri::keys(parameters) {
            warn!("ignored parameters: {}", keys);
        }

        Ok(Loopback {
            name: segments.join("/"),
            alias: url.to_string(),
            uuid,
        })
    }
}

impl GetName for Loopback {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}

#[async_trait(?Send)]
impl CreateDestroy for Loopback {
    type Error = NexusBdevError;

    async fn create(&self) -> Result<String, Self::Error> {
        if let Some(mut bdev) = Bdev::lookup_by_name(&self.name) {
            if let Some(uuid) = self.uuid {
                bdev.set_uuid(Some(uuid.to_string()));
            }
            if !bdev.add_alias(&self.alias) {
                error!(
                    "Failed to add alias {} to device {}",
                    self.alias,
                    self.get_name()
                );
            }
        };
        Ok(self.get_name())
    }

    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        instances().iter_mut().for_each(|n| {
            n.children
                .iter_mut()
                .filter(|c| {
                    c.bdev.is_some()
                        && c.bdev.as_ref().unwrap().name() == self.name
                })
                .for_each(|c| {
                    // This is a loopback (local) device, so although we call
                    // remove, the underlying bdev is not destroyed.
                    c.remove();
                })
        });
        Ok(())
    }
}
