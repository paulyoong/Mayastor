use crate::common::fio_run_verify;
use common::compose::Builder;
use composer::{Binary, ComposeTest, ContainerSpec, RpcHandle};
use etcd_client::Client;
use mayastor::bdev::{ChildState as MayastorChildState, Reason};
use rpc::mayastor::{
    BdevShareRequest,
    BdevUri,
    Child,
    ChildState,
    CreateNexusRequest,
    CreateReply,
    Nexus,
    NexusState,
    Null,
    PublishNexusRequest,
    ShareProtocolNexus,
};
use std::convert::TryFrom;

pub mod common;

#[tokio::test]
#[ignore = "waiting on PR https://github.com/openebs/Mayastor/pull/880"]
async fn persist_child_state() {
    let (test, _etcd_endpoint) =
        start_infrastructure("persist_child_state").await;
    let ms1 = &mut test.grpc_handle("ms1").await.unwrap();
    let ms2 = &mut test.grpc_handle("ms2").await.unwrap();
    let ms3 = &mut test.grpc_handle("ms3").await.unwrap();

    // Create bdevs and share over nvmf.
    let child1 = create_and_share_bdevs(ms2).await;
    let child2 = create_and_share_bdevs(ms3).await;

    // Create and share a nexus.
    let nexus_uuid = "8272e9d3-3738-4e33-b8c3-769d8eed5771";
    let nexus_uri = create_and_publish_nexus(
        ms1,
        nexus_uuid,
        vec![child1.clone(), child2.clone()],
    )
    .await;

    // Unshare one of the children.
    ms3.bdev
        .unshare(CreateReply {
            name: "disk0".to_string(),
        })
        .await
        .expect("Failed to unshare");

    // Create and connect NVMF target.
    let target = nvmeadm::NvmeTarget::try_from(nexus_uri.clone()).unwrap();
    let devices = target.connect().unwrap();
    let fio_hdl = tokio::spawn(async move {
        fio_run_verify(&devices[0].path.to_string()).unwrap()
    });

    fio_hdl.await.expect_err("Fio is expected to fail.");

    // Disconnect NVMF target
    target.disconnect().unwrap();

    // Reshare bdev to prevent infinite nvmf retries.
    ms3.bdev
        .share(BdevShareRequest {
            name: "disk0".to_string(),
            proto: "nvmf".to_string(),
        })
        .await
        .expect("Failed to share");

    assert_eq!(
        get_nexus_state(ms1, &nexus_uuid).await.unwrap(),
        NexusState::NexusDegraded as i32
    );
    assert_eq!(
        get_child(ms1, &nexus_uuid, &child1).await.state,
        ChildState::ChildOnline as i32
    );
    assert_eq!(
        get_child(ms1, &nexus_uuid, &child2).await.state,
        ChildState::ChildFaulted as i32
    );

    // Use etcd-client to get the value in the store.
    let mut etcd = Client::connect(["0.0.0.0:2379"], None).await.unwrap();
    let response = etcd
        .get(child2.clone(), None)
        .await
        .expect("No entry found");
    let value = response.kvs().first().unwrap().value();
    let state: MayastorChildState = serde_json::from_slice(value).unwrap();
    assert_eq!(state, MayastorChildState::Faulted(Reason::IoError))
}

async fn start_infrastructure(test_name: &str) -> (ComposeTest, String) {
    let etcd_endpoint = format!("http://etcd.{}:2379", test_name);
    let test = Builder::new()
        .name(test_name)
        .add_container_spec(
            ContainerSpec::from_binary(
                "etcd",
                Binary::from_nix("etcd").with_args(vec![
                    "--data-dir",
                    "/tmp/etcd-data",
                    "--advertise-client-urls",
                    "http://0.0.0.0:2379",
                    "--listen-client-urls",
                    "http://0.0.0.0:2379",
                ]),
            )
            .with_portmap("2379", "2379")
            .with_portmap("2380", "2380"),
        )
        .add_container_bin(
            "ms1",
            Binary::from_dbg("mayastor").with_args(vec!["-p", &etcd_endpoint]),
        )
        .add_container_bin(
            "ms2",
            Binary::from_dbg("mayastor").with_args(vec!["-p", &etcd_endpoint]),
        )
        .add_container_bin(
            "ms3",
            Binary::from_dbg("mayastor").with_args(vec!["-p", &etcd_endpoint]),
        )
        .with_clean(false)
        .build()
        .await
        .unwrap();
    (test, etcd_endpoint)
}

/// Creates and publishes a nexus.
/// Returns the share uri of the nexus.
async fn create_and_publish_nexus(
    hdl: &mut RpcHandle,
    uuid: &str,
    children: Vec<String>,
) -> String {
    hdl.mayastor
        .create_nexus(CreateNexusRequest {
            uuid: uuid.to_string(),
            size: 20 * 1024 * 1024,
            children,
        })
        .await
        .expect("Failed to create nexus.");
    hdl.mayastor
        .publish_nexus(PublishNexusRequest {
            uuid: uuid.to_string(),
            key: "".to_string(),
            share: ShareProtocolNexus::NexusNvmf as i32,
        })
        .await
        .expect("Failed to publish nexus")
        .into_inner()
        .device_uri
}

/// Creates and shares a bdev over NVMf and returns the share uri.
async fn create_and_share_bdevs(hdl: &mut RpcHandle) -> String {
    hdl.bdev
        .create(BdevUri {
            uri: "malloc:///disk0?size_mb=100".into(),
        })
        .await
        .unwrap();
    let reply = hdl
        .bdev
        .share(BdevShareRequest {
            name: "disk0".into(),
            proto: "nvmf".into(),
        })
        .await
        .unwrap();
    reply.into_inner().uri
}

/// Returns the nexus with the given uuid.
async fn get_nexus(hdl: &mut RpcHandle, uuid: &str) -> Nexus {
    let nexus_list = hdl
        .mayastor
        .list_nexus(Null {})
        .await
        .unwrap()
        .into_inner()
        .nexus_list;
    let n = nexus_list
        .iter()
        .filter(|n| n.uuid == uuid)
        .collect::<Vec<_>>();
    assert_eq!(n.len(), 1);
    n[0].clone()
}

/// Returns the state of the nexus with the given uuid.
async fn get_nexus_state(hdl: &mut RpcHandle, uuid: &str) -> Option<i32> {
    let list = hdl
        .mayastor
        .list_nexus(Null {})
        .await
        .unwrap()
        .into_inner()
        .nexus_list;
    for nexus in list {
        if nexus.uuid == uuid {
            return Some(nexus.state);
        }
    }
    None
}

/// Returns a child with the given URI.
async fn get_child(
    hdl: &mut RpcHandle,
    nexus_uuid: &str,
    child_uri: &str,
) -> Child {
    let n = get_nexus(hdl, nexus_uuid).await;
    let c = n
        .children
        .iter()
        .filter(|c| c.uri == child_uri)
        .collect::<Vec<_>>();
    assert_eq!(c.len(), 1);
    c[0].clone()
}
