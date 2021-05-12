use common::compose::Builder;
use composer::RpcHandle;
use rpc::mayastor::{
    BdevShareRequest,
    BdevUri,
    Child,
    ChildState,
    CreateNexusRequest,
    DestroyNexusRequest,
    FaultNexusChildRequest,
    Nexus,
    NexusState,
    Null,
};

pub mod common;

#[tokio::test]
async fn nexus_create_test() {
    let test = Builder::new()
        .name("nexus_create_test")
        .add_container("ms1")
        .add_container("ms2")
        .add_container("ms3")
        .add_container("ms4")
        .with_clean(false)
        .build()
        .await
        .unwrap();
    let ms1 = &mut test.grpc_handle("ms1").await.unwrap();
    let ms2 = &mut test.grpc_handle("ms2").await.unwrap();
    let ms3 = &mut test.grpc_handle("ms3").await.unwrap();

    // Create bdevs and share over nvmf

    let child1 = create_and_share_bdevs(ms2).await;
    let child2 = create_and_share_bdevs(ms3).await;

    // Create a nexus with 2 children.

    let nexus_uuid = "8272e9d3-3738-4e33-b8c3-769d8eed5771";
    ms1.mayastor
        .create_nexus(CreateNexusRequest {
            uuid: nexus_uuid.to_string(),
            size: 20 * 1024 * 1024,
            children: vec![child1.clone(), child2.clone()],
        })
        .await
        .expect("Failed to create nexus.");
    assert_eq!(num_nexuses(ms1).await, 1);
    assert_eq!(
        get_nexus_state(ms1, nexus_uuid).await.unwrap(),
        NexusState::NexusOnline as i32
    );

    // Check the nexus and children are healthy.

    let nexus = get_nexus(ms1, nexus_uuid).await;
    assert_eq!(nexus.children.len(), 2);
    let child = get_child(ms1, nexus_uuid, &child1).await;
    assert_eq!(child.state, ChildState::ChildOnline as i32);
    let child = get_child(ms1, nexus_uuid, &child2).await;
    assert_eq!(child.state, ChildState::ChildOnline as i32);

    // Recreate the nexus and check that the nexus and children remain healthy.

    destroy_and_create_nexus(
        ms1,
        nexus_uuid,
        vec![child1.clone(), child2.clone()],
    )
    .await;
    assert_eq!(
        get_nexus_state(ms1, nexus_uuid).await.unwrap(),
        NexusState::NexusOnline as i32
    );
    let nexus = get_nexus(ms1, nexus_uuid).await;
    assert_eq!(nexus.children.len(), 2);
    let child = get_child(ms1, nexus_uuid, &child1).await;
    assert_eq!(child.state, ChildState::ChildOnline as i32);
    let child = get_child(ms1, nexus_uuid, &child2).await;
    assert_eq!(child.state, ChildState::ChildOnline as i32);

    // Fault a child.

    ms1.mayastor
        .fault_nexus_child(FaultNexusChildRequest {
            uuid: nexus_uuid.to_string(),
            uri: child2.clone(),
        })
        .await
        .expect("Failed to fault child");

    assert_eq!(
        get_nexus_state(ms1, nexus_uuid).await.unwrap(),
        NexusState::NexusDegraded as i32
    );
    let child = get_child(ms1, nexus_uuid, &child2).await;
    assert_eq!(child.state, ChildState::ChildFaulted as i32);

    // Recreate the nexus with the faulted child.
    // The faulted child should not be used by the nexus and the nexus should be
    // healthy.

    destroy_and_create_nexus(
        ms1,
        nexus_uuid,
        vec![child1.clone(), child2.clone()],
    )
    .await;

    assert_eq!(
        get_nexus_state(ms1, nexus_uuid).await.unwrap(),
        NexusState::NexusOnline as i32
    );
    let nexus = get_nexus(ms1, nexus_uuid).await;
    assert_eq!(nexus.children.len(), 1);
    let child = get_child(ms1, nexus_uuid, &child1).await;
    assert_eq!(child.state, ChildState::ChildOnline as i32);

    // Recreate the nexus with the faulted child and a new child.
    // Only existing healthy children should be included in the nexus.

    let ms4 = &mut test.grpc_handle("ms4").await.unwrap();
    let new_child = create_and_share_bdevs(ms4).await;
    destroy_and_create_nexus(
        ms1,
        nexus_uuid,
        vec![child1.clone(), child2.clone(), new_child.clone()],
    )
    .await;
    assert_eq!(
        get_nexus_state(ms1, nexus_uuid).await.unwrap(),
        NexusState::NexusOnline as i32
    );
    let nexus = get_nexus(ms1, nexus_uuid).await;
    assert_eq!(nexus.children.len(), 1);
    let child = get_child(ms1, nexus_uuid, &child1).await;
    assert_eq!(child.state, ChildState::ChildOnline as i32);
}

/// Destroys and recreates a nexus.
async fn destroy_and_create_nexus(
    hdl: &mut RpcHandle,
    uuid: &str,
    children: Vec<String>,
) {
    hdl.mayastor
        .destroy_nexus(DestroyNexusRequest {
            uuid: uuid.to_string(),
        })
        .await
        .expect("Failed to destroy nexus");
    assert_eq!(num_nexuses(hdl).await, 0);

    hdl.mayastor
        .create_nexus(CreateNexusRequest {
            uuid: uuid.to_string(),
            size: 20 * 1024 * 1024,
            children,
        })
        .await
        .expect("Failed to create nexus.");
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

/// Returns the number of nexuses.
async fn num_nexuses(hdl: &mut RpcHandle) -> usize {
    let nexus_list = hdl
        .mayastor
        .list_nexus(Null {})
        .await
        .unwrap()
        .into_inner()
        .nexus_list;
    nexus_list.len()
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