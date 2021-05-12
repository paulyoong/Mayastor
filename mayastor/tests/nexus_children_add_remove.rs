//!
//! This test is roughly the same as the tests in nexus_add_remove. However,
//! this test does not use nvmf targets rather uring bdevs

use mayastor::bdev::{nexus_create, nexus_lookup};
use once_cell::sync::OnceCell;

static DISKNAME1: &str = "/tmp/disk1.img";
static DISKNAME2: &str = "/tmp/disk2.img";
static DISKNAME3: &str = "/tmp/disk3.img";

use crate::common::MayastorTest;
use composer::{Builder, ComposeTest};
use mayastor::core::{MayastorCliArgs, Share};

pub mod common;

pub fn mayastor() -> &'static MayastorTest<'static> {
    static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

    MAYASTOR.get_or_init(|| {
        MayastorTest::new(MayastorCliArgs {
            reactor_mask: "0x2".to_string(),
            no_pci: true,
            grpc_endpoint: "0.0.0.0".to_string(),
            ..Default::default()
        })
    })
}

async fn start_etcd(test_name: &str) -> ComposeTest {
    // Start etcd container.
    Builder::new()
        .name(&test_name)
        .add_etcd_container()
        .build()
        .await
        .unwrap()
}

/// create a nexus with two file based devices
/// and then, once created, share it and then
/// remove one of the children
#[tokio::test]
#[ignore]
async fn remove_children_from_nexus() {
    let _etcd = start_etcd("remove_children_from_nexus").await;
    // we can only start mayastor once we run it within the same process, and
    // during start mayastor will create a thread for each of the cores
    // (0x2) here.
    //
    // grpc is not used in this case, and we use channels to send work to
    // mayastor from the runtime here.

    let ms = mayastor();

    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    // create a nexus with two children
    ms.spawn(async {
        nexus_create(
            "remove_from_nexus",
            60 * 1024 * 1024,
            None,
            &[
                format!("uring:///{}", DISKNAME1),
                format!("uring:///{}", DISKNAME2),
            ],
        )
        .await
    })
    .await
    .expect("failed to create nexus");

    // lookup the nexus and share it over nvmf
    ms.spawn(async {
        let nexus =
            nexus_lookup("remove_from_nexus").expect("nexus is not found!");
        nexus.share_nvmf(None).await
    })
    .await
    .expect("failed to share nexus over nvmf");

    // lookup the nexus, and remove a child
    ms.spawn(async {
        let nexus =
            nexus_lookup("remove_from_nexus").expect("nexus is not found!");
        nexus.remove_child(&format!("uring:///{}", DISKNAME1)).await
    })
    .await
    .expect("failed to remove child from nexus");

    // destroy it
    ms.spawn(async {
        let nexus =
            nexus_lookup("remove_from_nexus").expect("nexus is not found!");
        nexus.destroy().await.unwrap();
    })
    .await;

    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
}

/// similar as the above test case however, instead of removal we add one
#[tokio::test]
async fn nexus_add_child() {
    let _etcd = start_etcd("nexus_add_child").await;
    let ms = mayastor();
    // we can only start mayastor once
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);
    common::truncate_file(DISKNAME3, 64 * 1024);

    ms.spawn(async {
        nexus_create(
            "nexus_add_child",
            60 * 1024 * 1024,
            None,
            &[
                format!("uring:///{}", DISKNAME1),
                format!("uring:///{}", DISKNAME2),
            ],
        )
        .await
        .expect("failed to create nexus");
    })
    .await;

    ms.spawn(async {
        let nexus =
            nexus_lookup("nexus_add_child").expect("nexus is not found!");
        nexus
            .share_nvmf(None)
            .await
            .expect("failed to share nexus over nvmf");
    })
    .await;

    ms.spawn(async {
        let nexus =
            nexus_lookup("nexus_add_child").expect("nexus is not found!");
        nexus
            .add_child(&format!("uring:///{}", DISKNAME3), false)
            .await
    })
    .await
    .unwrap();

    ms.spawn(async {
        let nexus =
            nexus_lookup("nexus_add_child").expect("nexus is not found!");
        nexus.destroy().await.unwrap();
    })
    .await;

    common::delete_file(&[
        DISKNAME1.into(),
        DISKNAME2.into(),
        DISKNAME3.into(),
    ]);
}
