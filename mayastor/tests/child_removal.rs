use mayastor::core::{Bdev, BdevHandle};
/// Tests to understand how the bdev handles and descriptors work
use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::{MayastorCliArgs, MayastorEnvironment, Reactor},
};

pub mod common;

static NEXUS_NAME: &str = "HANDLE_NEXUS";
static VOLUME_SIZE: u64 = 10 * 1024 * 1024;

static DISKNAME1: &str = "/tmp/py-disk1.img";
static BDEVNAME1: &str = "aio:///tmp/py-disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/py-disk2.img";
static BDEVNAME2: &str = "aio:///tmp/py-disk2.img?blk_size=512";

fn test_start() {
    // Delete old disk images
    common::delete_file(&[DISKNAME1.to_string(), DISKNAME2.to_string()]);
    // Create disk images
    common::truncate_file_bytes(DISKNAME1, VOLUME_SIZE);
    common::truncate_file_bytes(DISKNAME2, VOLUME_SIZE);
    test_init!();
}

fn test_end() {
    common::delete_file(&[DISKNAME1.to_string(), DISKNAME2.to_string()]);
}

async fn create_nexus() {
    let ch = vec![BDEVNAME1.to_string(), BDEVNAME2.to_string()];
    nexus_create(NEXUS_NAME, VOLUME_SIZE, None, &ch)
        .await
        .unwrap();
}

#[test]
fn successful_remove_child() {
    test_start();
    Reactor::block_on(async {
        create_nexus().await;
        let n = nexus_lookup(NEXUS_NAME).expect("Nexus not found");
        n.remove_child(&BDEVNAME2).await.unwrap();
        assert_eq!(n.children.len(), 1);
        n.destroy().await.unwrap();
    });
    test_end();
}

#[test]
fn remove_child_with_open_descriptor() {
    test_start();
    Reactor::block_on(async {
        create_nexus().await;
        let n = nexus_lookup(NEXUS_NAME).expect("Nexus not found");
        let _d = Bdev::open_by_name(&BDEVNAME2, true)
            .expect("Failed to open descriptor to second child");
        n.remove_child(&BDEVNAME2).await.unwrap();
        assert_eq!(n.children.len(), 1);
        n.destroy().await.unwrap();
    });
    test_end();
}

#[test]
fn remove_child_with_open_handle() {
    test_start();
    Reactor::block_on(async {
        create_nexus().await;
        let n = nexus_lookup(NEXUS_NAME).expect("Nexus not found");
        let _hdl = BdevHandle::open(BDEVNAME2, true, false)
            .expect("failed to create the handle!");
        n.remove_child(&BDEVNAME2).await.unwrap();
        assert_eq!(n.children.len(), 1);
        n.destroy().await.unwrap();
    });
    test_end();
}
