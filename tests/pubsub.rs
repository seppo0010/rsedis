extern crate rsedis;

use std::sync::mpsc::channel;

use rsedis::database::Database;

#[test]
fn pubsub_basic() {
    let mut database = Database::new();
    let channel_name = vec![1u8, 2, 3];
    let message = vec![2u8, 3, 4, 5, 6];
    let (tx, rx) = channel();
    database.subscribe(channel_name.clone(), tx);
    database.publish(&channel_name, &message);
    assert_eq!(rx.recv().unwrap(), message);
}
