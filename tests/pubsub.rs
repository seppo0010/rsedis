extern crate rsedis;

use std::sync::mpsc::channel;

use rsedis::database::{Database, PubsubEvent};

#[test]
fn pubsub_basic() {
    let mut database = Database::mock();
    let channel_name = vec![1u8, 2, 3];
    let message = vec![2u8, 3, 4, 5, 6];
    let (tx, rx) = channel();
    database.subscribe(channel_name.clone(), tx);
    database.publish(&channel_name, &message);
    assert_eq!(rx.recv().unwrap(), PubsubEvent::Message(channel_name, None, message));
}

#[test]
fn unsubscribe() {
    let mut database = Database::mock();
    let channel_name = vec![1u8, 2, 3];
    let message = vec![2u8, 3, 4, 5, 6];
    let (tx, rx) = channel();
    let subscriber_id = database.subscribe(channel_name.clone(), tx);
    database.unsubscribe(channel_name.clone(), subscriber_id);
    database.publish(&channel_name, &message);
    assert!(rx.try_recv().is_err());
}

#[test]
fn pubsub_pattern() {
    let mut database = Database::mock();
    let channel_name = vec![1u8, 2, 3];
    let message = vec![2u8, 3, 4, 5, 6];
    let (tx, rx) = channel();
    database.psubscribe(channel_name.clone(), tx);
    database.publish(&channel_name, &message);
    assert_eq!(rx.recv().unwrap(), PubsubEvent::Message(channel_name.clone(), Some(channel_name.clone()), message));
}
