extern crate rsedis;

use rsedis::database::Database;
use rsedis::database::Value;

#[test]
fn set_get() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];
    let expected = Vec::clone(&value);
    database.set(&key, value);
    match database.get(&key) {
        Some(val) => {
            match val {
                &Value::Data(ref bytes) => assert_eq!(*bytes, expected),
                _ => assert!(false),
            }
        }
        _ => assert!(false),
    }
}

#[test]
fn get_empty() {
    let database = Database::new();
    let key = vec![1u8];
    match database.get(&key) {
        None => {},
        _ => assert!(false),
    }
}

#[test]
fn set_set_get() {
    let mut database = Database::new();
    let key = vec![1u8];
    database.set(&key, vec![0u8, 0, 0]);
    let value = vec![1u8, 2, 3, 4];
    let expected = Vec::clone(&value);
    database.set(&key, value);
    match database.get(&key) {
        Some(val) => {
            match val {
                &Value::Data(ref bytes) => assert_eq!(*bytes, expected),
                _ => assert!(false),
            }
        }
        _ => assert!(false),
    }
}

#[test]
fn append_append_get() {
    let mut database = Database::new();
    let key = vec![1u8];
    assert_eq!(database.append(&key, vec![0u8, 0, 0]), 3);
    assert_eq!(database.append(&key, vec![1u8, 2, 3, 4]), 7);
    match database.get(&key) {
        Some(val) => {
            match val {
                &Value::Data(ref bytes) => assert_eq!(*bytes, vec![0u8, 0, 0, 1, 2, 3, 4]),
                _ => assert!(false),
            }
        }
        _ => assert!(false),
    }
}

#[test]
fn set_number() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = b"123".to_vec();
    database.set(&key, value);
    match database.get(&key) {
        Some(val) => {
            match val {
                &Value::Integer(ref num) => assert_eq!(*num, 123u64),
                _ => assert!(false),
            }
        }
        _ => assert!(false),
    }
}

#[test]
fn append_number() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = b"123".to_vec();
    database.set(&key, value);
    assert_eq!(database.append(&key, b"asd".to_vec()), 6);
    match database.get(&key) {
        Some(val) => {
            match val {
                &Value::Data(ref bytes) => assert_eq!(*bytes, b"123asd".to_vec()),
                _ => assert!(false),
            }
        }
        _ => assert!(false),
    }
}
