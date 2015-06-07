extern crate rsedis;

use std;

use rsedis::database::Database;
use rsedis::database::Value;

#[test]
fn set_get() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];
    let expected = Vec::clone(&value);
    assert!(database.get_or_create(0, &key).set(value).is_ok());
    match database.get(0, &key) {
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
    match database.get(0, &key) {
        None => {},
        _ => assert!(false),
    }
}

#[test]
fn set_set_get() {
    let mut database = Database::new();
    let key = vec![1u8];
    assert!(database.get_or_create(0, &key).set(vec![0u8, 0, 0]).is_ok());
    let value = vec![1u8, 2, 3, 4];
    let expected = Vec::clone(&value);
    assert!(database.get_or_create(0, &key).set(value).is_ok());
    match database.get(0, &key) {
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
    assert_eq!(database.get_or_create(0, &key).append(vec![0u8, 0, 0]).unwrap(), 3);
    assert_eq!(database.get_or_create(0, &key).append(vec![1u8, 2, 3, 4]).unwrap(), 7);
    match database.get(0, &key) {
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
    assert!(database.get_or_create(0, &key).set(value).is_ok());
    match database.get(0, &key) {
        Some(val) => {
            match val {
                &Value::Integer(ref num) => assert_eq!(*num, 123i64),
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
    assert!(database.get_or_create(0, &key).set(value).is_ok());
    assert_eq!(database.get_or_create(0, &key).append(b"asd".to_vec()).unwrap(), 6);
    match database.get(0, &key) {
        Some(val) => {
            match val {
                &Value::Data(ref bytes) => assert_eq!(*bytes, b"123asd".to_vec()),
                _ => assert!(false),
            }
        }
        _ => assert!(false),
    }
}

#[test]
fn remove_value() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];
    assert!(database.get_or_create(0, &key).set(value).is_ok());
    match database.remove(0, &key) {
        Some(_) => {},
        _ => assert!(false),
    }
    match database.remove(0, &key) {
        Some(_) => assert!(false),
        _ => {},
    }
}

#[test]
fn incr_str() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = b"123".to_vec();
    assert!(database.get_or_create(0, &key).set(value).is_ok());
    assert_eq!(database.get_or_create(0, &key).incr(1).unwrap(), 124);
    match database.get(0, &key) {
        Some(val) => {
            match val {
                &Value::Integer(i) => assert_eq!(i, 124),
                _ => assert!(false),
            }
        }
        _ => assert!(false),
    }
}

#[test]
fn incr_create_update() {
    let mut database = Database::new();
    let key = vec![1u8];
    assert_eq!(database.get_or_create(0, &key).incr(124).unwrap(), 124);
    match database.get(0, &key) {
        Some(val) => {
            match val {
                &Value::Integer(i) => assert_eq!(i, 124),
                _ => assert!(false),
            }
        }
        _ => assert!(false),
    }
    assert_eq!(database.get_or_create(0, &key).incr(1).unwrap(), 125);
    match database.get(0, &key) {
        Some(val) => {
            match val {
                &Value::Integer(i) => assert_eq!(i, 125),
                _ => assert!(false),
            }
        }
        _ => assert!(false),
    }
}

#[test]
fn incr_overflow() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = format!("{}", std::i64::MAX).into_bytes();
    assert!(database.get_or_create(0, &key).set(value).is_ok());
    assert!(database.get_or_create(0, &key).incr(1).is_err());
}
