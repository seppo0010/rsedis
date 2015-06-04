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
    assert!(database.get_or_create(&key).set(value).is_ok());
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
    assert!(database.get_or_create(&key).set(vec![0u8, 0, 0]).is_ok());
    let value = vec![1u8, 2, 3, 4];
    let expected = Vec::clone(&value);
    assert!(database.get_or_create(&key).set(value).is_ok());
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
    assert_eq!(database.get_or_create(&key).append(vec![0u8, 0, 0]).unwrap(), 3);
    assert_eq!(database.get_or_create(&key).append(vec![1u8, 2, 3, 4]).unwrap(), 7);
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
    assert!(database.get_or_create(&key).set(value).is_ok());
    match database.get(&key) {
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
    assert!(database.get_or_create(&key).set(value).is_ok());
    assert_eq!(database.get_or_create(&key).append(b"asd".to_vec()).unwrap(), 6);
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

#[test]
fn remove_value() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];
    assert!(database.get_or_create(&key).set(value).is_ok());
    match database.remove(&key) {
        Some(_) => {},
        _ => assert!(false),
    }
    match database.remove(&key) {
        Some(_) => assert!(false),
        _ => {},
    }
}

#[test]
fn incr_str() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = b"123".to_vec();
    assert!(database.get_or_create(&key).set(value).is_ok());
    assert_eq!(database.get_or_create(&key).incr(1).unwrap(), 124);
    match database.get(&key) {
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
    assert_eq!(database.get_or_create(&key).incr(124).unwrap(), 124);
    match database.get(&key) {
        Some(val) => {
            match val {
                &Value::Integer(i) => assert_eq!(i, 124),
                _ => assert!(false),
            }
        }
        _ => assert!(false),
    }
    assert_eq!(database.get_or_create(&key).incr(1).unwrap(), 125);
    match database.get(&key) {
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
    assert!(database.get_or_create(&key).set(value).is_ok());
    assert!(database.get_or_create(&key).incr(1).is_err());
}

#[test]
fn lpush() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];
    let expected = Vec::clone(&value);
    let value2 = vec![1u8, 5, 6, 7];
    let expected2 = Vec::clone(&value2);
    assert!(database.get_or_create(&key).push(value, false).is_ok());
    match database.get(&key) {
        Some(val) => {
            match val {
                &Value::List(ref list) => {
                    assert_eq!(list.len(), 1);
                    assert_eq!(list.front(), Some(&expected));
                }
                _ => assert!(false),
            }
        }
        _ => assert!(false),
    }

    assert!(database.get_or_create(&key).push(value2, false).is_ok());
    match database.get(&key) {
        Some(val) => {
            match val {
                &Value::List(ref list) => {
                    assert_eq!(list.len(), 2);
                    assert_eq!(list.back(), Some(&expected));
                    assert_eq!(list.front(), Some(&expected2));
                }
                _ => assert!(false),
            }
        }
        _ => assert!(false),
    }
}

#[test]
fn lpop() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];
    let expected = Vec::clone(&value);
    let value2 = vec![1u8, 5, 6, 7];
    let expected2 = Vec::clone(&value2);
    assert!(database.get_or_create(&key).push(value, false).is_ok());
    assert!(database.get_or_create(&key).push(value2, false).is_ok());
    let v2 = database.get_or_create(&key).pop(false).unwrap();
    assert_eq!(v2, Some(expected2));
    let v1 = database.get_or_create(&key).pop(false).unwrap();
    assert_eq!(v1, Some(expected));
    let v0 = database.get_or_create(&key).pop(false).unwrap();
    assert_eq!(v0, None);
}

#[test]
fn lindex() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];
    let expected = Vec::clone(&value);
    let value2 = vec![1u8, 5, 6, 7];
    let expected2 = Vec::clone(&value2);
    let el = database.get_or_create(&key);
    assert!(el.push(value, false).is_ok());
    assert!(el.push(value2, false).is_ok());
    let v2 = el.lindex(0).unwrap();
    assert_eq!(v2, Some(&expected2));
    let v1 = el.lindex(1).unwrap();
    assert_eq!(v1, Some(&expected));
    let v0 = el.lindex(2).unwrap();
    assert_eq!(v0, None);

    let v21 = el.lindex(-2).unwrap();
    assert_eq!(v21, Some(&expected2));
    let v11 = el.lindex(-1).unwrap();
    assert_eq!(v11, Some(&expected));
    let v01 = el.lindex(-3).unwrap();
    assert_eq!(v01, None);
}

#[test]
fn linsert() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];
    let value2 = vec![1u8, 5, 6, 7];

    let mut el = database.get_or_create(&key);
    assert!(el.push(Vec::clone(&value), true).is_ok());
    assert!(el.push(Vec::clone(&value2), true).is_ok());
    assert_eq!(el.lindex(0).unwrap(), Some(&value));
    assert_eq!(el.lindex(1).unwrap(), Some(&value2));

    let inserted = vec![8u8, 0, 1, 2];
    assert_eq!(el.linsert(true, Vec::clone(&value2), Vec::clone(&inserted)).unwrap().unwrap(), 3);
    assert_eq!(el.lindex(0).unwrap(), Some(&value));
    assert_eq!(el.lindex(1).unwrap(), Some(&inserted));
    assert_eq!(el.lindex(2).unwrap(), Some(&value2));

    assert_eq!(el.linsert(true, vec![], Vec::clone(&inserted)).unwrap(), None);
}

#[test]
fn llen() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];
    let value2 = vec![1u8, 5, 6, 7];
    let value3 = vec![1u8, 8, 9, 10];

    let mut el = database.get_or_create(&key);
    assert!(el.push(Vec::clone(&value), true).is_ok());
    assert!(el.push(Vec::clone(&value2), true).is_ok());
    assert_eq!(el.llen().unwrap(), 2);

    assert!(el.push(Vec::clone(&value3), true).is_ok());
    assert_eq!(el.llen().unwrap(), 3);
}

#[test]
fn lrange() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];
    let value2 = vec![1u8, 5, 6, 7];
    let value3 = vec![1u8, 8, 9, 10];

    let mut el = database.get_or_create(&key);
    assert!(el.push(Vec::clone(&value), true).is_ok());
    assert!(el.push(Vec::clone(&value2), true).is_ok());
    assert!(el.push(Vec::clone(&value3), true).is_ok());

    assert_eq!(el.lrange(-100, 100).unwrap(), vec![&value, &value2, &value3]);
    assert_eq!(el.lrange(0, 1).unwrap(), vec![&value, &value2]);
    assert_eq!(el.lrange(0, 0).unwrap(), vec![&value]);
    assert_eq!(el.lrange(1, -1).unwrap(), vec![&value2, &value3]);
}

#[test]
fn lrem_left_unlimited() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];
    let value2 = vec![1u8, 5, 6, 7];
    let value3 = vec![1u8, 8, 9, 10];

    let mut el = database.get_or_create(&key);
    assert!(el.push(Vec::clone(&value), true).is_ok());
    assert!(el.push(Vec::clone(&value2), true).is_ok());
    assert!(el.push(Vec::clone(&value3), true).is_ok());

    assert_eq!(el.lrem(true, 0, Vec::clone(&value3)).unwrap(), 1);
    assert_eq!(el.llen().unwrap(), 2);
    assert_eq!(el.lrem(true, 0, Vec::clone(&value)).unwrap(), 1);
    assert_eq!(el.llen().unwrap(), 1);
    assert_eq!(el.lrem(true, 0, Vec::clone(&value2)).unwrap(), 1);
    assert_eq!(el, &Value::Nil);
}

#[test]
fn lrem_left_limited() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];
    let value2 = vec![5u8, 6, 7, 8];

    let mut el = database.get_or_create(&key);
    assert!(el.push(Vec::clone(&value), true).is_ok());
    assert!(el.push(Vec::clone(&value), true).is_ok());
    assert!(el.push(Vec::clone(&value), true).is_ok());
    assert!(el.push(Vec::clone(&value2), true).is_ok());
    assert!(el.push(Vec::clone(&value), true).is_ok());

    assert_eq!(el.lrem(true, 3, Vec::clone(&value)).unwrap(), 3);
    assert_eq!(el.llen().unwrap(), 2);
    match el {
        &mut Value::List(ref list) => assert_eq!(list.front().unwrap(), &value2),
        _ => panic!(),
    }
    assert_eq!(el.lrem(true, 3, Vec::clone(&value)).unwrap(), 1);
    assert_eq!(el.llen().unwrap(), 1);
}

#[test]
fn lrem_right_unlimited() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];
    let value2 = vec![1u8, 5, 6, 7];
    let value3 = vec![1u8, 8, 9, 10];

    let mut el = database.get_or_create(&key);
    assert!(el.push(Vec::clone(&value), true).is_ok());
    assert!(el.push(Vec::clone(&value2), true).is_ok());
    assert!(el.push(Vec::clone(&value3), true).is_ok());

    assert_eq!(el.lrem(false, 0, Vec::clone(&value3)).unwrap(), 1);
    assert_eq!(el.llen().unwrap(), 2);
    assert_eq!(el.lrem(false, 0, Vec::clone(&value)).unwrap(), 1);
    assert_eq!(el.llen().unwrap(), 1);
    assert_eq!(el.lrem(false, 0, Vec::clone(&value2)).unwrap(), 1);
    assert_eq!(el, &Value::Nil);
}

#[test]
fn lrem_right_limited() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];
    let value2 = vec![5u8, 6, 7, 8];

    let mut el = database.get_or_create(&key);
    assert!(el.push(Vec::clone(&value), true).is_ok());
    assert!(el.push(Vec::clone(&value), true).is_ok());
    assert!(el.push(Vec::clone(&value), true).is_ok());
    assert!(el.push(Vec::clone(&value2), true).is_ok());
    assert!(el.push(Vec::clone(&value), true).is_ok());

    assert_eq!(el.lrem(false, 3, Vec::clone(&value)).unwrap(), 3);
    assert_eq!(el.llen().unwrap(), 2);
    match el {
        &mut Value::List(ref list) => assert_eq!(list.front().unwrap(), &value),
        _ => panic!(),
    }
    assert_eq!(el.lrem(false, 3, Vec::clone(&value)).unwrap(), 1);
    assert_eq!(el.llen().unwrap(), 1);
}
