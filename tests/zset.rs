extern crate rsedis;

use std::collections::Bound;

use rsedis::database::Value;

#[test]
fn zadd_basic() {
    let mut value = Value::Nil;
    let s1 = 1.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 1.0;
    let v2 = vec![5, 6, 7, 8];

    assert_eq!(value.zadd(s1, v1.clone(), false, false, false).unwrap(), true);
    assert_eq!(value.zadd(s1, v1.clone(), false, false, false).unwrap(), false);
    assert_eq!(value.zadd(s2, v2.clone(), false, false, false).unwrap(), true);
    assert_eq!(value.zadd(s1, v2.clone(), false, false, false).unwrap(), false);
    match value {
        Value::SortedSet(_, hs) => {
            assert_eq!(hs.get(&v1).unwrap(), &s1);
            assert_eq!(hs.get(&v2).unwrap(), &s1);
        },
        _ => panic!("Expected zset"),
    }
}

#[test]
fn zadd_nx() {
    let mut value = Value::Nil;
    let s1 = 1.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 1.0;
    let v2 = vec![5, 6, 7, 8];

    assert_eq!(value.zadd(s1, v1.clone(), true, false, false).unwrap(), true);
    assert_eq!(value.zadd(s1, v1.clone(), true, false, false).unwrap(), false);
    assert_eq!(value.zadd(s2, v2.clone(), true, false, false).unwrap(), true);
    assert_eq!(value.zadd(s1, v2.clone(), true, false, false).unwrap(), false);
    match value {
        Value::SortedSet(_, hs) => {
            assert_eq!(hs.get(&v1).unwrap(), &s1);
            assert_eq!(hs.get(&v2).unwrap(), &s2);
        },
        _ => panic!("Expected zset"),
    }
}

#[test]
fn zadd_xx() {
    let mut value = Value::Nil;
    let s1 = 1.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 2.0;

    assert_eq!(value.zadd(s1, v1.clone(), false, true, false).unwrap(), false);
    assert_eq!(value.zadd(s1, v1.clone(), false, false, false).unwrap(), true);
    assert_eq!(value.zadd(s2, v1.clone(), false, true, false).unwrap(), false);
    match value {
        Value::SortedSet(_, hs) => {
            assert_eq!(hs.get(&v1).unwrap(), &s2);
        },
        _ => panic!("Expected zset"),
    }
}

#[test]
fn zadd_ch() {
    let mut value = Value::Nil;
    let s1 = 1.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 2.0;

    assert_eq!(value.zadd(s1, v1.clone(), false, false, true).unwrap(), true);
    assert_eq!(value.zadd(s1, v1.clone(), false, false, false).unwrap(), false);
    assert_eq!(value.zadd(s2, v1.clone(), false, false, true).unwrap(), true);
    match value {
        Value::SortedSet(_, hs) => {
            assert_eq!(hs.get(&v1).unwrap(), &s2);
        },
        _ => panic!("Expected zset"),
    }
}

#[test]
fn zcount() {
    let mut value = Value::Nil;
    let s1 = 1.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 2.0;
    let v2 = vec![5, 6, 7, 8];

    assert_eq!(value.zadd(s1, v1.clone(), false, false, false).unwrap(), true);
    assert_eq!(value.zadd(s2, v2.clone(), false, false, false).unwrap(), true);
    assert_eq!(value.zcount(Bound::Included(0.0), Bound::Included(5.0)).unwrap(), 2);
    assert_eq!(value.zcount(Bound::Included(1.0), Bound::Included(2.0)).unwrap(), 2);
    assert_eq!(value.zcount(Bound::Excluded(1.0), Bound::Excluded(2.0)).unwrap(), 0);
    assert_eq!(value.zcount(Bound::Included(1.5), Bound::Included(2.0)).unwrap(), 1);
    assert_eq!(value.zcount(Bound::Included(5.0), Bound::Included(10.0)).unwrap(), 0);
}
