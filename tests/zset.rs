extern crate rsedis;

use std::usize;
use std::collections::Bound;

use rsedis::database::{Value, ValueSortedSet};

macro_rules! zadd {
    ($value: expr, $score: expr, $member: expr) => (
        $value.zadd($score, $member.clone(), false, false, false, false).unwrap()
    )
}

#[test]
fn zadd_basic() {
    let mut value = Value::Nil;
    let s1 = 1.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 1.0;
    let v2 = vec![5, 6, 7, 8];

    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(zadd!(value, s1, v1), false);
    assert_eq!(zadd!(value, s2, v2), true);
    assert_eq!(zadd!(value, s1, v2), false);
    match value {
        Value::SortedSet(value) => match value {
            ValueSortedSet::Data(_, hs) => {
                assert_eq!(hs.get(&v1).unwrap(), &s1);
                assert_eq!(hs.get(&v2).unwrap(), &s1);
            },
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

    assert_eq!(value.zadd(s1, v1.clone(), true, false, false, false).unwrap(), true);
    assert_eq!(value.zadd(s1, v1.clone(), true, false, false, false).unwrap(), false);
    assert_eq!(value.zadd(s2, v2.clone(), true, false, false, false).unwrap(), true);
    assert_eq!(value.zadd(s1, v2.clone(), true, false, false, false).unwrap(), false);
    match value {
        Value::SortedSet(value) => match value {
            ValueSortedSet::Data(_, hs) => {
                assert_eq!(hs.get(&v1).unwrap(), &s1);
                assert_eq!(hs.get(&v2).unwrap(), &s2);
            },
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

    assert_eq!(value.zadd(s1, v1.clone(), false, true, false, false).unwrap(), false);
    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(value.zadd(s2, v1.clone(), false, true, false, false).unwrap(), false);
    match value {
        Value::SortedSet(value) => match value {
            ValueSortedSet::Data(_, hs) => {
                assert_eq!(hs.get(&v1).unwrap(), &s2);
            },
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

    assert_eq!(value.zadd(s1, v1.clone(), false, false, true, false).unwrap(), true);
    assert_eq!(zadd!(value, s1, v1), false);
    assert_eq!(value.zadd(s2, v1.clone(), false, false, true, false).unwrap(), true);
    match value {
        Value::SortedSet(value) => match value {
            ValueSortedSet::Data(_, hs) => {
                assert_eq!(hs.get(&v1).unwrap(), &s2);
            },
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

    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(zadd!(value, s2, v2), true);
    assert_eq!(value.zcount(Bound::Included(0.0), Bound::Included(5.0)).unwrap(), 2);
    assert_eq!(value.zcount(Bound::Included(1.0), Bound::Included(2.0)).unwrap(), 2);
    assert_eq!(value.zcount(Bound::Excluded(1.0), Bound::Excluded(2.0)).unwrap(), 0);
    assert_eq!(value.zcount(Bound::Included(1.5), Bound::Included(2.0)).unwrap(), 1);
    assert_eq!(value.zcount(Bound::Included(5.0), Bound::Included(10.0)).unwrap(), 0);
}

#[test]
fn zrange() {
    let mut value = Value::Nil;
    let s1 = 0.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 0.0;
    let v2 = vec![5, 6, 7, 8];
    let s3 = 0.0;
    let v3 = vec![9, 10, 11, 12];

    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(zadd!(value, s3, v3), true);
    assert_eq!(zadd!(value, s2, v2), true);
    assert_eq!(value.zrange(0, -1, true).unwrap(), vec![
            vec![1, 2, 3, 4], b"0".to_vec(),
            vec![5, 6, 7, 8], b"0".to_vec(),
            vec![9, 10, 11, 12], b"0".to_vec(),
            ]);
    assert_eq!(value.zrange(1, 1, true).unwrap(), vec![
            vec![5, 6, 7, 8], b"0".to_vec(),
            ]);
    assert_eq!(value.zrange(2, 0, true).unwrap().len(), 0);
}

#[test]
fn zrangebyscore() {
    let mut value = Value::Nil;
    let s1 = 10.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 20.0;
    let v2 = vec![5, 6, 7, 8];
    let s3 = 30.0;
    let v3 = vec![9, 10, 11, 12];

    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(zadd!(value, s3, v3), true);
    assert_eq!(zadd!(value, s2, v2), true);
    assert_eq!(value.zrangebyscore(Bound::Unbounded, Bound::Unbounded, true, 0, usize::MAX, false).unwrap(), vec![
            vec![1, 2, 3, 4], b"10".to_vec(),
            vec![5, 6, 7, 8], b"20".to_vec(),
            vec![9, 10, 11, 12], b"30".to_vec(),
            ]);
    assert_eq!(value.zrangebyscore(Bound::Excluded(10.0), Bound::Included(20.0), true, 0, usize::MAX, false).unwrap(), vec![
            vec![5, 6, 7, 8], b"20".to_vec(),
            ]);
    assert_eq!(value.zrangebyscore(Bound::Included(20.0), Bound::Excluded(30.0), true, 0, usize::MAX, false).unwrap(), vec![
            vec![5, 6, 7, 8], b"20".to_vec(),
            ]);
    assert_eq!(value.zrangebyscore(Bound::Unbounded, Bound::Unbounded, true, 1, 1, false).unwrap(), vec![
            vec![5, 6, 7, 8], b"20".to_vec(),
            ]);
    assert_eq!(value.zrangebyscore(Bound::Excluded(30.0), Bound::Included(20.0), false, 0, usize::MAX, false).unwrap().len(), 0);
    assert_eq!(value.zrangebyscore(Bound::Excluded(30.0), Bound::Excluded(30.0), false, 0, usize::MAX, false).unwrap().len(), 0);
    assert_eq!(value.zrangebyscore(Bound::Included(30.0), Bound::Included(30.0), false, 0, usize::MAX, false).unwrap().len(), 1);
    assert_eq!(value.zrangebyscore(Bound::Included(30.0), Bound::Excluded(30.0), false, 0, usize::MAX, false).unwrap().len(), 0);
    assert_eq!(value.zrangebyscore(Bound::Included(21.0), Bound::Included(22.0), false, 0, usize::MAX, false).unwrap().len(), 0);
}

#[test]
fn zrevrangebyscore() {
    let mut value = Value::Nil;
    let s1 = 10.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 20.0;
    let v2 = vec![5, 6, 7, 8];
    let s3 = 30.0;
    let v3 = vec![9, 10, 11, 12];

    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(zadd!(value, s3, v3), true);
    assert_eq!(zadd!(value, s2, v2), true);
    assert_eq!(value.zrangebyscore(Bound::Unbounded, Bound::Unbounded, true, 0, usize::MAX, true).unwrap(), vec![
            vec![9, 10, 11, 12], b"30".to_vec(),
            vec![5, 6, 7, 8], b"20".to_vec(),
            vec![1, 2, 3, 4], b"10".to_vec(),
            ]);
    assert_eq!(value.zrangebyscore(Bound::Included(20.0), Bound::Excluded(10.0), true, 0, usize::MAX, true).unwrap(), vec![
            vec![5, 6, 7, 8], b"20".to_vec(),
            ]);
    assert_eq!(value.zrangebyscore(Bound::Excluded(30.0), Bound::Included(20.0), true, 0, usize::MAX, true).unwrap(), vec![
            vec![5, 6, 7, 8], b"20".to_vec(),
            ]);
    assert_eq!(value.zrangebyscore(Bound::Unbounded, Bound::Unbounded, true, 1, 1, true).unwrap(), vec![
            vec![5, 6, 7, 8], b"20".to_vec(),
            ]);
    assert_eq!(value.zrangebyscore(Bound::Included(20.0), Bound::Excluded(30.0), false, 0, usize::MAX, true).unwrap().len(), 0);
    assert_eq!(value.zrangebyscore(Bound::Excluded(30.0), Bound::Excluded(30.0), false, 0, usize::MAX, true).unwrap().len(), 0);
    assert_eq!(value.zrangebyscore(Bound::Included(30.0), Bound::Included(30.0), false, 0, usize::MAX, true).unwrap().len(), 1);
    assert_eq!(value.zrangebyscore(Bound::Excluded(30.0), Bound::Included(30.0), false, 0, usize::MAX, true).unwrap().len(), 0);
    assert_eq!(value.zrangebyscore(Bound::Included(22.0), Bound::Included(21.0), false, 0, usize::MAX, true).unwrap().len(), 0);
}

#[test]
fn zrank() {
    let mut value = Value::Nil;
    let s1 = 0.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 0.0;
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 10, 11, 12];

    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(zadd!(value, s2, v2), true);
    assert_eq!(value.zrank(v1.clone()).unwrap(), Some(0));
    assert_eq!(value.zrank(v2.clone()).unwrap(), Some(1));
    assert_eq!(value.zrank(v3.clone()).unwrap(), None);
}

#[test]
fn zadd_update() {
    let mut value = Value::Nil;
    let s1 = 0.0;
    let s2 = 1.0;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(zadd!(value, s2, v1), false);
    assert_eq!(value.zrange(0, -1, true).unwrap(), vec![
            vec![1, 2, 3, 4], b"1".to_vec(),
            ]);
}

#[test]
fn zadd_incr() {
    let mut value = Value::Nil;
    let s1 = 1.0;
    let incr = 2.0;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(value.zadd(s1, v1.clone(), false, false, false, true).unwrap(), true);
    assert_eq!(value.zrange(0, -1, true).unwrap(), vec![v1.clone(), b"1".to_vec()]);
    assert_eq!(value.zadd(incr, v1.clone(), false, false, false, true).unwrap(), false);
    assert_eq!(value.zrange(0, -1, true).unwrap(), vec![v1.clone(), b"3".to_vec()]);
}

#[test]
fn zadd_incr_ch() {
    let mut value = Value::Nil;
    let s1 = 1.0;
    let incr = 2.0;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(value.zadd(s1, v1.clone(), false, false, true, true).unwrap(), true);
    assert_eq!(value.zrange(0, -1, true).unwrap(), vec![v1.clone(), b"1".to_vec()]);
    assert_eq!(value.zadd(incr, v1.clone(), false, false, true, true).unwrap(), true);
    assert_eq!(value.zrange(0, -1, true).unwrap(), vec![v1.clone(), b"3".to_vec()]);
}

#[test]
fn zrem() {
    let mut value = Value::Nil;
    let s1 = 0.0;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(value.zrem(vec![8u8]).unwrap(), false);
    assert_eq!(value.zrem(v1.clone()).unwrap(), true);
    assert_eq!(value.zrem(v1.clone()).unwrap(), false);
}

#[test]
fn zincrby() {
    let mut value = Value::Nil;
    let s1 = 1.0;
    let s2 = 2.0;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(value.zincrby(s1.clone(), v1.clone()).unwrap(), s1);
    assert_eq!(value.zincrby(s2.clone(), v1.clone()).unwrap(), s1 + s2);
    assert_eq!(value.zincrby(- s1.clone(), v1.clone()).unwrap(), s2);
}
