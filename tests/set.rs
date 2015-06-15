extern crate rsedis;

use rsedis::database::Value;

#[test]
fn sadd() {
    let mut value = Value::Nil;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(value.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value.sadd(v1.clone()).unwrap(), false);
}

#[test]
fn srem() {
    let mut value = Value::Nil;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(value.srem(&v1).unwrap(), false);
    assert_eq!(value.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value.srem(&v1).unwrap(), true);
    assert_eq!(value.srem(&v1).unwrap(), false);
}

#[test]
fn sismember() {
    let mut value = Value::Nil;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(value.sismember(&v1).unwrap(), false);
    assert_eq!(value.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value.sismember(&v1).unwrap(), true);
}

#[test]
fn scard() {
    let mut value = Value::Nil;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(value.scard().unwrap(), 0);
    assert_eq!(value.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value.scard().unwrap(), 1);
}

#[test]
fn srandmember_toomany_nodup() {
    let mut value = Value::Nil;
    let v1 = vec![1];
    let v2 = vec![2];
    let v3 = vec![3];

    value.sadd(v1.clone()).unwrap();
    value.sadd(v2.clone()).unwrap();
    value.sadd(v3.clone()).unwrap();

    let mut v = value.srandmember(10, false).unwrap();
    v.sort_by(|a, b| a.cmp(b));
    assert_eq!(v, [v1, v2, v3]);
}

#[test]
fn srandmember_alot_dup() {
    let mut value = Value::Nil;
    let v1 = vec![1];
    let v2 = vec![2];
    let v3 = vec![3];

    value.sadd(v1.clone()).unwrap();
    value.sadd(v2.clone()).unwrap();
    value.sadd(v3.clone()).unwrap();

    let v = value.srandmember(100, true).unwrap();
    assert_eq!(v.len(), 100);
    // this test _probably_ passes
    assert!(v.contains(&v1));
    assert!(v.contains(&v2));
    assert!(v.contains(&v3));
}

#[test]
fn srandmember_nodup_all() {
    let mut value = Value::Nil;
    let v1 = vec![1];
    let v2 = vec![2];
    value.sadd(v1.clone()).unwrap();
    value.sadd(v2.clone()).unwrap();

    let mut v = value.srandmember(2, false).unwrap();
    v.sort_by(|a, b| a.cmp(b));
    assert!(v == vec![v1.clone(), v1.clone()] ||
            v == vec![v1.clone(), v2.clone()] ||
            v == vec![v2.clone(), v2.clone()]);
}

#[test]
fn srandmember_nodup_some() {
    let mut value = Value::Nil;
    let v1 = vec![1];
    let v2 = vec![2];
    value.sadd(v1.clone()).unwrap();
    value.sadd(v2.clone()).unwrap();

    let mut v = value.srandmember(1, false).unwrap();
    v.sort_by(|a, b| a.cmp(b));
    assert!(v == vec![v1.clone()] ||
            v == vec![v2.clone()]);
}

#[test]
fn srandmember_dup() {
    let mut value = Value::Nil;
    let v1 = vec![1];
    value.sadd(v1.clone()).unwrap();

    let v = value.srandmember(5, true).unwrap();
    assert_eq!(v, vec![v1.clone(), v1.clone(), v1.clone(), v1.clone(), v1.clone()]);
}

#[test]
fn spop_toomany() {
    let mut value = Value::Nil;
    let v1 = vec![1];
    let v2 = vec![2];
    let v3 = vec![3];

    value.sadd(v1.clone()).unwrap();
    value.sadd(v2.clone()).unwrap();
    value.sadd(v3.clone()).unwrap();

    let mut v = value.spop(10).unwrap();
    v.sort_by(|a, b| a.cmp(b));
    assert_eq!(v, [v1, v2, v3]);
}

#[test]
fn spop_some() {
    let mut value = Value::Nil;
    let v1 = vec![1];
    let v2 = vec![2];
    let v3 = vec![3];

    value.sadd(v1.clone()).unwrap();
    value.sadd(v2.clone()).unwrap();
    value.sadd(v3.clone()).unwrap();

    let v = value.spop(1).unwrap();
    assert!(v == [v1] || v == [v2] || v == [v3]);
}

#[test]
fn smembers() {
    let mut value = Value::Nil;
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 10, 11, 12];

    assert_eq!(value.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value.sadd(v2.clone()).unwrap(), true);
    assert_eq!(value.sadd(v3.clone()).unwrap(), true);

    let mut v = value.smembers().unwrap();
    v.sort_by(|a, b| a.cmp(b));
    assert_eq!(v, [v1, v2, v3]);
}

#[test]
fn sdiff() {
    let mut value1 = Value::Nil;
    let mut value2 = Value::Nil;
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![0, 9, 1, 2];

    assert_eq!(value1.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value1.sadd(v2.clone()).unwrap(), true);

    assert_eq!(value2.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value2.sadd(v3.clone()).unwrap(), true);

    assert_eq!(value1.sdiff(&vec![&value2]).unwrap().iter().collect::<Vec<_>>(),
            vec![&v2]);
}
