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
