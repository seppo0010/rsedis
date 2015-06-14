extern crate rsedis;

use std::collections::LinkedList;

use rsedis::database::Value;
use rsedis::database::ValueList;

#[test]
fn lpush() {
    let v1 = vec![1u8, 2, 3, 4];
    let v2 = vec![1u8, 5, 6, 7];
    let mut value = Value::List(ValueList::Data(LinkedList::new()));

    value.push(v1.clone(), false).unwrap();
    {
        let list = match value { Value::List(ref value) => match value { &ValueList::Data(ref l) => l }, _ => panic!("Expected list") };
        assert_eq!(list.len(), 1);
        assert_eq!(list.front(), Some(&v1));
    }

    value.push(v2.clone(), false).unwrap();
    {
        let list = match value { Value::List(ref value) => match value { &ValueList::Data(ref l) => l }, _ => panic!("Expected list") };
        assert_eq!(list.len(), 2);
        assert_eq!(list.back(), Some(&v1));
        assert_eq!(list.front(), Some(&v2));
    }
}

#[test]
fn lpop() {
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    value.push(v1.clone(), false).unwrap();
    value.push(v2.clone(), false).unwrap();
    assert_eq!(value.pop(false).unwrap(), Some(v2));
    assert_eq!(value.pop(false).unwrap(), Some(v1));
    assert_eq!(value.pop(false).unwrap(), None);
}

#[test]
fn lindex() {
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    value.push(v1.clone(), false).unwrap();
    value.push(v2.clone(), false).unwrap();

    assert_eq!(value.lindex(0).unwrap(), Some(&v2));
    assert_eq!(value.lindex(1).unwrap(), Some(&v1));
    assert_eq!(value.lindex(2).unwrap(), None);

    assert_eq!(value.lindex(-2).unwrap(), Some(&v2));
    assert_eq!(value.lindex(-1).unwrap(), Some(&v1));
    assert_eq!(value.lindex(-3).unwrap(), None);
}

#[test]
fn linsert() {
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 0, 1, 2];
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();

    assert_eq!(value.linsert(true, v2.clone(), v3.clone()).unwrap().unwrap(), 3);
    assert_eq!(value.lindex(0).unwrap(), Some(&v1));
    assert_eq!(value.lindex(1).unwrap(), Some(&v3));
    assert_eq!(value.lindex(2).unwrap(), Some(&v2));

    assert_eq!(value.linsert(true, vec![], v3.clone()).unwrap(), None);
}

#[test]
fn llen() {
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 0, 1, 2];
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();
    assert_eq!(value.llen().unwrap(), 2);

    value.push(v3.clone(), true).unwrap();
    assert_eq!(value.llen().unwrap(), 3);
}

#[test]
fn lrange() {
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 0, 1, 2];
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();
    value.push(v3.clone(), true).unwrap();

    assert_eq!(value.lrange(-100, 100).unwrap(), vec![&v1, &v2, &v3]);
    assert_eq!(value.lrange(0, 1).unwrap(), vec![&v1, &v2]);
    assert_eq!(value.lrange(0, 0).unwrap(), vec![&v1]);
    assert_eq!(value.lrange(1, -1).unwrap(), vec![&v2, &v3]);
}

#[test]
fn lrem_left_unlimited() {
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 0, 1, 2];
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();
    value.push(v3.clone(), true).unwrap();

    assert_eq!(value.lrem(true, 0, v3.clone()).unwrap(), 1);
    assert_eq!(value.llen().unwrap(), 2);
    assert_eq!(value.lrem(true, 0, v1.clone()).unwrap(), 1);
    assert_eq!(value.llen().unwrap(), 1);
    assert_eq!(value.lrem(true, 0, v2.clone()).unwrap(), 1);
    assert_eq!(value, Value::Nil);
}

#[test]
fn lrem_left_limited() {
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    value.push(v1.clone(), true).unwrap();
    value.push(v1.clone(), true).unwrap();
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();
    value.push(v1.clone(), true).unwrap();

    assert_eq!(value.lrem(true, 3, v1.clone()).unwrap(), 3);
    assert_eq!(value.llen().unwrap(), 2);
    {
        let list = match value { Value::List(ref value) => match value { &ValueList::Data(ref l) => l }, _ => panic!("Expected list") };
        assert_eq!(list.front().unwrap(), &v2);
    }
    assert_eq!(value.lrem(true, 3, v1.clone()).unwrap(), 1);
    assert_eq!(value.llen().unwrap(), 1);
}

#[test]
fn lrem_right_unlimited() {
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 0, 1, 2];
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();
    value.push(v3.clone(), true).unwrap();

    assert_eq!(value.lrem(false, 0, v3.clone()).unwrap(), 1);
    assert_eq!(value.llen().unwrap(), 2);
    assert_eq!(value.lrem(false, 0, v1.clone()).unwrap(), 1);
    assert_eq!(value.llen().unwrap(), 1);
    assert_eq!(value.lrem(false, 0, v2.clone()).unwrap(), 1);
    assert_eq!(value, Value::Nil);
}

#[test]
fn lrem_right_limited() {
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    value.push(v1.clone(), true).unwrap();
    value.push(v1.clone(), true).unwrap();
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();
    value.push(v1.clone(), true).unwrap();

    assert_eq!(value.lrem(false, 3, v1.clone()).unwrap(), 3);
    assert_eq!(value.llen().unwrap(), 2);
    {
        let list = match value { Value::List(ref value) => match value { &ValueList::Data(ref l) => l }, _ => panic!("Expected list") };
        assert_eq!(list.front().unwrap(), &v1);
    }
    assert_eq!(value.lrem(false, 3, v1.clone()).unwrap(), 1);
    assert_eq!(value.llen().unwrap(), 1);
}

#[test]
fn lset() {
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 0, 1, 2];
    let v4 = vec![3, 4, 5, 6];
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();
    value.push(v3.clone(), true).unwrap();

    assert_eq!(value.lset(1, v4.clone()).unwrap(), ());
    assert_eq!(value.lrange(0, -1).unwrap(), vec![&v1, &v4, &v3]);
    assert_eq!(value.lset(-1, v2.clone()).unwrap(), ());
    assert_eq!(value.lrange(0, -1).unwrap(), vec![&v1, &v4, &v2]);
}

#[test]
fn ltrim() {
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 0, 1, 2];
    let v4 = vec![3, 4, 5, 6];
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();
    value.push(v3.clone(), true).unwrap();
    value.push(v4.clone(), true).unwrap();

    assert_eq!(value.ltrim(1, 2).unwrap(), ());
    assert_eq!(value.llen().unwrap(), 2);
    assert_eq!(value.lrange(0, -1).unwrap(), vec![&v2, &v3]);
}
