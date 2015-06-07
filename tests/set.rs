extern crate rsedis;

use rsedis::database::Database;

#[test]
fn sadd() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];

    let mut el = database.get_or_create(0, &key);
    assert_eq!(el.sadd(Vec::clone(&value)).unwrap(), true);
    assert_eq!(el.sadd(Vec::clone(&value)).unwrap(), false);
}

#[test]
fn scard() {
    let mut database = Database::new();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];

    let mut el = database.get_or_create(0, &key);
    assert_eq!(el.scard().unwrap(), 0);
    assert_eq!(el.sadd(Vec::clone(&value)).unwrap(), true);
    assert_eq!(el.scard().unwrap(), 1);
}

#[test]
fn sdiff() {
    let mut database = Database::new();
    let key = vec![1u8];
    let key2 = vec![2u8];
    let value1 = vec![1u8, 2, 3, 4];
    let value2 = vec![1u8, 2, 3, 5];
    let value3 = vec![1u8, 2, 3, 6];

    {
        let mut el = database.get_or_create(0, &key);
        assert_eq!(el.sadd(Vec::clone(&value1)).unwrap(), true);
        assert_eq!(el.sadd(Vec::clone(&value2)).unwrap(), true);
    }
    {
        let mut el2 = database.get_or_create(0, &key2);
        assert_eq!(el2.sadd(Vec::clone(&value1)).unwrap(), true);
        assert_eq!(el2.sadd(Vec::clone(&value3)).unwrap(), true);
    }
    assert_eq!(database.get(0, &key).unwrap().sdiff(&vec![database.get(0, &key2).unwrap()]).unwrap().iter().collect::<Vec<_>>(),
            vec![&value2]);
}
