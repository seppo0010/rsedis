extern crate rsedis;

use std::collections::HashMap;
use std::collections::HashSet;
use std::str::from_utf8;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::thread;

use rsedis::database::Database;
use rsedis::database::Value;
use rsedis::parser::Parser;
use rsedis::parser::Argument;
use rsedis::command::command;
use rsedis::command::{Response, ResponseError};
use rsedis::util::mstime;

macro_rules! parser {
    ($str: expr) => ({
        let mut _args = Vec::new();
        let mut pos = 0;
        for segment in $str.split(|x| *x == b' ') {
            _args.push(Argument { pos: pos, len: segment.len() });
            pos += segment.len() + 1;
        }
        Parser::new($str, _args)
    })
}

fn getstr(database: &Database, key: &[u8]) -> String {
    match database.get(0, &key.to_vec()).unwrap() {
        &Value::Data(ref bytes) => return from_utf8(bytes).unwrap().to_owned(),
        &Value::Integer(i) => return format!("{}", i),
        _ => panic!("Got non-string"),
    }
    return String::new();
}

#[test]
fn nocommand() {
    let mut db = Database::new();
    let parser = Parser::new(b"", Vec::new());
    let response = command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    match response {
        Response::Error(_) => {},
        _ => assert!(false),
    };
}

#[test]
fn set_command() {
    let mut db = Database::new();
    let parser = parser!(b"set key value");
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    assert_eq!("value", getstr(&db, b"key"));

    assert_eq!(command(&parser!(b"set key2 value xx"), &mut db, &mut 0, None, None, None).unwrap(), Response::Nil);
    assert_eq!(command(&parser!(b"get key2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Nil);
    assert_eq!(command(&parser!(b"set key2 value nx"), &mut db, &mut 0, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    assert_eq!("value", getstr(&db, b"key2"));
    assert_eq!(command(&parser!(b"set key2 valuf xx"), &mut db, &mut 0, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    assert_eq!("valuf", getstr(&db, b"key2"));
    assert_eq!(command(&parser!(b"set key2 value nx"), &mut db, &mut 0, None, None, None).unwrap(), Response::Nil);
    assert_eq!("valuf", getstr(&db, b"key2"));

    assert_eq!(command(&parser!(b"set key3 value px 1234"), &mut db, &mut 0, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    let now = mstime();
    let exp = db.get_msexpiration(0, &b"key3".to_vec()).unwrap().clone();
    assert!(exp >= now + 1000);
    assert!(exp <= now + 1234);

    assert_eq!(command(&parser!(b"set key3 value ex 1234"), &mut db, &mut 0, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    let now = mstime();
    let exp = db.get_msexpiration(0, &b"key3".to_vec()).unwrap().clone();
    assert!(exp >= now + 1233 * 1000);
    assert!(exp <= now + 1234 * 1000);
}

#[test]
fn get_command() {
    let mut db = Database::new();
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    let parser = parser!(b"get key");
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Data("value".to_owned().into_bytes()));
    assert_eq!("value", getstr(&db, b"key"));
}

#[test]
fn del_command() {
    let mut db = Database::new();
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    let parser = parser!(b"del key key2");
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
}

#[test]
fn exists_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"exists key");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    }
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    {
        let parser = parser!(b"exists key");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    }
}

#[test]
fn expire_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"expire key 100");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    }
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    {
        let parser = parser!(b"expire key 100");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    }
    let now = mstime();
    let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
    assert!(exp >= now);
    assert!(exp <= now + 100 * 1000);
}

#[test]
fn pexpire_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"pexpire key 100");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    }
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    {
        let parser = parser!(b"pexpire key 100");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    }
    let now = mstime();
    let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
    assert!(exp >= now);
    assert!(exp <= now + 100);
}

#[test]
fn expireat_command() {
    let mut db = Database::new();
    let now = mstime() / 1000;
    let exp_exp = now + 100;
    let qs = format!("expireat key {}", exp_exp);
    let q = qs.as_bytes();
    {
        let parser = parser!(q);
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    }
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    {
        let parser = parser!(q);
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    }
    let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
    assert_eq!(exp, exp_exp * 1000);
}

#[test]
fn pexpireat_command() {
    let mut db = Database::new();
    let now = mstime();
    let exp_exp = now + 100;
    let qs = format!("pexpireat key {}", exp_exp);
    let q = qs.as_bytes();
    {
        let parser = parser!(q);
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    }
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    {
        let parser = parser!(q);
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    }
    let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
    assert_eq!(exp, exp_exp);
}

#[test]
fn ttl_command() {
    let mut db = Database::new();
    let parser = parser!(b"ttl key");
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(-2));
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(-1));
    db.set_msexpiration(0, b"key".to_vec(), mstime() + 100 * 1000);
    match command(&parser, &mut db, &mut 0, None, None, None).unwrap() {
        Response::Integer(i) => assert!(i <= 100 && i > 80),
        _ => panic!("Expected integer"),
    }
}

#[test]
fn pttl_command() {
    let mut db = Database::new();
    let parser = parser!(b"pttl key");
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(-2));
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(-1));
    db.set_msexpiration(0, b"key".to_vec(), mstime() + 100 * 1000);
    match command(&parser, &mut db, &mut 0, None, None, None).unwrap() {
        Response::Integer(i) => assert!(i <= 100 * 1000 && i > 80 * 1000),
        _ => panic!("Expected integer"),
    }
}

#[test]
fn persist_command() {
    let mut db = Database::new();
    let parser = parser!(b"persist key");
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    db.set_msexpiration(0, b"key".to_vec(), mstime() + 100 * 1000);
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
}

#[test]
fn type_command() {
    let mut db = Database::new();
    let parser = parser!(b"type key");
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Data(b"none".to_vec()));

    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Data(b"string".to_vec()));
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"1".to_vec()).is_ok());
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Data(b"string".to_vec()));

    assert!(db.remove(0, &b"key".to_vec()).is_some());
    assert!(db.get_or_create(0, &b"key".to_vec()).push(b"1".to_vec(), true).is_ok());
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Data(b"list".to_vec()));

    assert!(db.remove(0, &b"key".to_vec()).is_some());
    assert!(db.get_or_create(0, &b"key".to_vec()).sadd(b"1".to_vec()).is_ok());
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Data(b"set".to_vec()));

    // TODO: zset and hash
}

#[test]
fn serialize_status() {
    let response = Response::Status("OK".to_owned());
    assert_eq!(response.as_bytes(), b"+OK\r\n");
}

#[test]
fn serialize_error() {
    let response = Response::Error("ERR Invalid command".to_owned());
    assert_eq!(response.as_bytes(), b"-ERR Invalid command\r\n");
}

#[test]
fn serialize_string() {
    let response = Response::Data(b"ERR Invalid command".to_vec());
    assert_eq!(response.as_bytes(), b"$19\r\nERR Invalid command\r\n");
}

#[test]
fn serialize_nil() {
    let response = Response::Nil;
    assert_eq!(response.as_bytes(), b"$-1\r\n");
}

#[test]
fn serialize_integer() {
    let response = Response::Integer(123);
    assert_eq!(response.as_bytes(), b":123\r\n");
}

#[test]
fn append_command() {
    let mut db = Database::new();
    let parser = parser!(b"append key value");
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(5));

    let parser = parser!(b"append key value");
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(10));

    assert_eq!(db.get(0, &b"key".to_vec()).unwrap(), &Value::Data(b"valuevalue".to_vec()));
}

#[test]
fn incr_command() {
    let mut db = Database::new();
    let parser = parser!(b"incr key");
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
}

#[test]
fn incrby_command() {
    let mut db = Database::new();
    let parser = parser!(b"incrby key 5");
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(5));
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(10));
}

#[test]
fn decr_command() {
    let mut db = Database::new();
    let parser = parser!(b"decr key");
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(-1));
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(-2));
}

#[test]
fn decrby_command() {
    let mut db = Database::new();
    let parser = parser!(b"decrby key 5");
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(-5));
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(-10));
}

#[test]
fn lpush_command() {
    let mut db = Database::new();
    let parser = parser!(b"lpush key value");
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
}

#[test]
fn rpush_command() {
    let mut db = Database::new();
    let parser = parser!(b"rpush key value");
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
}

#[test]
fn lpop_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"rpush key value");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"rpush key valuf");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"lpop key");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Data("value".to_owned().into_bytes()));
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Data("valuf".to_owned().into_bytes()));
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Nil);
    }
}

#[test]
fn rpop_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"rpush key value");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"rpush key valuf");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"rpop key");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Data("valuf".to_owned().into_bytes()));
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Data("value".to_owned().into_bytes()));
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Nil);
    }
}

#[test]
fn lindex_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"rpush key value");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"rpush key valuf");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"lindex key 0");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Data("value".to_owned().into_bytes()));
    }
}

#[test]
fn linsert_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"rpush key value");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"rpush key valug");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"linsert key before valug valuf");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));
    }
}

#[test]
fn llen_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"rpush key value");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"llen key");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
    }
}

#[test]
fn lpushx_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"lpushx key value");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    }
    {
        let parser = parser!(b"lpush key value");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    }
    {
        let parser = parser!(b"lpushx key value");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
    }
}

#[test]
fn lrange_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"rpush key value");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"rpush key valuf");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"rpush key valug");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"lrange key 0 -1");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                    Response::Data("value".to_owned().into_bytes()),
                    Response::Data("valuf".to_owned().into_bytes()),
                    Response::Data("valug".to_owned().into_bytes()),
                    ]));
    }
}

#[test]
fn lrem_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"rpush key value");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"lrem key 2 value");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
    }
    {
        let parser = parser!(b"llen key");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
    }
}

#[test]
fn lset_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"rpush key value");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"lset key 2 valuf");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    }
    {
        let parser = parser!(b"lrange key 2 2");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                    Response::Data("valuf".to_owned().into_bytes()),
                    ]));
    }
}

#[test]
fn rpoplpush_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"rpush key value");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"rpush key valuf");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"llen key2");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    }
    {
        let parser = parser!(b"rpoplpush key key2");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Data("valuf".to_owned().into_bytes()));
    }
    {
        let parser = parser!(b"llen key");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    }
    {
        let parser = parser!(b"llen key2");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    }
    {
        let parser = parser!(b"rpoplpush key key2");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Data("value".to_owned().into_bytes()));
    }
    {
        let parser = parser!(b"llen key");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    }
    {
        let parser = parser!(b"llen key2");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
    }
}

#[test]
fn brpoplpush_nowait() {
    let mut db = Database::new();
    {
        let parser = parser!(b"rpush key value");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"rpush key valuf");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"llen key2");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    }
    {
        let parser = parser!(b"brpoplpush key key2 0");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Data("valuf".to_owned().into_bytes()));
    }
}

#[test]
fn brpoplpush_waiting() {
    let db = Arc::new(Mutex::new(Database::new()));
    let (tx, rx) = channel();
    let db2 = db.clone();
    thread::spawn(move || {
        let parser = parser!(b"brpoplpush key1 key2 0");
        let r = match command(&parser, &mut db.lock().unwrap(), &mut 0, None, None, None).unwrap_err() {
            ResponseError::Wait(receiver) => {
                tx.send(1).unwrap();
                receiver
            }
            _ => panic!("Unexpected error")
        };
        r.recv().unwrap();
        assert_eq!(command(&parser, &mut db.lock().unwrap(), &mut 0, None, None, None).unwrap(),
            Response::Data("value".to_owned().into_bytes()));
        tx.send(2).unwrap();
    });
    assert_eq!(rx.recv().unwrap(), 1);

    {
        let parser = parser!(b"rpush key1 value");
        command(&parser, &mut db2.lock().unwrap(), &mut 0, None, None, None).unwrap();
        assert_eq!(rx.recv().unwrap(), 2);
    }
    {
        let parser = parser!(b"lrange key2 0 -1");
        assert_eq!(command(&parser, &mut db2.lock().unwrap(), &mut 0, None, None, None).unwrap(), Response::Array(vec![
                    Response::Data("value".to_owned().into_bytes()),
                    ]));
    }
}

#[test]
fn brpoplpush_timeout() {
    let mut db = Database::new();
    {
        let parser = parser!(b"brpoplpush key key2 1");
        let receiver = match command(&parser, &mut db, &mut 0, None, None, None).unwrap_err() {
            ResponseError::Wait(receiver) => receiver,
            _ => panic!("Unexpected response"),
        };
        assert!(receiver.try_recv().is_err());
        thread::sleep_ms(1400);
        assert_eq!(receiver.try_recv().unwrap(), false);
    }
}

#[test]
fn ltrim_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"rpush key value");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"rpush key valuf");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"ltrim key 1 -2");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    }
    {
        let parser = parser!(b"lrange key 0 -1");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                    Response::Data("value".to_owned().into_bytes()),
                    Response::Data("valuf".to_owned().into_bytes()),
                    ]));
    }
}

#[test]
fn sadd_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"sadd key 1 1 1 2 3");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    }
}

#[test]
fn scard_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"sadd key 1 2 3");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"scard key");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));
    }
}

#[test]
fn sdiff_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"sadd key 1 2 3");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"sdiff key");
        let arr = match command(&parser, &mut db, &mut 0, None, None, None).unwrap() {
            Response::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let mut r = arr.iter().map(|el| match el {
            &Response::Data(ref el) => el.clone(),
            _ => panic!("Expected data"),
        }).collect::<Vec<_>>();
        r.sort();
        assert_eq!(r, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
    }
}

#[test]
fn sdiffstore_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"sadd key 1 2 3");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"sdiffstore target key");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));
    }
    {
        let mut set = HashSet::new();
        set.insert(b"1".to_vec());
        set.insert(b"2".to_vec());
        set.insert(b"3".to_vec());
        assert_eq!(db.get(0, &b"target".to_vec()).unwrap(), &Value::Set(set));
    }
}

#[test]
fn sdiff2_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"sadd key 1 2 3");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"sadd key2 2 3");
        command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    }
    {
        let parser = parser!(b"sdiff key key2");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(),
            Response::Array(vec![Response::Data(b"1".to_vec()),]));
    }
}

#[test]
fn select_command() {
    let mut db = Database::new();
    {
        let mut dbindex = 0;
        let parser = parser!(b"select 1");
        command(&parser, &mut db, &mut dbindex, None, None, None).unwrap();
        assert_eq!(dbindex, 1);
    }
}

#[test]
fn flushdb_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"set key value");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    }
    {
        let parser = parser!(b"set key valuf");
        assert_eq!(command(&parser, &mut db, &mut 1, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    }
    {
        let parser = parser!(b"flushdb");
        assert_eq!(command(&parser, &mut db, &mut 1, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    }
    {
        let parser = parser!(b"get key");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Data("value".to_owned().into_bytes()));
    }
    {
        let parser = parser!(b"get key");
        assert_eq!(command(&parser, &mut db, &mut 1, None, None, None).unwrap(), Response::Nil);
    }
}

#[test]
fn flushall_command() {
    let mut db = Database::new();
    {
        let parser = parser!(b"set key value");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    }
    {
        let parser = parser!(b"set key valuf");
        assert_eq!(command(&parser, &mut db, &mut 1, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    }
    {
        let parser = parser!(b"flushall");
        assert_eq!(command(&parser, &mut db, &mut 1, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    }
    {
        let parser = parser!(b"get key");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Nil);
    }
    {
        let parser = parser!(b"get key");
        assert_eq!(command(&parser, &mut db, &mut 1, None, None, None).unwrap(), Response::Nil);
    }
}

#[test]
fn subscribe_publish_command() {
    let mut db = Database::new();
    let mut subscriptions = HashMap::new();
    let mut psubscriptions = HashMap::new();
    let (tx, rx) = channel();

    {
        let parser = parser!(b"subscribe channel");
        assert!(command(&parser, &mut db, &mut 0, Some(&mut subscriptions), Some(&mut psubscriptions), Some(&tx)).is_err());
    }

    {
        let parser = parser!(b"publish channel hello-world");
        assert_eq!(command(&parser, &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    }

    {
        let parser = parser!(b"unsubscribe channel");
        assert!(command(&parser, &mut db, &mut 0, Some(&mut subscriptions), Some(&mut psubscriptions), Some(&tx)).is_err());
    }

    assert_eq!(rx.try_recv().unwrap().as_response(),
            Response::Array(vec![
                Response::Data(b"subscribe".to_vec()),
                Response::Data(b"channel".to_vec()),
                Response::Integer(1),
                ])
        );
    assert_eq!(rx.try_recv().unwrap().as_response(),
            Response::Array(vec![
                Response::Data(b"message".to_vec()),
                Response::Data(b"channel".to_vec()),
                Response::Data(b"hello-world".to_vec()),
                ])
        );
    assert_eq!(rx.try_recv().unwrap().as_response(),
            Response::Array(vec![
                Response::Data(b"unsubscribe".to_vec()),
                Response::Data(b"channel".to_vec()),
                Response::Integer(0),
                ])
        );
}
