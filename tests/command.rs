extern crate rsedis;

use std::collections::HashMap;
use std::collections::HashSet;
use std::str::from_utf8;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::thread;

use rsedis::database::Database;
use rsedis::database::{Value, ValueString, ValueSet};
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
        &Value::String(ref value) => match value {
            &ValueString::Data(ref bytes) => from_utf8(bytes).unwrap().to_owned(),
            &ValueString::Integer(i) => format!("{}", i),
        },
        _ => panic!("Got non-string"),
    }
}

#[test]
fn nocommand() {
    let mut db = Database::mock();
    let parser = Parser::new(b"", Vec::new());
    let response = command(&parser, &mut db, &mut 0, None, None, None).unwrap();
    match response {
        Response::Error(_) => {},
        _ => assert!(false),
    };
}

#[test]
fn set_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"set key value"), &mut db, &mut 0, None, None, None).unwrap(), Response::Status("OK".to_owned()));
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
fn setnx_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"setnx key value"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!("value", getstr(&db, b"key"));
    assert_eq!(command(&parser!(b"setnx key valuf"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert_eq!("value", getstr(&db, b"key"));
}

#[test]
fn setex_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"setex key 1234 value"), &mut db, &mut 0, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    let now = mstime();
    let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
    assert!(exp >= now + 1233 * 1000);
    assert!(exp <= now + 1234 * 1000);
}

#[test]
fn psetex_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"psetex key 1234 value"), &mut db, &mut 0, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    let now = mstime();
    let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
    assert!(exp >= now + 1000);
    assert!(exp <= now + 1234);
}

#[test]
fn get_command() {
    let mut db = Database::mock();
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser!(b"get key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data("value".to_owned().into_bytes()));
    assert_eq!("value", getstr(&db, b"key"));
}

#[test]
fn mget_command() {
    let mut db = Database::mock();
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser!(b"mget key key2"), &mut db, &mut 0, None, None, None).unwrap(),
            Response::Array(
            vec![
                Response::Data("value".to_owned().into_bytes()),
                Response::Nil,
            ])
            );
}

#[test]
fn getrange_command() {
    let mut db = Database::mock();
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser!(b"getrange key 1 -2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data("alu".to_owned().into_bytes()));
}

#[test]
fn setrange_command() {
    let mut db = Database::mock();
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser!(b"setrange key 1 i"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(5));
    assert_eq!("vilue", getstr(&db, b"key"));
}

#[test]
fn setbit_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"setbit key 1 0"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert_eq!(command(&parser!(b"setbit key 1 1"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert_eq!("@", getstr(&db, b"key"));
    assert_eq!(command(&parser!(b"setbit key 1 0"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
}

#[test]
fn getbit_command() {
    let mut db = Database::mock();
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser!(b"getbit key 4"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert_eq!(command(&parser!(b"getbit key 5"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!(command(&parser!(b"getbit key 6"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!(command(&parser!(b"getbit key 7"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert_eq!(command(&parser!(b"getbit key 800"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
}

#[test]
fn strlen_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"strlen key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser!(b"strlen key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(5));
}

#[test]
fn del_command() {
    let mut db = Database::mock();
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser!(b"del key key2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
}

#[test]
fn exists_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"exists key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser!(b"exists key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
}

#[test]
fn expire_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"expire key 100"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser!(b"expire key 100"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    let now = mstime();
    let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
    assert!(exp >= now);
    assert!(exp <= now + 100 * 1000);
}

#[test]
fn pexpire_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"pexpire key 100"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser!(b"pexpire key 100"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    let now = mstime();
    let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
    assert!(exp >= now);
    assert!(exp <= now + 100);
}

#[test]
fn expireat_command() {
    let mut db = Database::mock();
    let now = mstime() / 1000;
    let exp_exp = now + 100;
    let qs = format!("expireat key {}", exp_exp);
    let q = qs.as_bytes();
    assert_eq!(command(&parser!(q), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser!(q), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
    assert_eq!(exp, exp_exp * 1000);
}

#[test]
fn pexpireat_command() {
    let mut db = Database::mock();
    let now = mstime();
    let exp_exp = now + 100;
    let qs = format!("pexpireat key {}", exp_exp);
    let q = qs.as_bytes();
    assert_eq!(command(&parser!(q), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser!(q), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
    assert_eq!(exp, exp_exp);
}

#[test]
fn ttl_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"ttl key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(-2));
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser!(b"ttl key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(-1));
    db.set_msexpiration(0, b"key".to_vec(), mstime() + 100 * 1000);
    match command(&parser!(b"ttl key"), &mut db, &mut 0, None, None, None).unwrap() {
        Response::Integer(i) => assert!(i <= 100 && i > 80),
        _ => panic!("Expected integer"),
    }
}

#[test]
fn pttl_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"pttl key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(-2));
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser!(b"pttl key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(-1));
    db.set_msexpiration(0, b"key".to_vec(), mstime() + 100 * 1000);
    match command(&parser!(b"pttl key"), &mut db, &mut 0, None, None, None).unwrap() {
        Response::Integer(i) => assert!(i <= 100 * 1000 && i > 80 * 1000),
        _ => panic!("Expected integer"),
    }
}

#[test]
fn persist_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"persist key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser!(b"persist key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    db.set_msexpiration(0, b"key".to_vec(), mstime() + 100 * 1000);
    assert_eq!(command(&parser!(b"persist key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
}

#[test]
fn type_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"type key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data(b"none".to_vec()));

    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    assert_eq!(command(&parser!(b"type key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data(b"string".to_vec()));
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"1".to_vec()).is_ok());
    assert_eq!(command(&parser!(b"type key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data(b"string".to_vec()));

    assert!(db.remove(0, &b"key".to_vec()).is_some());
    assert!(db.get_or_create(0, &b"key".to_vec()).push(b"1".to_vec(), true).is_ok());
    assert_eq!(command(&parser!(b"type key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data(b"list".to_vec()));

    assert!(db.remove(0, &b"key".to_vec()).is_some());
    assert!(db.get_or_create(0, &b"key".to_vec()).sadd(b"1".to_vec()).is_ok());
    assert_eq!(command(&parser!(b"type key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data(b"set".to_vec()));

    assert!(db.remove(0, &b"key".to_vec()).is_some());
    assert!(db.get_or_create(0, &b"key".to_vec()).zadd(3.0, b"1".to_vec(), false, false, false, false).is_ok());
    assert_eq!(command(&parser!(b"type key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data(b"zset".to_vec()));

    // TODO: hash
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
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"append key value"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(5));
    assert_eq!(command(&parser!(b"append key value"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(10));
    assert_eq!(db.get(0, &b"key".to_vec()).unwrap(), &Value::String(ValueString::Data(b"valuevalue".to_vec())));
}

#[test]
fn incr_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"incr key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!(command(&parser!(b"incr key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
}

#[test]
fn incrby_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"incrby key 5"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(5));
    assert_eq!(command(&parser!(b"incrby key 5"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(10));
}

#[test]
fn decr_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"decr key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(-1));
    assert_eq!(command(&parser!(b"decr key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(-2));
}

#[test]
fn decrby_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"decrby key 5"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(-5));
    assert_eq!(command(&parser!(b"decrby key 5"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(-10));
}

#[test]
fn lpush_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"lpush key value"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!(command(&parser!(b"lpush key value"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
}

#[test]
fn rpush_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!(command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
}

#[test]
fn lpop_command() {
    let mut db = Database::mock();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key valuf"), &mut db, &mut 0, None, None, None).unwrap();
    assert_eq!(command(&parser!(b"lpop key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data("value".to_owned().into_bytes()));
    assert_eq!(command(&parser!(b"lpop key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data("valuf".to_owned().into_bytes()));
    assert_eq!(command(&parser!(b"lpop key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Nil);
}

#[test]
fn rpop_command() {
    let mut db = Database::mock();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key valuf"), &mut db, &mut 0, None, None, None).unwrap();
    assert_eq!(command(&parser!(b"rpop key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data("valuf".to_owned().into_bytes()));
    assert_eq!(command(&parser!(b"rpop key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data("value".to_owned().into_bytes()));
    assert_eq!(command(&parser!(b"rpop key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Nil);
}

#[test]
fn lindex_command() {
    let mut db = Database::mock();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key valuf"), &mut db, &mut 0, None, None, None).unwrap();
    assert_eq!(command(&parser!(b"lindex key 0"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data("value".to_owned().into_bytes()));
}

#[test]
fn linsert_command() {
    let mut db = Database::mock();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key valug"), &mut db, &mut 0, None, None, None).unwrap();
    assert_eq!(command(&parser!(b"linsert key before valug valuf"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));
}

#[test]
fn llen_command() {
    let mut db = Database::mock();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    assert_eq!(command(&parser!(b"llen key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
}

#[test]
fn lpushx_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"lpushx key value"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert_eq!(command(&parser!(b"lpush key value"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!(command(&parser!(b"lpushx key value"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
}

#[test]
fn lrange_command() {
    let mut db = Database::mock();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key valuf"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key valug"), &mut db, &mut 0, None, None, None).unwrap();
    assert_eq!(command(&parser!(b"lrange key 0 -1"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data("value".to_owned().into_bytes()),
                Response::Data("valuf".to_owned().into_bytes()),
                Response::Data("valug".to_owned().into_bytes()),
                ]));
}

#[test]
fn lrem_command() {
    let mut db = Database::mock();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    assert_eq!(command(&parser!(b"lrem key 2 value"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
    assert_eq!(command(&parser!(b"llen key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
}

#[test]
fn lset_command() {
    let mut db = Database::mock();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    assert_eq!(command(&parser!(b"lset key 2 valuf"), &mut db, &mut 0, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    assert_eq!(command(&parser!(b"lrange key 2 2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data("valuf".to_owned().into_bytes()),
                ]));
}

#[test]
fn rpoplpush_command() {
    let mut db = Database::mock();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key valuf"), &mut db, &mut 0, None, None, None).unwrap();
    assert_eq!(command(&parser!(b"llen key2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert_eq!(command(&parser!(b"rpoplpush key key2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data("valuf".to_owned().into_bytes()));
    assert_eq!(command(&parser!(b"llen key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!(command(&parser!(b"llen key2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!(command(&parser!(b"rpoplpush key key2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data("value".to_owned().into_bytes()));
    assert_eq!(command(&parser!(b"llen key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert_eq!(command(&parser!(b"llen key2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
}

#[test]
fn brpoplpush_nowait() {
    let mut db = Database::mock();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key valuf"), &mut db, &mut 0, None, None, None).unwrap();
    assert_eq!(command(&parser!(b"llen key2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert_eq!(command(&parser!(b"brpoplpush key key2 0"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data("valuf".to_owned().into_bytes()));
}

#[test]
fn brpoplpush_waiting() {
    let db = Arc::new(Mutex::new(Database::mock()));
    let (tx, rx) = channel();
    let db2 = db.clone();
    thread::spawn(move || {
        let r = match command(&parser!(b"brpoplpush key1 key2 0"), &mut db.lock().unwrap(), &mut 0, None, None, None).unwrap_err() {
            ResponseError::Wait(receiver) => {
                tx.send(1).unwrap();
                receiver
            }
            _ => panic!("Unexpected error")
        };
        r.recv().unwrap();
        assert_eq!(command(&parser!(b"brpoplpush key1 key2 0"), &mut db.lock().unwrap(), &mut 0, None, None, None).unwrap(),
            Response::Data("value".to_owned().into_bytes()));
        tx.send(2).unwrap();
    });
    assert_eq!(rx.recv().unwrap(), 1);

    command(&parser!(b"rpush key1 value"), &mut db2.lock().unwrap(), &mut 0, None, None, None).unwrap();
    assert_eq!(rx.recv().unwrap(), 2);
    assert_eq!(command(&parser!(b"lrange key2 0 -1"), &mut db2.lock().unwrap(), &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data("value".to_owned().into_bytes()),
                ]));
}

#[test]
fn brpoplpush_timeout() {
    let mut db = Database::mock();
    let receiver = match command(&parser!(b"brpoplpush key key2 1"), &mut db, &mut 0, None, None, None).unwrap_err() {
        ResponseError::Wait(receiver) => receiver,
        _ => panic!("Unexpected response"),
    };
    assert!(receiver.try_recv().is_err());
    thread::sleep_ms(1400);
    assert_eq!(receiver.try_recv().unwrap(), false);
}


#[test]
fn brpop_nowait() {
    let mut db = Database::mock();
    command(&parser!(b"rpush key1 value"), &mut db, &mut 0, None, None, None).unwrap();
    assert_eq!(command(&parser!(b"brpop key1 key2 0"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data("key1".to_owned().into_bytes()),
                Response::Data("value".to_owned().into_bytes()),
                ]));
}

#[test]
fn brpop_waiting() {
    let db = Arc::new(Mutex::new(Database::mock()));
    let (tx, rx) = channel();
    let db2 = db.clone();
    thread::spawn(move || {
        let r = match command(&parser!(b"brpop key1 key2 0"), &mut db.lock().unwrap(), &mut 0, None, None, None).unwrap_err() {
            ResponseError::Wait(receiver) => {
                tx.send(1).unwrap();
                receiver
            }
            _ => panic!("Unexpected error")
        };
        r.recv().unwrap();
        assert_eq!(command(&parser!(b"brpop key1 key2 0"), &mut db.lock().unwrap(), &mut 0, None, None, None).unwrap(),
            Response::Array(vec![
                Response::Data("key2".to_owned().into_bytes()),
                Response::Data("value".to_owned().into_bytes()),
                ]));
        tx.send(2).unwrap();
    });
    assert_eq!(rx.recv().unwrap(), 1);

    {
        command(&parser!(b"rpush key2 value"), &mut db2.lock().unwrap(), &mut 0, None, None, None).unwrap();
        assert_eq!(rx.recv().unwrap(), 2);
    }

    {
        assert_eq!(command(&parser!(b"llen key2"), &mut db2.lock().unwrap(), &mut 0, None, None, None).unwrap(), Response::Integer(0));
    }
}

#[test]
fn brpop_timeout() {
    let mut db = Database::mock();
    let receiver = match command(&parser!(b"brpop key1 key2 1"), &mut db, &mut 0, None, None, None).unwrap_err() {
        ResponseError::Wait(receiver) => receiver,
        _ => panic!("Unexpected response"),
    };
    assert!(receiver.try_recv().is_err());
    thread::sleep_ms(1400);
    assert_eq!(receiver.try_recv().unwrap(), false);
}

#[test]
fn ltrim_command() {
    let mut db = Database::mock();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key value"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key valuf"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"rpush key valuf"), &mut db, &mut 0, None, None, None).unwrap();
    assert_eq!(command(&parser!(b"ltrim key 1 -2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    assert_eq!(command(&parser!(b"lrange key 0 -1"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data("value".to_owned().into_bytes()),
                Response::Data("valuf".to_owned().into_bytes()),
                ]));
}

#[test]
fn sadd_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"sadd key 1 1 1 2 3"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));
    assert_eq!(command(&parser!(b"sadd key 1 1 1 2 3"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
}

#[test]
fn srem_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"sadd key 1 1 1 2 3"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));
    assert_eq!(command(&parser!(b"srem key 2 3 4"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
    assert_eq!(command(&parser!(b"srem key 2 3 4"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
}

#[test]
fn sismember_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"sadd key 1 2 3"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));
    assert_eq!(command(&parser!(b"sismember key 2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!(command(&parser!(b"sismember key 4"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
}

#[test]
fn smembers_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"sadd key 1 2 3"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));
    match command(&parser!(b"smembers key"), &mut db, &mut 0, None, None, None).unwrap() {
        Response::Array(ref arr) => {
            let mut array = arr.iter().map(|x| match x {
                &Response::Data(ref d) => d.clone(),
                _ => panic!("Expected data"),
            }).collect::<Vec<_>>();
            array.sort_by(|a, b| a.cmp(b));
            assert_eq!(array, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
        },
        _ => panic!("Expected array"),
    }
}

#[test]
fn srandmember_command1() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"sadd key 1 2 3"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));
    let r = command(&parser!(b"srandmember key"), &mut db, &mut 0, None, None, None).unwrap();
    assert!(r == Response::Data(b"1".to_vec()) ||
            r == Response::Data(b"2".to_vec()) ||
            r == Response::Data(b"3".to_vec())
            );
    assert_eq!(command(&parser!(b"scard key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));
}

#[test]
fn srandmember_command2() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"sadd key 1 2 3"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));
    let r = command(&parser!(b"srandmember key 1"), &mut db, &mut 0, None, None, None).unwrap();
    assert!(r == Response::Array(vec![Response::Data(b"1".to_vec())]) ||
            r == Response::Array(vec![Response::Data(b"2".to_vec())]) ||
            r == Response::Array(vec![Response::Data(b"3".to_vec())])
            );
    assert_eq!(command(&parser!(b"scard key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));
}

#[test]
fn spop_command1() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"sadd key 1 2 3"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));
    let r = command(&parser!(b"spop key"), &mut db, &mut 0, None, None, None).unwrap();
    assert!(r == Response::Data(b"1".to_vec()) ||
            r == Response::Data(b"2".to_vec()) ||
            r == Response::Data(b"3".to_vec())
            );
    assert_eq!(command(&parser!(b"scard key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
}

#[test]
fn spop_command2() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"sadd key 1 2 3"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));
    let r = command(&parser!(b"spop key 1"), &mut db, &mut 0, None, None, None).unwrap();
    assert!(r == Response::Array(vec![Response::Data(b"1".to_vec())]) ||
            r == Response::Array(vec![Response::Data(b"2".to_vec())]) ||
            r == Response::Array(vec![Response::Data(b"3".to_vec())])
            );
    assert_eq!(command(&parser!(b"scard key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
}

#[test]
fn smove_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"sadd k1 1 2 3"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));

    assert_eq!(command(&parser!(b"smove k1 k2 1"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!(command(&parser!(b"smove k1 k2 1"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));

    assert_eq!(command(&parser!(b"smove k1 k2 2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!(command(&parser!(b"smove k1 k2 2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));

    assert_eq!(command(&parser!(b"smove k1 k2 5"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));

    assert_eq!(command(&parser!(b"set k3 value"), &mut db, &mut 0, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    assert_eq!(command(&parser!(b"smove k1 k3 3"), &mut db, &mut 0, None, None, None).unwrap(), Response::Error("Invalid destination".to_owned()));
}

#[test]
fn scard_command() {
    let mut db = Database::mock();
    command(&parser!(b"sadd key 1 2 3"), &mut db, &mut 0, None, None, None).unwrap();
    assert_eq!(command(&parser!(b"scard key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));
}

#[test]
fn sdiff_command() {
    let mut db = Database::mock();
    command(&parser!(b"sadd key 1 2 3"), &mut db, &mut 0, None, None, None).unwrap();

    let arr = match command(&parser!(b"sdiff key"), &mut db, &mut 0, None, None, None).unwrap() {
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

#[test]
fn sdiffstore_command() {
    let mut db = Database::mock();
    command(&parser!(b"sadd key1 1 2 3"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"sadd key2 3 4 5"), &mut db, &mut 0, None, None, None).unwrap();
    assert_eq!(command(&parser!(b"sdiffstore target key1 key2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));

    let set = vec![b"1".to_vec(), b"2".to_vec()].iter().cloned().collect::<HashSet<_>>();
    assert_eq!(db.get(0, &b"target".to_vec()).unwrap(), &Value::Set(ValueSet::Data(set)));
}

#[test]
fn sdiff2_command() {
    let mut db = Database::mock();
    command(&parser!(b"sadd key1 1 2 3"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"sadd key2 2 3"), &mut db, &mut 0, None, None, None).unwrap();
    assert_eq!(command(&parser!(b"sdiff key1 key2"), &mut db, &mut 0, None, None, None).unwrap(),
        Response::Array(vec![Response::Data(b"1".to_vec()),]));
}

#[test]
fn sinter_command() {
    let mut db = Database::mock();
    command(&parser!(b"sadd key 1 2 3"), &mut db, &mut 0, None, None, None).unwrap();

    let arr = match command(&parser!(b"sinter key"), &mut db, &mut 0, None, None, None).unwrap() {
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

#[test]
fn sinter2_command() {
    let mut db = Database::mock();
    command(&parser!(b"sadd key1 1 2 3"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"sadd key2 2 3 4 5"), &mut db, &mut 0, None, None, None).unwrap();

    let arr = match command(&parser!(b"sinter key1 key2"), &mut db, &mut 0, None, None, None).unwrap() {
        Response::Array(arr) => arr,
        _ => panic!("Expected array"),
    };
    let mut r = arr.iter().map(|el| match el {
        &Response::Data(ref el) => el.clone(),
        _ => panic!("Expected data"),
    }).collect::<Vec<_>>();
    r.sort();
    assert_eq!(r, vec![b"2".to_vec(), b"3".to_vec()]);
}

#[test]
fn sinterstore_command() {
    let mut db = Database::mock();
    command(&parser!(b"sadd key1 1 2 3"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"sadd key2 2 3 5"), &mut db, &mut 0, None, None, None).unwrap();
    assert_eq!(command(&parser!(b"sinterstore target key1 key2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));

    let set = vec![b"3".to_vec(), b"2".to_vec()].iter().cloned().collect::<HashSet<_>>();
    assert_eq!(db.get(0, &b"target".to_vec()).unwrap(), &Value::Set(ValueSet::Data(set)));
}

#[test]
fn sunion_command() {
    let mut db = Database::mock();
    command(&parser!(b"sadd key 1 2 3"), &mut db, &mut 0, None, None, None).unwrap();

    let arr = match command(&parser!(b"sunion key"), &mut db, &mut 0, None, None, None).unwrap() {
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

#[test]
fn sunion2_command() {
    let mut db = Database::mock();
    command(&parser!(b"sadd key1 1 2 3"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"sadd key2 2 3 4"), &mut db, &mut 0, None, None, None).unwrap();

    let arr = match command(&parser!(b"sunion key1 key2"), &mut db, &mut 0, None, None, None).unwrap() {
        Response::Array(arr) => arr,
        _ => panic!("Expected array"),
    };
    let mut r = arr.iter().map(|el| match el {
        &Response::Data(ref el) => el.clone(),
        _ => panic!("Expected data"),
    }).collect::<Vec<_>>();
    r.sort();
    assert_eq!(r, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec(), b"4".to_vec()]);
}

#[test]
fn sunionstore_command() {
    let mut db = Database::mock();
    command(&parser!(b"sadd key1 1 2 3"), &mut db, &mut 0, None, None, None).unwrap();
    command(&parser!(b"sadd key2 2 3 4"), &mut db, &mut 0, None, None, None).unwrap();
    assert_eq!(command(&parser!(b"sunionstore target key1 key2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(4));

    let set = vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec(), b"4".to_vec()].iter().cloned().collect::<HashSet<_>>();
    assert_eq!(db.get(0, &b"target".to_vec()).unwrap(), &Value::Set(ValueSet::Data(set)));
}

#[test]
fn zadd_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"zadd key 1 a 2 b"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
    assert_eq!(command(&parser!(b"zadd key 1 a 2 b"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert_eq!(command(&parser!(b"zadd key XX 2 a 3 b"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert_eq!(command(&parser!(b"zadd key CH 2 a 2 b 2 c"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
    assert_eq!(command(&parser!(b"zadd key NX 1 a 2 b 3 c 4 d"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!(command(&parser!(b"zadd key XX CH 2 b 2 d 2 e"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
}

#[test]
fn zincrby_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"zincrby key 3 a"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data(b"3".to_vec()));
    assert_eq!(command(&parser!(b"zincrby key 4 a"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data(b"7".to_vec()));
}

#[test]
fn zcount_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"zadd key 1 a 2 b 3 c 4 d"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(4));
    assert_eq!(command(&parser!(b"zcount key 2 3"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
    assert_eq!(command(&parser!(b"zcount key 2 3"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
    assert_eq!(command(&parser!(b"zcount key (2 3"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!(command(&parser!(b"zcount key -inf inf"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(4));
}

#[test]
fn zrange_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"zadd key 1 a 2 b 3 c 4 d"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(4));
    assert_eq!(command(&parser!(b"zrange key 0 0"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data(b"a".to_vec()),
                ]));
    assert_eq!(command(&parser!(b"zrange key 0 0 withscoreS"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data(b"a".to_vec()),
                Response::Data(b"1".to_vec()),
                ]));
    assert_eq!(command(&parser!(b"zrange key -2 -1 WITHSCORES"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data(b"c".to_vec()),
                Response::Data(b"3".to_vec()),
                Response::Data(b"d".to_vec()),
                Response::Data(b"4".to_vec()),
                ]));
}

#[test]
fn zrank_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"zadd key 1 a 2 b 3 c 4 d"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(4));
    assert_eq!(command(&parser!(b"zrank key a"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(0));
    assert_eq!(command(&parser!(b"zrank key b"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert_eq!(command(&parser!(b"zrank key e"), &mut db, &mut 0, None, None, None).unwrap(), Response::Nil);
}

#[test]
fn zrem_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"zadd key 1 a 2 b 3 c"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(3));
    assert_eq!(command(&parser!(b"zrem key a b d e"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(2));
    assert_eq!(command(&parser!(b"zrem key c"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
}

#[test]
fn select_command() {
    let mut db = Database::mock();
    let mut dbindex = 0;
    command(&parser!(b"select 1"), &mut db, &mut dbindex, None, None, None).unwrap();
    assert_eq!(dbindex, 1);
}

#[test]
fn flushdb_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"set key value"), &mut db, &mut 0, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    assert_eq!(command(&parser!(b"set key valuf"), &mut db, &mut 1, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    assert_eq!(command(&parser!(b"flushdb"), &mut db, &mut 1, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    assert_eq!(command(&parser!(b"get key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Data("value".to_owned().into_bytes()));
    assert_eq!(command(&parser!(b"get key"), &mut db, &mut 1, None, None, None).unwrap(), Response::Nil);
}

#[test]
fn flushall_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"set key value"), &mut db, &mut 0, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    assert_eq!(command(&parser!(b"set key valuf"), &mut db, &mut 1, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    assert_eq!(command(&parser!(b"flushall"), &mut db, &mut 1, None, None, None).unwrap(), Response::Status("OK".to_owned()));
    assert_eq!(command(&parser!(b"get key"), &mut db, &mut 0, None, None, None).unwrap(), Response::Nil);
    assert_eq!(command(&parser!(b"get key"), &mut db, &mut 1, None, None, None).unwrap(), Response::Nil);
}

#[test]
fn subscribe_publish_command() {
    let mut db = Database::mock();
    let mut subscriptions = HashMap::new();
    let mut psubscriptions = HashMap::new();
    let (tx, rx) = channel();
    assert!(command(&parser!(b"subscribe channel"), &mut db, &mut 0, Some(&mut subscriptions), Some(&mut psubscriptions), Some(&tx)).is_err());
    assert_eq!(command(&parser!(b"publish channel hello-world"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(1));
    assert!(command(&parser!(b"unsubscribe channel"), &mut db, &mut 0, Some(&mut subscriptions), Some(&mut psubscriptions), Some(&tx)).is_err());

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
