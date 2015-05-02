extern crate rsedis;

use std::str::from_utf8;

use rsedis::database::Database;
use rsedis::database::Value;
use rsedis::parser::Parser;
use rsedis::parser::Argument;
use rsedis::command::command;
use rsedis::command::Response;

fn getstr(database: &Database, key: &[u8]) -> String {
    match database.get(&key.to_vec()) {
        Some(val) => {
            match val {
                &Value::Data(ref bytes) => return from_utf8(bytes).unwrap().to_string(),
                &Value::Integer(i) => return format!("{}", i),
                &Value::Nil => panic!("Got nil"),
            }
        },
        _ => assert!(false),
    }
    return String::new();
}

#[test]
fn nocommand() {
    let mut db = Database::new();
    let parser = Parser::new(b"", 0, Vec::new());
    let response = command(&parser, &mut db);
    match response {
        Response::Error(_) => {},
        _ => assert!(false),
    };
}

#[test]
fn set_command() {
    let mut db = Database::new();
    let parser = Parser::new(b"setkeyvalue", 3, vec!(
                Argument {pos: 0, len: 3},
                Argument {pos: 3, len: 3},
                Argument {pos: 6, len: 5},
                ));
    let response = command(&parser, &mut db);
    match response {
        Response::Status(msg) => assert_eq!(msg, "OK"),
        _ => assert!(false),
    };
    assert_eq!("value", getstr(&db, b"key"));
}

#[test]
fn get_command() {
    let mut db = Database::new();
    db.get_or_create(&b"key".to_vec()).set(b"value".to_vec());
    let parser = Parser::new(b"getkey", 2, vec!(
                Argument {pos: 0, len: 3},
                Argument {pos: 3, len: 3},
                ));
    let response = command(&parser, &mut db);
    match response {
        Response::Data(msg) => assert_eq!(msg, b"value"),
        _ => assert!(false),
    };
    assert_eq!("value", getstr(&db, b"key"));
}

#[test]
fn del_command() {
    let mut db = Database::new();
    db.get_or_create(&b"key".to_vec()).set(b"value".to_vec());
    let parser = Parser::new(b"delkeykey2", 3, vec!(
                Argument {pos: 0, len: 3},
                Argument {pos: 3, len: 3},
                Argument {pos: 6, len: 4},
                ));
    let response = command(&parser, &mut db);
    match response {
        Response::Integer(i) => assert_eq!(i, 1),
        _ => assert!(false),
    };
}

#[test]
fn serialize_status() {
    let response = Response::Status("OK".to_string());
    assert_eq!(response.as_bytes(), b"+OK\r\n");
}

#[test]
fn serialize_error() {
    let response = Response::Error("ERR Invalid command".to_string());
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
    let parser = Parser::new(b"appendkeyvalue", 3, vec!(
                Argument {pos: 0, len: 6},
                Argument {pos: 6, len: 3},
                Argument {pos: 9, len: 5},
                ));
    let response = command(&parser, &mut db);
    match response {
        Response::Integer(i) => assert_eq!(i, 5),
        _ => assert!(false),
    };

    let parser = Parser::new(b"appendkeyvalue", 3, vec!(
                Argument {pos: 0, len: 6},
                Argument {pos: 6, len: 3},
                Argument {pos: 9, len: 5},
                ));
    let response = command(&parser, &mut db);
    match response {
        Response::Integer(i) => assert_eq!(i, 10),
        _ => assert!(false),
    };

    match db.get(&b"key".to_vec()).unwrap() {
        &Value::Data(ref val) => assert_eq!(val, b"valuevalue"),
            _ => assert!(false),
    }
}
