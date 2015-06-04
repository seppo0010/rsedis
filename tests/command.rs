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
                _ => panic!("Got non-string"),
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
    assert!(db.get_or_create(&b"key".to_vec()).set(b"value".to_vec()).is_ok());
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
    assert!(db.get_or_create(&b"key".to_vec()).set(b"value".to_vec()).is_ok());
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

#[test]
fn incr_command() {
    let mut db = Database::new();
    let parser = Parser::new(b"incrkey", 2, vec!(
                Argument {pos: 0, len: 4},
                Argument {pos: 4, len: 3},
                ));
    match command(&parser, &mut db) {
        Response::Integer(i) => assert_eq!(i, 1),
        _ => assert!(false),
    };
    match command(&parser, &mut db) {
        Response::Integer(i) => assert_eq!(i, 2),
        _ => assert!(false),
    };
}

#[test]
fn incrby_command() {
    let mut db = Database::new();
    let parser = Parser::new(b"incrbykey5", 3, vec!(
                Argument {pos: 0, len: 6},
                Argument {pos: 6, len: 3},
                Argument {pos: 9, len: 1},
                ));
    match command(&parser, &mut db) {
        Response::Integer(i) => assert_eq!(i, 5),
        _ => assert!(false),
    };
    match command(&parser, &mut db) {
        Response::Integer(i) => assert_eq!(i, 10),
        _ => assert!(false),
    };
}

#[test]
fn decr_command() {
    let mut db = Database::new();
    let parser = Parser::new(b"decrkey", 2, vec!(
                Argument {pos: 0, len: 4},
                Argument {pos: 4, len: 3},
                ));
    match command(&parser, &mut db) {
        Response::Integer(i) => assert_eq!(i, -1),
        _ => assert!(false),
    };
    match command(&parser, &mut db) {
        Response::Integer(i) => assert_eq!(i, -2),
        _ => assert!(false),
    };
}

#[test]
fn decrby_command() {
    let mut db = Database::new();
    let parser = Parser::new(b"decrbykey5", 3, vec!(
                Argument {pos: 0, len: 6},
                Argument {pos: 6, len: 3},
                Argument {pos: 9, len: 1},
                ));
    match command(&parser, &mut db) {
        Response::Integer(i) => assert_eq!(i, -5),
        _ => assert!(false),
    };
    match command(&parser, &mut db) {
        Response::Integer(i) => assert_eq!(i, -10),
        _ => assert!(false),
    };
}

#[test]
fn lpush_command() {
    let mut db = Database::new();
    let parser = Parser::new(b"lpushkeyvalue", 3, vec!(
                Argument {pos: 0, len: 5},
                Argument {pos: 5, len: 3},
                Argument {pos: 8, len: 5},
                ));
    match command(&parser, &mut db) {
        Response::Integer(i) => assert_eq!(i, 1),
        _ => assert!(false),
    };
    match command(&parser, &mut db) {
        Response::Integer(i) => assert_eq!(i, 2),
        _ => assert!(false),
    };
}

#[test]
fn rpush_command() {
    let mut db = Database::new();
    let parser = Parser::new(b"rpushkeyvalue", 3, vec!(
                Argument {pos: 0, len: 5},
                Argument {pos: 5, len: 3},
                Argument {pos: 8, len: 5},
                ));
    match command(&parser, &mut db) {
        Response::Integer(i) => assert_eq!(i, 1),
        _ => assert!(false),
    };
    match command(&parser, &mut db) {
        Response::Integer(i) => assert_eq!(i, 2),
        _ => assert!(false),
    };
}

#[test]
fn lpop_command() {
    let mut db = Database::new();
    {
        let parser = Parser::new(b"rpushkeyvalue", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db);
    }
    {
        let parser = Parser::new(b"rpushkeyvaluf", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db);
    }
    {
        let parser = Parser::new(b"lpopkeyvalue", 2, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 3},
                    ));
        match command(&parser, &mut db) {
            Response::Data(d) => assert_eq!(d, "value".to_string().into_bytes()),
            _ => assert!(false),
        };
        match command(&parser, &mut db) {
            Response::Data(d) => assert_eq!(d, "valuf".to_string().into_bytes()),
            _ => assert!(false),
        };
        match command(&parser, &mut db) {
            Response::Nil => {},
            _ => assert!(false),
        };
    }
}

#[test]
fn rpop_command() {
    let mut db = Database::new();
    {
        let parser = Parser::new(b"rpushkeyvalue", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db);
    }
    {
        let parser = Parser::new(b"rpushkeyvaluf", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db);
    }
    {
        let parser = Parser::new(b"rpopkeyvalue", 2, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 3},
                    ));
        match command(&parser, &mut db) {
            Response::Data(d) => assert_eq!(d, "valuf".to_string().into_bytes()),
            _ => assert!(false),
        };
        match command(&parser, &mut db) {
            Response::Data(d) => assert_eq!(d, "value".to_string().into_bytes()),
            _ => assert!(false),
        };
        match command(&parser, &mut db) {
            Response::Nil => {},
            _ => assert!(false),
        };
    }
}

#[test]
fn lindex_command() {
    let mut db = Database::new();
    {
        let parser = Parser::new(b"rpushkeyvalue", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db);
    }
    {
        let parser = Parser::new(b"rpushkeyvaluf", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db);
    }
    {
        let parser = Parser::new(b"lindexkey0", 3, vec!(
                    Argument {pos: 0, len: 6},
                    Argument {pos: 6, len: 3},
                    Argument {pos: 9, len: 1},
                    ));
        match command(&parser, &mut db) {
            Response::Data(d) => assert_eq!(d, "value".to_string().into_bytes()),
            _ => assert!(false),
        };
    }
}

#[test]
fn linsert_command() {
    let mut db = Database::new();
    {
        let parser = Parser::new(b"rpushkeyvalue", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db);
    }
    {
        let parser = Parser::new(b"rpushkeyvalug", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db);
    }
    {
        let parser = Parser::new(b"linsertkeybeforevalugvaluf", 5, vec!(
                    Argument {pos: 0, len: 7},
                    Argument {pos: 7, len: 3},
                    Argument {pos: 10, len: 6},
                    Argument {pos: 16, len: 5},
                    Argument {pos: 21, len: 5},
                    ));
        match command(&parser, &mut db) {
            Response::Integer(d) => assert_eq!(d, 3),
            _ => assert!(false),
        };
    }
}

#[test]
fn llen_command() {
    let mut db = Database::new();
    {
        let parser = Parser::new(b"rpushkeyvalue", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db);
        command(&parser, &mut db);
    }
    {
        let parser = Parser::new(b"llenkey", 2, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 3},
                    ));
        match command(&parser, &mut db) {
            Response::Integer(d) => assert_eq!(d, 2),
            _ => assert!(false),
        };
    }
}

#[test]
fn lpushx_command() {
    let mut db = Database::new();
    {
        let parser = Parser::new(b"lpushxkeyvalue", 3, vec!(
                    Argument {pos: 0, len: 6},
                    Argument {pos: 6, len: 3},
                    Argument {pos: 9, len: 5},
                    ));
        match command(&parser, &mut db) {
            Response::Integer(d) => assert_eq!(d, 0),
            _ => assert!(false),
        };
    }
    {
        let parser = Parser::new(b"lpushkeyvalue", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        match command(&parser, &mut db) {
            Response::Integer(d) => assert_eq!(d, 1),
            _ => assert!(false),
        };
    }
    {
        let parser = Parser::new(b"lpushxkeyvalue", 3, vec!(
                    Argument {pos: 0, len: 6},
                    Argument {pos: 6, len: 3},
                    Argument {pos: 9, len: 5},
                    ));
        match command(&parser, &mut db) {
            Response::Integer(d) => assert_eq!(d, 2),
            _ => assert!(false),
        };
    }
}

#[test]
fn lrange_command() {
    let mut db = Database::new();
    {
        let parser = Parser::new(b"rpushkeyvalue", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db);
    }
    {
        let parser = Parser::new(b"rpushkeyvaluf", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db);
    }
    {
        let parser = Parser::new(b"rpushkeyvalug", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db);
    }
    {
        let parser = Parser::new(b"lrange key 0 -1", 4, vec!(
                    Argument {pos: 0, len: 6},
                    Argument {pos: 7, len: 3},
                    Argument {pos: 11, len: 1},
                    Argument {pos: 13, len: 2},
                    ));
        match command(&parser, &mut db) {
            Response::Array(ref arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], Response::Data("value".to_string().into_bytes()));
                assert_eq!(arr[1], Response::Data("valuf".to_string().into_bytes()));
                assert_eq!(arr[2], Response::Data("valug".to_string().into_bytes()));
            },
            _ => assert!(false),
        };
    }
}

#[test]
fn lrem_command() {
    let mut db = Database::new();
    {
        let parser = Parser::new(b"rpushkeyvalue", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db);
        command(&parser, &mut db);
        command(&parser, &mut db);
        command(&parser, &mut db);
    }
    {
        let parser = Parser::new(b"lremkey2value", 4, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 3},
                    Argument {pos: 7, len: 1},
                    Argument {pos: 8, len: 5},
                    ));
        match command(&parser, &mut db) {
            Response::Integer(d) => assert_eq!(d, 2),
            _ => assert!(false),
        };
    }
    {
        let parser = Parser::new(b"llenkey", 2, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 3},
                    ));
        match command(&parser, &mut db) {
            Response::Integer(d) => assert_eq!(d, 2),
            _ => assert!(false),
        };
    }
}
