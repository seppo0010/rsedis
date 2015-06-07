extern crate rsedis;

use std::str::from_utf8;

use rsedis::database::Database;
use rsedis::database::Value;
use rsedis::parser::Parser;
use rsedis::parser::Argument;
use rsedis::command::command;
use rsedis::command::Response;

fn getstr(database: &Database, key: &[u8]) -> String {
    match database.get(0, &key.to_vec()) {
        Some(val) => {
            match val {
                &Value::Data(ref bytes) => return from_utf8(bytes).unwrap().to_owned(),
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
    let response = command(&parser, &mut db, &mut 0);
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
    assert_eq!(command(&parser, &mut db, &mut 0), Response::Status("OK".to_owned()));
    assert_eq!("value", getstr(&db, b"key"));
}

#[test]
fn get_command() {
    let mut db = Database::new();
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    let parser = Parser::new(b"getkey", 2, vec!(
                Argument {pos: 0, len: 3},
                Argument {pos: 3, len: 3},
                ));
    assert_eq!(command(&parser, &mut db, &mut 0), Response::Data("value".to_owned().into_bytes()));
    assert_eq!("value", getstr(&db, b"key"));
}

#[test]
fn del_command() {
    let mut db = Database::new();
    assert!(db.get_or_create(0, &b"key".to_vec()).set(b"value".to_vec()).is_ok());
    let parser = Parser::new(b"delkeykey2", 3, vec!(
                Argument {pos: 0, len: 3},
                Argument {pos: 3, len: 3},
                Argument {pos: 6, len: 4},
                ));
    assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(1));
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
    let parser = Parser::new(b"appendkeyvalue", 3, vec!(
                Argument {pos: 0, len: 6},
                Argument {pos: 6, len: 3},
                Argument {pos: 9, len: 5},
                ));
    assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(5));

    let parser = Parser::new(b"appendkeyvalue", 3, vec!(
                Argument {pos: 0, len: 6},
                Argument {pos: 6, len: 3},
                Argument {pos: 9, len: 5},
                ));
    assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(10));

    assert_eq!(db.get(0, &b"key".to_vec()).unwrap(), &Value::Data(b"valuevalue".to_vec()));
}

#[test]
fn incr_command() {
    let mut db = Database::new();
    let parser = Parser::new(b"incrkey", 2, vec!(
                Argument {pos: 0, len: 4},
                Argument {pos: 4, len: 3},
                ));
    assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(1));
    assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(2));
}

#[test]
fn incrby_command() {
    let mut db = Database::new();
    let parser = Parser::new(b"incrbykey5", 3, vec!(
                Argument {pos: 0, len: 6},
                Argument {pos: 6, len: 3},
                Argument {pos: 9, len: 1},
                ));
    assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(5));
    assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(10));
}

#[test]
fn decr_command() {
    let mut db = Database::new();
    let parser = Parser::new(b"decrkey", 2, vec!(
                Argument {pos: 0, len: 4},
                Argument {pos: 4, len: 3},
                ));
    assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(-1));
    assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(-2));
}

#[test]
fn decrby_command() {
    let mut db = Database::new();
    let parser = Parser::new(b"decrbykey5", 3, vec!(
                Argument {pos: 0, len: 6},
                Argument {pos: 6, len: 3},
                Argument {pos: 9, len: 1},
                ));
    assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(-5));
    assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(-10));
}

#[test]
fn lpush_command() {
    let mut db = Database::new();
    let parser = Parser::new(b"lpushkeyvalue", 3, vec!(
                Argument {pos: 0, len: 5},
                Argument {pos: 5, len: 3},
                Argument {pos: 8, len: 5},
                ));
    assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(1));
    assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(2));
}

#[test]
fn rpush_command() {
    let mut db = Database::new();
    let parser = Parser::new(b"rpushkeyvalue", 3, vec!(
                Argument {pos: 0, len: 5},
                Argument {pos: 5, len: 3},
                Argument {pos: 8, len: 5},
                ));
    assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(1));
    assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(2));
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
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"rpushkeyvaluf", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"lpopkeyvalue", 2, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 3},
                    ));

        assert_eq!(command(&parser, &mut db, &mut 0), Response::Data("value".to_owned().into_bytes()));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Data("valuf".to_owned().into_bytes()));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Nil);
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
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"rpushkeyvaluf", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"rpopkeyvalue", 2, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 3},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Data("valuf".to_owned().into_bytes()));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Data("value".to_owned().into_bytes()));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Nil);
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
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"rpushkeyvaluf", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"lindexkey0", 3, vec!(
                    Argument {pos: 0, len: 6},
                    Argument {pos: 6, len: 3},
                    Argument {pos: 9, len: 1},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Data("value".to_owned().into_bytes()));
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
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"rpushkeyvalug", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"linsertkeybeforevalugvaluf", 5, vec!(
                    Argument {pos: 0, len: 7},
                    Argument {pos: 7, len: 3},
                    Argument {pos: 10, len: 6},
                    Argument {pos: 16, len: 5},
                    Argument {pos: 21, len: 5},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(3));
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
        command(&parser, &mut db, &mut 0);
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"llenkey", 2, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 3},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(2));
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
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(0));
    }
    {
        let parser = Parser::new(b"lpushkeyvalue", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(1));
    }
    {
        let parser = Parser::new(b"lpushxkeyvalue", 3, vec!(
                    Argument {pos: 0, len: 6},
                    Argument {pos: 6, len: 3},
                    Argument {pos: 9, len: 5},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(2));
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
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"rpushkeyvaluf", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"rpushkeyvalug", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"lrange key 0 -1", 4, vec!(
                    Argument {pos: 0, len: 6},
                    Argument {pos: 7, len: 3},
                    Argument {pos: 11, len: 1},
                    Argument {pos: 13, len: 2},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Array(vec![
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
        let parser = Parser::new(b"rpushkeyvalue", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db, &mut 0);
        command(&parser, &mut db, &mut 0);
        command(&parser, &mut db, &mut 0);
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"lremkey2value", 4, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 3},
                    Argument {pos: 7, len: 1},
                    Argument {pos: 8, len: 5},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(2));
    }
    {
        let parser = Parser::new(b"llenkey", 2, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 3},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(2));
    }
}

#[test]
fn lset_command() {
    let mut db = Database::new();
    {
        let parser = Parser::new(b"rpushkeyvalue", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db, &mut 0);
        command(&parser, &mut db, &mut 0);
        command(&parser, &mut db, &mut 0);
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"lsetkey2valuf", 4, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 3},
                    Argument {pos: 7, len: 1},
                    Argument {pos: 8, len: 5},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Status("OK".to_owned()));
    }
    {
        let parser = Parser::new(b"lrangekey22", 4, vec!(
                    Argument {pos: 0, len: 6},
                    Argument {pos: 6, len: 3},
                    Argument {pos: 9, len: 1},
                    Argument {pos: 10, len: 1},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Array(vec![
                    Response::Data("valuf".to_owned().into_bytes()),
                    ]));
    }
}

#[test]
fn rpoplpush_command() {
    let mut db = Database::new();
    {
        let parser = Parser::new(b"rpushkeyvalue", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"rpushkeyvaluf", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"llenkey2", 2, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 4},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(0));
    }
    {
        let parser = Parser::new(b"rpoplpushkeykey2", 3, vec!(
                    Argument {pos: 0, len: 9},
                    Argument {pos: 9, len: 3},
                    Argument {pos: 12, len: 4},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Data("valuf".to_owned().into_bytes()));
    }
    {
        let parser = Parser::new(b"llenkey", 2, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 3},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(1));
    }
    {
        let parser = Parser::new(b"llenkey2", 2, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 4},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(1));
    }
    {
        let parser = Parser::new(b"rpoplpushkeykey2", 3, vec!(
                    Argument {pos: 0, len: 9},
                    Argument {pos: 9, len: 3},
                    Argument {pos: 12, len: 4},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Data("value".to_owned().into_bytes()));
    }
    {
        let parser = Parser::new(b"llenkey", 2, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 3},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(0));
    }
    {
        let parser = Parser::new(b"llenkey2", 2, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 4},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(2));
    }
}

#[test]
fn ltrim_command() {
    let mut db = Database::new();
    {
        let parser = Parser::new(b"rpushkeyvalue", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db, &mut 0);
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"rpushkeyvaluf", 3, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 5},
                    ));
        command(&parser, &mut db, &mut 0);
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"ltrimkey1-2", 4, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    Argument {pos: 8, len: 1},
                    Argument {pos: 9, len: 2},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Status("OK".to_owned()));
    }
    {
        let parser = Parser::new(b"lrangekey0-1", 4, vec!(
                    Argument {pos: 0, len: 6},
                    Argument {pos: 6, len: 3},
                    Argument {pos: 9, len: 1},
                    Argument {pos: 10, len: 2},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Array(vec![
                    Response::Data("value".to_owned().into_bytes()),
                    Response::Data("valuf".to_owned().into_bytes()),
                    ]));
    }
}

#[test]
fn sadd_command() {
    let mut db = Database::new();
    {
        let parser = Parser::new(b"saddkey11123", 7, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 3},
                    Argument {pos: 7, len: 1},
                    Argument {pos: 8, len: 1},
                    Argument {pos: 9, len: 1},
                    Argument {pos: 10, len: 1},
                    Argument {pos: 11, len: 1},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(3));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(0));
    }
}

#[test]
fn scard_command() {
    let mut db = Database::new();
    {
        let parser = Parser::new(b"saddkey123", 5, vec!(
                    Argument {pos: 0, len: 4},
                    Argument {pos: 4, len: 3},
                    Argument {pos: 7, len: 1},
                    Argument {pos: 8, len: 1},
                    Argument {pos: 9, len: 1},
                    ));
        command(&parser, &mut db, &mut 0);
    }
    {
        let parser = Parser::new(b"scardkey", 2, vec!(
                    Argument {pos: 0, len: 5},
                    Argument {pos: 5, len: 3},
                    ));
        assert_eq!(command(&parser, &mut db, &mut 0), Response::Integer(3));
    }
}

#[test]
fn select_command() {
    let mut db = Database::new();
    {
        let mut dbindex = 0;
        let parser = Parser::new(b"select1", 2, vec!(
                    Argument {pos: 0, len: 6},
                    Argument {pos: 6, len: 1},
                    ));
        command(&parser, &mut db, &mut dbindex);
        assert_eq!(dbindex, 1);
    }
}
