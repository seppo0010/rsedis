use super::database::Database;
use super::database::Value;
use super::parser::Parser;

pub enum Response {
    Nil,
    Integer(i64),
    Data(Vec<u8>),
    Error(String),
    Status(String),
}

impl Response {
    pub fn as_bytes(&self) -> Vec<u8> {
        match *self {
            Response::Nil => return b"$-1\r\n".to_vec(),
            Response::Data(ref d) => {
                return [&b"$"[..], &format!("{}\r\n", d.len()).into_bytes()[..], &d[..], &"\r\n".to_string().into_bytes()[..]].concat();
            }
            Response::Integer(ref i) => {
                return [&b":"[..], &format!("{}\r\n", i).into_bytes()[..]].concat();
            }
            Response::Error(ref d) => {
                return [&b"-"[..], (*d).as_bytes(), &"\r\n".to_string().into_bytes()[..]].concat();
            }
            Response::Status(ref d) => {
                return [&b"+"[..], (*d).as_bytes(), &"\r\n".to_string().into_bytes()[..]].concat();
            }
        }
    }
}

macro_rules! validate {
    ($expr: expr, $err: expr) => (
        if !($expr) {
            return Response::Error($err.to_string());
        }
    )
}

macro_rules! try_validate {
    ($expr: expr, $err: expr) => ({
        let res = $expr;
        validate!(res.is_ok(), $err);
        res.unwrap()
    })
}

fn set(parser: &Parser, db: &mut Database) -> Response {
    validate!(parser.argc == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let val = try_validate!(parser.get_vec(2), "Invalid value");
    db.get_or_create(&key).set(val);
    return Response::Status("OK".to_string());
}

fn del(parser: &Parser, db: &mut Database) -> Response {
    validate!(parser.argc >= 2, "Wrong number of parameters");
    let mut c = 0;
    for i in 1..parser.argc {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        match db.remove(&key) {
            Some(_) => c += 1,
            None => {},
        }
    }
    return Response::Integer(c);
}

fn flushall(parser: &Parser, db: &mut Database) -> Response {
    validate!(parser.argc == 1, "Wrong number of parameters");
    db.clear();
    return Response::Status("OK".to_string());
}

fn append(parser: &Parser, db: &mut Database) -> Response {
    validate!(parser.argc == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let val = try_validate!(parser.get_vec(2), "Invalid value");
    let len = db.get_or_create(&key).append(val);
    return Response::Integer(len as i64);
}

fn get(parser: &Parser, db: &mut Database) -> Response {
    validate!(parser.argc == 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let obj = db.get(&key);
    match obj {
        Some(value) => {
            match value {
                &Value::Data(ref data) => return Response::Data(data.clone()),
                &Value::Integer(ref int) => return Response::Data(format!("{}", int).into_bytes()),
                &Value::Nil => panic!("Should not have a nil"),
            }
        }
        None => return Response::Nil,
    }
}

fn generic_incr(parser: &Parser, db: &mut Database, increment: i64) -> Response {
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    match db.get_or_create(&key).incr(increment) {
        Some(val) => Response::Integer(val),
        None =>  Response::Error("ERR Not an integer".to_string()),
    }
}

fn incr(parser: &Parser, db: &mut Database) -> Response {
    validate!(parser.argc == 2, "Wrong number of parameters");
    return generic_incr(parser, db, 1);
}

fn decr(parser: &Parser, db: &mut Database) -> Response {
    validate!(parser.argc == 2, "Wrong number of parameters");
    return generic_incr(parser, db, -1);
}

fn incrby(parser: &Parser, db: &mut Database) -> Response {
    validate!(parser.argc == 3, "Wrong number of parameters");
    let try_increment = parser.get_i64(2);
    if try_increment.is_err() { return Response::Error("Invalid increment".to_string()); }
    return generic_incr(parser, db, try_increment.unwrap());
}

fn decrby(parser: &Parser, db: &mut Database) -> Response {
    validate!(parser.argc == 3, "Wrong number of parameters");
    let try_increment = parser.get_i64(2);
    if try_increment.is_err() { return Response::Error("Invalid decrement".to_string()); }
    return generic_incr(parser, db, -try_increment.unwrap());
}

fn ping(parser: &Parser, db: &mut Database) -> Response {
    #![allow(unused_variables)]
    validate!(parser.argc <= 2, "Wrong number of parameters");
    if parser.argc == 2 {
        return Response::Data(parser.get_vec(1).unwrap());
    }
    return Response::Data(b"PONG".to_vec());
}

pub fn command(parser: &Parser, db: &mut Database) -> Response {
    if parser.argc == 0 {
        return Response::Error("Not enough arguments".to_string());
    }
    let try_command = parser.get_str(0);
    if try_command.is_err() {
        return Response::Error("Invalid command".to_string());
    }
    match try_command.unwrap() {
        "set" => return set(parser, db),
        "del" => return del(parser, db),
        "append" => return append(parser, db),
        "get" => return get(parser, db),
        "incr" => return incr(parser, db),
        "decr" => return decr(parser, db),
        "incrby" => return incrby(parser, db),
        "decrby" => return decrby(parser, db),
        "ping" => return ping(parser, db),
        "flushall" => return flushall(parser, db),
        _ => return Response::Error("Unknown command".to_string()),
    };
}
