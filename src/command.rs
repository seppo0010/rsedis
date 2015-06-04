use std::ascii::AsciiExt;

use super::database::Database;
use super::database::Value;
use super::parser::Parser;

#[derive(PartialEq)]
#[derive(Debug)]
pub enum Response {
    Nil,
    Integer(i64),
    Data(Vec<u8>),
    Error(String),
    Status(String),
    Array(Vec<Response>),
}

impl Response {
    pub fn as_bytes(&self) -> Vec<u8> {
        return match *self {
            Response::Nil => b"$-1\r\n".to_vec(),
            Response::Data(ref d) => [&b"$"[..], &format!("{}\r\n", d.len()).into_bytes()[..], &d[..], &"\r\n".to_string().into_bytes()[..]].concat(),
            Response::Integer(ref i) => [&b":"[..], &format!("{}\r\n", i).into_bytes()[..]].concat(),
            Response::Error(ref d) => [&b"-"[..], (*d).as_bytes(), &"\r\n".to_string().into_bytes()[..]].concat(),
            Response::Status(ref d) => [&b"+"[..], (*d).as_bytes(), &"\r\n".to_string().into_bytes()[..]].concat(),
            Response::Array(ref a) => [&b"*"[..],  &format!("{}\r\n", a.len()).into_bytes()[..],
                &(a.iter().map(|el| el.as_bytes()).collect::<Vec<_>>()[..].concat())[..]
                ].concat()
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
    return match db.get_or_create(&key).set(val) {
        Ok(_) => Response::Status("OK".to_string()),
        Err(err) => Response::Error(err.to_string()),
    }
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
    return match db.get_or_create(&key).append(val) {
        Ok(len) => Response::Integer(len as i64),
        Err(err) => Response::Error(err.to_string()),
    }
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
                _ => panic!("Should be an integer or data"),
            }
        }
        None => return Response::Nil,
    }
}

fn generic_incr(parser: &Parser, db: &mut Database, increment: i64) -> Response {
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    match db.get_or_create(&key).incr(increment) {
        Ok(val) => Response::Integer(val),
        Err(err) =>  Response::Error(err.to_string()),
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

fn generic_push(parser: &Parser, db: &mut Database, right: bool, create: bool) -> Response {
    validate!(parser.argc == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let val = try_validate!(parser.get_vec(2), "Invalid value");
    let el;
    if create {
        el = db.get_or_create(&key);
    } else {
        match db.get_mut(&key) {
            Some(_el) => el = _el,
            None => return Response::Integer(0),
        }
    }
    return match el.push(val, right) {
        Ok(listsize) => Response::Integer(listsize as i64),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn lpush(parser: &Parser, db: &mut Database) -> Response {
    return generic_push(parser, db, false, true);
}

fn rpush(parser: &Parser, db: &mut Database) -> Response {
    return generic_push(parser, db, true, true);
}

fn lpushx(parser: &Parser, db: &mut Database) -> Response {
    return generic_push(parser, db, false, false);
}

fn rpushx(parser: &Parser, db: &mut Database) -> Response {
    return generic_push(parser, db, true, false);
}

fn generic_pop(parser: &Parser, db: &mut Database, right: bool) -> Response {
    validate!(parser.argc == 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    return match db.get_mut(&key) {
        Some(mut list) => match list.pop(right) {
            Ok(el) => {
                match el {
                    Some(val) => Response::Data(val),
                    None => Response::Nil,
                }
            }
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Nil,
    }
}

fn lpop(parser: &Parser, db: &mut Database) -> Response {
    return generic_pop(parser, db, false);
}

fn rpop(parser: &Parser, db: &mut Database) -> Response {
    return generic_pop(parser, db, true);
}

fn lindex(parser: &Parser, db: &mut Database) -> Response {
    validate!(parser.argc == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let index = try_validate!(parser.get_i64(2), "Invalid index");
    return match db.get(&key) {
        Some(el) => match el.lindex(index) {
            Ok(el) => {
                match el {
                    Some(val) => Response::Data(val.clone()),
                    None => Response::Nil,
                }
            }
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Nil,
    }
}

fn linsert(parser: &Parser, db: &mut Database) -> Response {
    validate!(parser.argc == 5, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let before_str = try_validate!(parser.get_str(2), "Syntax error");
    let pivot = try_validate!(parser.get_vec(3), "Invalid pivot");
    let value = try_validate!(parser.get_vec(4), "Invalid value");
    let before;
    match &*before_str.to_ascii_lowercase() {
        "after" => before = false,
        "before" => before = true,
        _ => return Response::Error("ERR Syntax error".to_string()),
    };
    return match db.get_mut(&key) {
        Some(mut el) => match el.linsert(before, pivot, value) {
            Ok(r) => {
                match r {
                    Some(listsize) => Response::Integer(listsize as i64),
                    None => Response::Integer(-1),
                }
            }
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Integer(-1),
    }
}

fn llen(parser: &Parser, db: &mut Database) -> Response {
    validate!(parser.argc == 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    return match db.get(&key) {
        Some(el) => match el.llen() {
            Ok(listsize) => Response::Integer(listsize as i64),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Integer(0),
    }
}

fn lrange(parser: &Parser, db: &mut Database) -> Response {
    validate!(parser.argc == 4, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let start = try_validate!(parser.get_i64(2), "Invalid range");
    let stop  = try_validate!(parser.get_i64(3), "Invalid range");
    return match db.get(&key) {
        Some(el) => match el.lrange(start, stop) {
            Ok(items) => Response::Array(items.iter().map(|i| Response::Data(i.to_vec())).collect()),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Array(Vec::new()),
    }
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
        "lpush" => return lpush(parser, db),
        "rpush" => return rpush(parser, db),
        "lpushx" => return lpushx(parser, db),
        "rpushx" => return rpushx(parser, db),
        "lpop" => return lpop(parser, db),
        "rpop" => return rpop(parser, db),
        "lindex" => return lindex(parser, db),
        "linsert" => return linsert(parser, db),
        "llen" => return llen(parser, db),
        "lrange" => return lrange(parser, db),
        _ => return Response::Error("Unknown command".to_string()),
    };
}
