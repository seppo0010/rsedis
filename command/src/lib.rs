extern crate database;
extern crate parser;
extern crate response;
extern crate util;

use std::ascii::AsciiExt;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter, Error};
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::Sender;
use std::sync::mpsc::channel;
use std::thread;
use std::usize;

use database::PubsubEvent;
use database::Database;
use database::{Value, ValueSet};
use response::{Response, ResponseError};
use parser::{Parser, Argument};
use util::mstime;


macro_rules! opt_validate {
    ($expr: expr, $err: expr) => (
        if !($expr) {
            return Ok(Response::Error($err.to_string()));
        }
    )
}

macro_rules! try_opt_validate {
    ($expr: expr, $err: expr) => ({
        match $expr {
            Ok(r) => r,
            Err(_) => return Ok(Response::Error($err.to_string())),
        }
    })
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
        match $expr {
            Ok(r) => r,
            Err(_) => return Response::Error($err.to_string()),
        }
    })
}

macro_rules! get_values {
    ($start: expr, $parser: expr, $db: expr, $dbindex: expr) => ({
        validate!($parser.argv.len() >= (1 + $start), "Wrong number of parameters");
        let mut sets = Vec::with_capacity($parser.argv.len() - (1 + $start));
        let key = try_validate!($parser.get_vec($start), "Invalid key");
        let el = match $db.get($dbindex, &key) {
            Some(e) => e,
            None => return Response::Array(vec![]),
        };
        for i in ($start + 1)..$parser.argv.len() {
            let key = try_validate!($parser.get_vec(i), "Invalid key");
            match $db.get($dbindex, &key) {
                Some(e) => sets.push(e),
                None => (),
            };
        }
        (el, sets)
    })
}

fn generic_set(db: &mut Database, dbindex: usize, key: Vec<u8>, val: Vec<u8>, nx: bool, xx: bool, expiration: Option<i64>) -> Result<bool, Response>  {
    if nx && db.get(dbindex, &key).is_some() {
        return Ok(false);
    }

    if xx && db.get(dbindex, &key).is_none() {
        return Ok(false);
    }

    match db.get_or_create(dbindex, &key).set(val) {
        Ok(_) => {
            db.key_publish(&key);
            match expiration {
                Some(msexp) => db.set_msexpiration(dbindex, key, msexp + mstime()),
                None => (),
            }
            Ok(true)
        }
        Err(err) => Err(Response::Error(err.to_string())),
    }
}

fn set(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let val = try_validate!(parser.get_vec(2), "Invalid value");
    let mut nx = false;
    let mut xx = false;
    let mut expiration = None;
    let mut skip = false;
    for i in 3..parser.argv.len() {
        if skip {
            skip = false;
            continue;
        }
        let param = try_validate!(parser.get_str(i), "Invalid parameter");
        match &*param.to_ascii_lowercase() {
            "nx" => nx = true,
            "xx" => xx = true,
            "px" => {
                let px = try_validate!(parser.get_i64(i + 1), "Invalid parameter");
                expiration = Some(px);
                skip = true;
            },
            "ex" => {
                let ex = try_validate!(parser.get_i64(i + 1), "Invalid parameter");
                expiration = Some(ex * 1000);
                skip = true;
            },
            _ => return Response::Error("Invalid parameter".to_owned()),
        }
    }

    match generic_set(db, dbindex, key, val, nx, xx, expiration) {
        Ok(updated) => if updated { Response::Status("OK".to_owned()) } else { Response::Nil },
        Err(r) => r,
    }
}

fn setnx(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let val = try_validate!(parser.get_vec(2), "Invalid value");
    match generic_set(db, dbindex, key, val, true, false, None) {
        Ok(updated) => Response::Integer(if updated { 1 } else { 0 }),
        Err(r) => r
    }
}

fn setex(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 4, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let exp = try_validate!(parser.get_i64(2), "Invalid expiration");
    let val = try_validate!(parser.get_vec(3), "Invalid value");
    match generic_set(db, dbindex, key, val, false, false, Some(exp * 1000)) {
        Ok(_) => Response::Status("OK".to_owned()),
        Err(r) => r
    }
}

fn psetex(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 4, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let exp = try_validate!(parser.get_i64(2), "Invalid value");
    let val = try_validate!(parser.get_vec(3), "Invalid value");
    match generic_set(db, dbindex, key, val, false, false, Some(exp)) {
        Ok(_) => Response::Status("OK".to_owned()),
        Err(r) => r
    }
}

fn exists(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    Response::Integer(match db.get(dbindex, &key) {
        Some(_) => 1,
        None => 0,
    })
}

fn del(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 2, "Wrong number of parameters");
    let mut c = 0;
    for i in 1..parser.argv.len() {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        match db.remove(dbindex, &key) {
            Some(_) => {
                c += 1;
                db.key_publish(&key);
            },
            None => {},
        }
    }
    return Response::Integer(c);
}

fn generic_expire(db: &mut Database, dbindex: usize, key: Vec<u8>, msexpiration: i64) -> Response {
    Response::Integer(match db.get(dbindex, &key) {
        Some(_) => {
            db.set_msexpiration(dbindex, key, msexpiration);
            1
        },
        None => 0,
    })
}

fn expire(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let expiration = try_validate!(parser.get_i64(2), "Invalid expiration");
    generic_expire(db, dbindex, key, mstime() + expiration * 1000)
}

fn expireat(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let expiration = try_validate!(parser.get_i64(2), "Invalid expiration");
    generic_expire(db, dbindex, key, expiration * 1000)
}

fn pexpire(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let expiration = try_validate!(parser.get_i64(2), "Invalid expiration");
    generic_expire(db, dbindex, key, mstime() + expiration)
}

fn pexpireat(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let expiration = try_validate!(parser.get_i64(2), "Invalid expiration");
    generic_expire(db, dbindex, key, expiration)
}

fn flushdb(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 1, "Wrong number of parameters");
    db.clear(dbindex);
    return Response::Status("OK".to_owned());
}

fn generic_ttl(db: &mut Database, dbindex: usize, key: &Vec<u8>, divisor: i64) -> Response {
    Response::Integer(match db.get(dbindex, key) {
        Some(_) => {
            match db.get_msexpiration(dbindex, key) {
                Some(exp) => (exp - mstime()) / divisor,
                None => -1,
            }
        },
        None => -2,
    })
}

fn ttl(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    generic_ttl(db, dbindex, &key, 1000)
}

fn pttl(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    generic_ttl(db, dbindex, &key, 1)
}

fn persist(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    Response::Integer(match db.remove_msexpiration(dbindex, &key) {
        Some(_) => 1,
        None => 0,
    })
}

fn dbtype(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    match db.get(dbindex, &key) {
        Some(value) => {
            match value {
                &Value::Nil => Response::Data("none".to_owned().into_bytes()),
                &Value::String(_) => Response::Data("string".to_owned().into_bytes()),
                &Value::List(_) => Response::Data("list".to_owned().into_bytes()),
                &Value::Set(_) => Response::Data("set".to_owned().into_bytes()),
                &Value::SortedSet(_) => Response::Data("zset".to_owned().into_bytes()),
            }
        }
        None => Response::Data("none".to_owned().into_bytes()),
    }
}

fn flushall(parser: &Parser, db: &mut Database, _: usize) -> Response {
    validate!(parser.argv.len() == 1, "Wrong number of parameters");
    db.clearall();
    return Response::Status("OK".to_owned());
}

fn append(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let val = try_validate!(parser.get_vec(2), "Invalid value");
    let r = match db.get_or_create(dbindex, &key).append(val) {
        Ok(len) => Response::Integer(len as i64),
        Err(err) => Response::Error(err.to_string()),
    };
    db.key_publish(&key);
    r
}

fn generic_get(db: &Database, dbindex: usize, key: Vec<u8>) -> Response {
    let obj = db.get(dbindex, &key);
    match obj {
        Some(value) => match value.get() {
            Ok(r) => Response::Data(r),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Nil,
    }
}

fn get(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    generic_get(db, dbindex, key)
}

fn mget(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 2, "Wrong number of parameters");
    let mut responses = Vec::with_capacity(parser.argv.len() - 1);
    for i in 1..parser.argv.len() {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        responses.push(generic_get(db, dbindex, key));
    }
    Response::Array(responses)
}

fn getrange(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 4, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let start = try_validate!(parser.get_i64(2), "Invalid range");
    let stop = try_validate!(parser.get_i64(3), "Invalid range");
    let obj = db.get(dbindex, &key);
    match obj {
        Some(value) => match value.getrange(start, stop) {
            Ok(r) => Response::Data(r),
            Err(e) => Response::Error(e.to_string()),
        },
        None => return Response::Nil,
    }
}

fn setrange(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 4, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let index = try_validate!(parser.get_i64(2), "Invalid index");
    let value = try_validate!(parser.get_vec(3), "Invalid value");
    match db.get_or_create(dbindex, &key).setrange(index, value) {
        Ok(s) => Response::Integer(s as i64),
        Err(e) => Response::Error(e.to_string()),
    }
}

fn setbit(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 4, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let index = try_validate!(parser.get_i64(2), "Invalid index");
    validate!(index >= 0, "Invalid index");
    let value = try_validate!(parser.get_i64(3), "Invalid value");
    validate!(value == 0 || value == 1, "Value out of range");
    match db.get_or_create(dbindex, &key).setbit(index as usize, value == 1) {
        Ok(s) => Response::Integer(if s { 1 } else { 0 }),
        Err(e) => Response::Error(e.to_string()),
    }
}

fn getbit(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let index = try_validate!(parser.get_i64(2), "Invalid index");
    validate!(index >= 0, "Invalid index");
    match db.get_or_create(dbindex, &key).getbit(index as usize) {
        Ok(s) => Response::Integer(if s { 1 } else { 0 }),
        Err(e) => Response::Error(e.to_string()),
    }
}

fn strlen(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let obj = db.get(dbindex, &key);
    match obj {
        Some(value) => {
            match value.strlen() {
                Ok(r) => Response::Integer(r as i64),
                Err(err) => Response::Error(err.to_string()),
            }
        }
        None => return Response::Integer(0),
    }
}

fn generic_incr(parser: &Parser, db: &mut Database, dbindex: usize, increment: i64) -> Response {
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let r = match db.get_or_create(dbindex, &key).incr(increment) {
        Ok(val) => Response::Integer(val),
        Err(err) =>  Response::Error(err.to_string()),
    };
    db.key_publish(&key);
    r
}

fn incr(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 2, "Wrong number of parameters");
    return generic_incr(parser, db, dbindex, 1);
}

fn decr(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 2, "Wrong number of parameters");
    return generic_incr(parser, db, dbindex, -1);
}

fn incrby(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 3, "Wrong number of parameters");
    match parser.get_i64(2) {
        Ok(increment) => generic_incr(parser, db, dbindex, increment),
        Err(_) => Response::Error("Invalid increment".to_owned()),
    }
}

fn decrby(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 3, "Wrong number of parameters");
    match parser.get_i64(2) {
        Ok(decrement) => generic_incr(parser, db, dbindex, -decrement),
        Err(_) => Response::Error("Invalid increment".to_owned()),
    }
}

fn generic_push(parser: &Parser, db: &mut Database, dbindex: usize, right: bool, create: bool) -> Response {
    // TODO variadic
    validate!(parser.argv.len() == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let val = try_validate!(parser.get_vec(2), "Invalid value");
    let r = {
        let el;
        if create {
            el = db.get_or_create(dbindex, &key);
        } else {
            match db.get_mut(dbindex, &key) {
                Some(_el) => el = _el,
                None => return Response::Integer(0),
            }
        }
        match el.push(val, right) {
            Ok(listsize) => Response::Integer(listsize as i64),
            Err(err) => Response::Error(err.to_string()),
        }
    };
    db.key_publish(&key);
    r
}

fn lpush(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    return generic_push(parser, db, dbindex, false, true);
}

fn rpush(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    return generic_push(parser, db, dbindex, true, true);
}

fn lpushx(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    return generic_push(parser, db, dbindex, false, false);
}

fn rpushx(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    return generic_push(parser, db, dbindex, true, false);
}

fn generic_pop(parser: &Parser, db: &mut Database, dbindex: usize, right: bool) -> Response {
    validate!(parser.argv.len() == 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let r = {
        match db.get_mut(dbindex, &key) {
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
    };
    db.key_publish(&key);
    r
}

fn lpop(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    return generic_pop(parser, db, dbindex, false);
}

fn rpop(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    return generic_pop(parser, db, dbindex, true);
}

fn generic_rpoplpush(db: &mut Database, dbindex: usize, source: &Vec<u8>, destination: &Vec<u8>) -> Response {
    #![allow(unused_must_use)]

    match db.get(dbindex, destination) {
        Some(el) => match el.llen() {
            Ok(_) => (),
            Err(_) => return Response::Error("Destination is not a list".to_owned()),
        },
        None => (),
    }

    let el = {
        let sourcelist = match db.get_mut(dbindex, source) {
            Some(sourcelist) => {
                if sourcelist.llen().is_err() {
                    return Response::Error("Source is not a list".to_owned());
                }
                sourcelist
            },
            None => return Response::Nil,
        };
        match sourcelist.pop(true) {
            Ok(el) => match el {
                Some(el) => el,
                None => return Response::Nil,
            },
            Err(err) => return Response::Error(err.to_string()),
        }
    };

    let resp = {
        let destinationlist = db.get_or_create(dbindex, destination);
        destinationlist.push(el.clone(), false);
        Response::Data(el)
    };
    db.key_publish(source);
    db.key_publish(destination);
    resp
}

fn rpoplpush(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 3, "Wrong number of parameters");
    let source = try_validate!(parser.get_vec(1), "Invalid source");
    let destination = try_validate!(parser.get_vec(2), "Invalid destination");
    generic_rpoplpush(db, dbindex, &source, &destination)
}

fn brpoplpush(parser: &Parser, db: &mut Database, dbindex: usize) -> Result<Response, ResponseError> {
    #![allow(unused_must_use)]
    opt_validate!(parser.argv.len() == 4, "Wrong number of parameters");

    let source = try_opt_validate!(parser.get_vec(1), "Invalid source");
    let destination = try_opt_validate!(parser.get_vec(2), "Invalid destination");
    let timeout = try_opt_validate!(parser.get_i64(3), "Invalid timeout");

    let r = generic_rpoplpush(db, dbindex, &source, &destination);
    if r != Response::Nil {
        return Ok(r);
    }

    let (tx, rx) = channel();
    if timeout > 0 {
        let txc = tx.clone();
        thread::spawn(move || {
            thread::sleep_ms(timeout as u32 * 1000);
            txc.send(false);
        });
    }
    db.key_subscribe(&source, tx);
    return Err(ResponseError::Wait(rx));
}

fn generic_bpop(parser: &Parser, db: &mut Database, dbindex: usize, right: bool) -> Result<Response, ResponseError> {
    #![allow(unused_must_use)]
    opt_validate!(parser.argv.len() >= 3, "Wrong number of parameters");

    let mut keys = vec![];
    for i in 1..parser.argv.len() - 1 {
        let key = try_opt_validate!(parser.get_vec(i), "Invalid key");
        match db.get_mut(dbindex, &key) {
            Some(mut list) => match list.pop(right) {
                Ok(el) => {
                    match el {
                        Some(val) => return Ok(Response::Array(vec![
                                    Response::Data(key),
                                    Response::Data(val),
                                    ])),
                        None => (),
                    }
                }
                Err(err) => return Ok(Response::Error(err.to_string())),
            },
            None => (),
        }
        keys.push(key);
    }
    let timeout = try_opt_validate!(parser.get_i64(parser.argv.len() - 1), "Invalid timeout");

    let (tx, rx) = channel();
    if timeout > 0 {
        let txc = tx.clone();
        thread::spawn(move || {
            thread::sleep_ms(timeout as u32 * 1000);
            txc.send(false);
        });
    }
    for key in keys {
        db.key_subscribe(&key, tx.clone());
    }
    return Err(ResponseError::Wait(rx));
}

fn brpop(parser: &Parser, db: &mut Database, dbindex: usize) -> Result<Response, ResponseError> {
    generic_bpop(parser, db, dbindex, true)
}

fn blpop(parser: &Parser, db: &mut Database, dbindex: usize) -> Result<Response, ResponseError> {
    generic_bpop(parser, db, dbindex, false)
}

fn lindex(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let index = try_validate!(parser.get_i64(2), "Invalid index");
    return match db.get(dbindex, &key) {
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

fn linsert(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 5, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let before_str = try_validate!(parser.get_str(2), "Syntax error");
    let pivot = try_validate!(parser.get_vec(3), "Invalid pivot");
    let value = try_validate!(parser.get_vec(4), "Invalid value");
    let before;
    match &*before_str.to_ascii_lowercase() {
        "after" => before = false,
        "before" => before = true,
        _ => return Response::Error("ERR Syntax error".to_owned()),
    };
    let r = match db.get_mut(dbindex, &key) {
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
    };
    db.key_publish(&key);
    r
}

fn llen(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    return match db.get(dbindex, &key) {
        Some(el) => match el.llen() {
            Ok(listsize) => Response::Integer(listsize as i64),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Integer(0),
    }
}

fn lrange(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 4, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let start = try_validate!(parser.get_i64(2), "Invalid range");
    let stop  = try_validate!(parser.get_i64(3), "Invalid range");
    return match db.get(dbindex, &key) {
        Some(el) => match el.lrange(start, stop) {
            Ok(items) => Response::Array(items.iter().map(|i| Response::Data(i.to_vec())).collect()),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Array(Vec::new()),
    }
}

fn lrem(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 4, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let count = try_validate!(parser.get_i64(2), "Invalid count");
    let value = try_validate!(parser.get_vec(3), "Invalid value");
    let r = match db.get_mut(dbindex, &key) {
        Some(ref mut el) => match el.lrem(count < 0, count.abs() as usize, value) {
            Ok(removed) => Response::Integer(removed as i64),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Array(Vec::new()),
    };
    db.key_publish(&key);
    r
}

fn lset(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 4, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let index = try_validate!(parser.get_i64(2), "Invalid index");
    let value = try_validate!(parser.get_vec(3), "Invalid value");
    let r = match db.get_mut(dbindex, &key) {
        Some(ref mut el) => match el.lset(index, value) {
            Ok(()) => Response::Status("OK".to_owned()),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Error("ERR no such key".to_owned()),
    };
    db.key_publish(&key);
    r
}

fn ltrim(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 4, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let start = try_validate!(parser.get_i64(2), "Invalid start");
    let stop = try_validate!(parser.get_i64(3), "Invalid stop");
    let r = match db.get_mut(dbindex, &key) {
        Some(ref mut el) => match el.ltrim(start, stop) {
            Ok(()) => Response::Status("OK".to_owned()),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Status("OK".to_owned()),
    };
    db.key_publish(&key);
    r
}

fn sadd(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() > 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut count = 0;
    {
        let el = db.get_or_create(dbindex, &key);
        for i in 2..parser.argv.len() {
            let val = try_validate!(parser.get_vec(i), "Invalid value");
            match el.sadd(val) {
                Ok(added) => if added { count += 1 },
                Err(err) => return Response::Error(err.to_string()),
            }
        }
    }
    db.key_publish(&key);
    return Response::Integer(count);
}

fn srem(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() > 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut count = 0;
    {
        let el = db.get_or_create(dbindex, &key);
        for i in 2..parser.argv.len() {
            let val = try_validate!(parser.get_vec(i), "Invalid value");
            match el.srem(&val) {
                Ok(removed) => if removed { count += 1 },
                Err(err) => return Response::Error(err.to_string()),
            }
        }
    }
    db.key_publish(&key);
    return Response::Integer(count);
}

fn sismember(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let member = try_validate!(parser.get_vec(2), "Invalid key");
    Response::Integer(match db.get(dbindex, &key) {
        Some(el) => match el.sismember(&member) {
            Ok(e) => if e { 1 } else { 0 },
            Err(err) => return Response::Error(err.to_string()),
        },
        None => 0,
    })
}

fn srandmember(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 2 || parser.argv.len() == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let value = match db.get(dbindex, &key) {
        Some(el) => el,
        None => return if parser.argv.len() == 2 { Response::Nil } else { Response::Array(vec![]) },
    };
    if parser.argv.len() == 2 {
        match value.srandmember(1, false) {
            Ok(els) => if els.len() > 0 { Response::Data(els[0].clone()) } else { Response::Nil },
            Err(err) => Response::Error(err.to_string()),
        }
    } else {
        let _count = try_validate!(parser.get_i64(2), "Invalid count");
        let count = { if _count < 0 { - _count } else { _count } } as usize;
        let allow_duplicates = _count < 0;
        match value.srandmember(count, allow_duplicates) {
            Ok(els) => Response::Array(els.iter().map(|x| Response::Data(x.clone())).collect::<Vec<_>>()),
            Err(err) => Response::Error(err.to_string()),
        }
    }
}

fn smembers(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let value = match db.get(dbindex, &key) {
        Some(el) => el,
        None => return Response::Array(vec![]),
    };
    match value.smembers() {
        Ok(els) => Response::Array(els.iter().map(|x| Response::Data(x.clone())).collect::<Vec<_>>()),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn spop(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 2 || parser.argv.len() == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut value = match db.get_mut(dbindex, &key) {
        Some(el) => el,
        None => return if parser.argv.len() == 2 { Response::Nil } else { Response::Array(vec![]) },
    };
    if parser.argv.len() == 2 {
        match value.spop(1) {
            Ok(els) => if els.len() > 0 { Response::Data(els[0].clone()) } else { Response::Nil },
            Err(err) => Response::Error(err.to_string()),
        }
    } else {
        let count = try_validate!(parser.get_i64(2), "Invalid count");
        match value.spop(count as usize) {
            Ok(els) => Response::Array(els.iter().map(|x| Response::Data(x.clone())).collect::<Vec<_>>()),
            Err(err) => Response::Error(err.to_string()),
        }
    }
}

fn smove(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 4, "Wrong number of parameters");
    let source_key = try_validate!(parser.get_vec(1), "Invalid key");
    let destination_key = try_validate!(parser.get_vec(2), "Invalid destination");
    let member = try_validate!(parser.get_vec(3), "Invalid member");

    {
        match db.get(dbindex, &destination_key) {
            Some(e) => if !e.is_set() { return Response::Error("Invalid destination".to_owned()); },
            None => (),
        }
    }
    {
        let source = match db.get_mut(dbindex, &source_key) {
            Some(s) => s,
            None => return Response::Integer(0),
        };

        match source.srem(&member) {
            Ok(removed) => if !removed { return Response::Integer(0); },
            Err(err) => return Response::Error(err.to_string()),
        }
    }

    {
        let destination = db.get_or_create(dbindex, &destination_key);
        match destination.sadd(member) {
            Ok(_) => (),
            Err(err) => panic!("Unexpected failure {}", err.to_string()),
        }
    }

    db.key_publish(&source_key);
    db.key_publish(&destination_key);
    return Response::Integer(1);
}

fn scard(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Integer(0),
    };
    return match el.scard() {
        Ok(count) => Response::Integer(count as i64),
        Err(err) => return Response::Error(err.to_string()),
    }
}

fn sdiff(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    let (el, sets) = get_values!(1, parser, db, dbindex);

    match el.sdiff(&sets) {
        Ok(set) => {
            Response::Array(set.iter().map(|x| Response::Data(x.clone())).collect::<Vec<_>>())
        },
        Err(err) => Response::Error(err.to_string()),
    }
}

fn sdiffstore(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let destination_key = try_validate!(parser.get_vec(1), "Invalid destination");
    let set = {
        let (el, sets) = get_values!(2, parser, db, dbindex);
        match el.sdiff(&sets) {
            Ok(set) => set,
            Err(err) => return Response::Error(err.to_string()),
        }
    };

    db.remove(dbindex, &destination_key);
    let r = set.len() as i64;
    db.get_or_create(dbindex, &destination_key).create_set(set);
    Response::Integer(r)
}

fn sinter(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    let (el, sets) = get_values!(1, parser, db, dbindex);
    return match el.sinter(&sets) {
        Ok(set) => {
            Response::Array(set.iter().map(|x| Response::Data(x.clone())).collect::<Vec<_>>())
        },
        Err(err) => Response::Error(err.to_string()),
    }
}

fn sinterstore(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let destination_key = try_validate!(parser.get_vec(1), "Invalid destination");
    let set = {
        let (el, sets) = get_values!(2, parser, db, dbindex);
        match el.sinter(&sets) {
            Ok(set) => set,
            Err(err) => return Response::Error(err.to_string()),
        }
    };

    db.remove(dbindex, &destination_key);
    let r = set.len() as i64;
    db.get_or_create(dbindex, &destination_key).create_set(set);
    Response::Integer(r)
}

fn sunion(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    let (el, sets) = get_values!(1, parser, db, dbindex);
    return match el.sunion(&sets) {
        Ok(set) => {
            Response::Array(set.iter().map(|x| Response::Data(x.clone())).collect::<Vec<_>>())
        },
        Err(err) => Response::Error(err.to_string()),
    }
}

fn sunionstore(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let destination_key = try_validate!(parser.get_vec(1), "Invalid destination");
    let set = {
        let (el, sets) = get_values!(2, parser, db, dbindex);
        match el.sunion(&sets) {
            Ok(set) => set,
            Err(err) => return Response::Error(err.to_string()),
        }
    };

    db.remove(dbindex, &destination_key);
    let r = set.len() as i64;
    db.get_or_create(dbindex, &destination_key).create_set(set);
    Response::Integer(r)
}

fn zadd(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    let len = parser.argv.len();
    validate!(len >= 4, "Wrong number of parameters");
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;
    let mut incr = false;
    let mut i = 2;

    // up to 4 optional flags
    for _ in 0..4 {
        let opt = match parser.get_str(i) {
            Ok(s) => s,
            Err(_) => break,
        };
        i += 1;
        match &*opt.to_ascii_lowercase() {
            "nx" => nx = true,
            "xx" => xx = true,
            "ch" => ch = true,
            "incr" => incr = true,
            _ => {i -= 1; break},
        }
    }

    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut count = 0;
    {
        let el = db.get_or_create(dbindex, &key);
        for _ in 0..((len - i) / 2) {
            let score = try_validate!(parser.get_f64(i), "Invalid score");
            let val = try_validate!(parser.get_vec(i + 1), "Invalid value");
            match el.zadd(score, val, nx, xx, ch, incr) {
                Ok(added) => if added { count += 1 },
                Err(err) => return Response::Error(err.to_string()),
            }
            i += 2; // omg, so ugly `for`
        }
    }
    if count > 0 {
        db.key_publish(&key);
    }
    return Response::Integer(count);
}

fn zincrby(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 4, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let newscore = {
        let el = db.get_or_create(dbindex, &key);
        let score = try_validate!(parser.get_f64(2), "Invalid score");
        let member = try_validate!(parser.get_vec(3), "Invalid member");
        match el.zincrby(score, member) {
            Ok(score) => score,
            Err(err) => return Response::Error(err.to_string()),
        }
    };
    db.key_publish(&key);
    return Response::Data(format!("{}", newscore).into_bytes());
}

fn zrem(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut count = 0;
    {
        let el = match db.get_mut(dbindex, &key) {
            Some(el) => el,
            None => return Response::Integer(0),
        };
        for i in 2..parser.argv.len() {
            let member = try_validate!(parser.get_vec(i), "Invalid member");
            match el.zrem(member) {
                Ok(removed) => if removed { count += 1 },
                Err(err) => return Response::Error(err.to_string()),
            }
        }
    }
    if count > 0 {
        db.key_publish(&key);
    }
    return Response::Integer(count);
}

fn zcount(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 4, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let min = try_validate!(parser.get_f64_bound(2), "Invalid min");
    let max = try_validate!(parser.get_f64_bound(3), "Invalid max");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Integer(0),
    };
    match el.zcount(min, max) {
        Ok(c) => Response::Integer(c as i64),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn generic_zrange(parser: &Parser, db: &mut Database, dbindex: usize, rev: bool) -> Response {
    validate!(parser.argv.len() == 4 || parser.argv.len() == 5, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let start = try_validate!(parser.get_i64(2), "Invalid start");
    let stop = try_validate!(parser.get_i64(3), "Invalid stop");
    let withscores = parser.argv.len() == 5;
    if withscores {
        let p4 = try_validate!(parser.get_str(4), "Syntax error");
        validate!(p4.to_ascii_lowercase() == "withscores", "Syntax error");
    }
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(Vec::new()),
    };
    match el.zrange(start, stop, withscores, rev) {
        Ok(r) => Response::Array(r.iter().map(|x| Response::Data(x.clone())).collect::<Vec<_>>()),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn zrange(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    generic_zrange(parser, db, dbindex, false)
}

fn zrevrange(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    generic_zrange(parser, db, dbindex, true)
}

fn generic_zrangebyscore(parser: &Parser, db: &mut Database, dbindex: usize, rev: bool) -> Response {
    let len = parser.argv.len();
    validate!(len == 4 || len == 5 || len == 7 || len == 8, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let min = try_validate!(parser.get_f64_bound(2), "Invalid min");
    let max = try_validate!(parser.get_f64_bound(3), "Invalid max");
    let withscores = len == 5 || len == 8;
    if withscores {
        let p4 = try_validate!(parser.get_str(4), "Syntax error");
        validate!(p4.to_ascii_lowercase() == "withscores", "Syntax error");
    }

    let mut offset = 0;
    let mut count = usize::MAX;
    let limit = len >= 7;
    if limit {
        let p = try_validate!(parser.get_str(len - 3), "Syntax error");
        validate!(p.to_ascii_lowercase() == "limit", "Syntax error");
        offset = try_validate!(parser.get_i64(len - 2), "Syntax error") as usize;
        count = try_validate!(parser.get_i64(len - 1), "Syntax error") as usize;
    }

    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(Vec::new()),
    };
    match el.zrangebyscore(min, max, withscores, offset, count, rev) {
        Ok(r) => Response::Array(r.iter().map(|x| Response::Data(x.clone())).collect::<Vec<_>>()),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn zrangebyscore(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    generic_zrangebyscore(parser, db, dbindex, false)
}

fn zrevrangebyscore(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    generic_zrangebyscore(parser, db, dbindex, true)
}

fn zrank(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let member = try_validate!(parser.get_vec(2), "Invalid member");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Nil,
    };
    match el.zrank(member) {
        Ok(r) => match r {
            Some(v) => Response::Integer(v as i64),
            None => Response::Nil,
        },
        Err(err) => Response::Error(err.to_string()),
    }
}

fn ping(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    #![allow(unused_variables)]
    validate!(parser.argv.len() <= 2, "Wrong number of parameters");
    if parser.argv.len() == 2 {
        match parser.get_vec(1) {
            Ok(r) => Response::Data(r),
            Err(err) => Response::Error(err.to_string()),
        }
    } else {
        Response::Data(b"PONG".to_vec())
    }
}

fn subscribe(
        parser: &Parser,
        db: &mut Database,
        subscriptions: &mut HashMap<Vec<u8>, usize>,
        pattern_subscriptions_len: usize,
        sender: &Sender<PubsubEvent>
        ) -> Result<Response, ResponseError> {
    opt_validate!(parser.argv.len() >= 2, "Wrong number of parameters");
    for i in 1..parser.argv.len() {
        let channel_name = try_opt_validate!(parser.get_vec(i), "Invalid channel");
        let subscriber_id = db.subscribe(channel_name.clone(), sender.clone());
        subscriptions.insert(channel_name.clone(), subscriber_id);
        match sender.send(PubsubEvent::Subscription(channel_name.clone(), pattern_subscriptions_len + subscriptions.len())) {
            Ok(_) => None,
            Err(_) => subscriptions.remove(&channel_name),
        };
    }
    Err(ResponseError::NoReply)
}

fn unsubscribe(
        parser: &Parser,
        db: &mut Database,
        subscriptions: &mut HashMap<Vec<u8>, usize>,
        pattern_subscriptions_len: usize,
        sender: &Sender<PubsubEvent>
        ) -> Result<Response, ResponseError> {
    #![allow(unused_must_use)]
    opt_validate!(parser.argv.len() >= 2, "Wrong number of parameters");
    for i in 1..parser.argv.len() {
        let channel_name = try_opt_validate!(parser.get_vec(i), "Invalid channel");
        match subscriptions.remove(&channel_name) {
            Some(subscriber_id) => {
                db.unsubscribe(channel_name.clone(), subscriber_id);
                sender.send(PubsubEvent::Unsubscription(channel_name, pattern_subscriptions_len + subscriptions.len()));
            },
            None => (),
        }
    }
    Err(ResponseError::NoReply)
}

fn psubscribe(
        parser: &Parser,
        db: &mut Database,
        subscriptions_len: usize,
        pattern_subscriptions: &mut HashMap<Vec<u8>, usize>,
        sender: &Sender<PubsubEvent>
        ) -> Result<Response, ResponseError> {
    opt_validate!(parser.argv.len() >= 2, "Wrong number of parameters");
    for i in 1..parser.argv.len() {
        let pattern = try_opt_validate!(parser.get_vec(i), "Invalid channel");
        let subscriber_id = db.psubscribe(pattern.clone(), sender.clone());
        pattern_subscriptions.insert(pattern.clone(), subscriber_id);
        match sender.send(PubsubEvent::PatternSubscription(pattern.clone(), subscriptions_len + pattern_subscriptions.len())) {
            Ok(_) => None,
            Err(_) => pattern_subscriptions.remove(&pattern),
        };
    }
    Err(ResponseError::NoReply)
}

fn punsubscribe(
        parser: &Parser,
        db: &mut Database,
        subscriptions_len: usize,
        pattern_subscriptions: &mut HashMap<Vec<u8>, usize>,
        sender: &Sender<PubsubEvent>
        ) -> Result<Response, ResponseError> {
    #![allow(unused_must_use)]
    opt_validate!(parser.argv.len() >= 2, "Wrong number of parameters");
    for i in 1..parser.argv.len() {
        let pattern = try_opt_validate!(parser.get_vec(i), "Invalid channel");
        match pattern_subscriptions.remove(&pattern) {
            Some(subscriber_id) => {
                db.punsubscribe(pattern.clone(), subscriber_id);
                sender.send(PubsubEvent::PatternUnsubscription(pattern, subscriptions_len + pattern_subscriptions.len()));
            },
            None => (),
        }
    }
    Err(ResponseError::NoReply)}

fn publish(parser: &Parser, db: &mut Database) -> Response {
    validate!(parser.argv.len() == 3, "Wrong number of parameters");
    let channel_name = try_validate!(parser.get_vec(1), "Invalid channel");
    let message = try_validate!(parser.get_vec(2), "Invalid channel");
    Response::Integer(db.publish(&channel_name, &message) as i64)
}

pub fn command(
        parser: &Parser,
        db: &mut Database,
        _dbindex: &mut usize,
        subscriptions: Option<&mut HashMap<Vec<u8>, usize>>,
        pattern_subscriptions: Option<&mut HashMap<Vec<u8>, usize>>,
        sender: Option<&Sender<PubsubEvent>>
        ) -> Result<Response, ResponseError> {
    opt_validate!(parser.argv.len() > 0, "Not enough arguments");
    let command = try_opt_validate!(parser.get_str(0), "Invalid command");
    if command == "select" {
        opt_validate!(parser.argv.len() == 2, "Wrong number of parameters");
        let dbindex = try_opt_validate!(parser.get_i64(1), "Invalid dbindex") as usize;
        if dbindex > db.size {
            return Ok(Response::Error("dbindex out of range".to_owned()));
        }
        *_dbindex = dbindex;
        return Ok(Response::Status("OK".to_owned()));
    }
    let dbindex = _dbindex.clone();
    return Ok(match command {
        "pexpireat" => pexpireat(parser, db, dbindex),
        "pexpire" => pexpire(parser, db, dbindex),
        "expireat" => expireat(parser, db, dbindex),
        "expire" => expire(parser, db, dbindex),
        "ttl" => ttl(parser, db, dbindex),
        "pttl" => pttl(parser, db, dbindex),
        "persist" => persist(parser, db, dbindex),
        "type" => dbtype(parser, db, dbindex),
        "set" => set(parser, db, dbindex),
        "setnx" => setnx(parser, db, dbindex),
        "setex" => setex(parser, db, dbindex),
        "psetex" => psetex(parser, db, dbindex),
        "del" => del(parser, db, dbindex),
        "append" => append(parser, db, dbindex),
        "get" => get(parser, db, dbindex),
        "getrange" => getrange(parser, db, dbindex),
        "mget" => mget(parser, db, dbindex),
        "substr" => getrange(parser, db, dbindex),
        "setrange" => setrange(parser, db, dbindex),
        "setbit" => setbit(parser, db, dbindex),
        "getbit" => getbit(parser, db, dbindex),
        "strlen" => strlen(parser, db, dbindex),
        "incr" => incr(parser, db, dbindex),
        "decr" => decr(parser, db, dbindex),
        "incrby" => incrby(parser, db, dbindex),
        "decrby" => decrby(parser, db, dbindex),
        "exists" => exists(parser, db, dbindex),
        "ping" => ping(parser, db, dbindex),
        "flushdb" => flushdb(parser, db, dbindex),
        "flushall" => flushall(parser, db, dbindex),
        "lpush" => lpush(parser, db, dbindex),
        "rpush" => rpush(parser, db, dbindex),
        "lpushx" => lpushx(parser, db, dbindex),
        "rpushx" => rpushx(parser, db, dbindex),
        "lpop" => lpop(parser, db, dbindex),
        "rpop" => rpop(parser, db, dbindex),
        "lindex" => lindex(parser, db, dbindex),
        "linsert" => linsert(parser, db, dbindex),
        "llen" => llen(parser, db, dbindex),
        "lrange" => lrange(parser, db, dbindex),
        "lrem" => lrem(parser, db, dbindex),
        "lset" => lset(parser, db, dbindex),
        "ltrim" => ltrim(parser, db, dbindex),
        "rpoplpush" => rpoplpush(parser, db, dbindex),
        "brpoplpush" => return brpoplpush(parser, db, dbindex),
        "brpop" => return brpop(parser, db, dbindex),
        "blpop" => return blpop(parser, db, dbindex),
        "sadd" => sadd(parser, db, dbindex),
        "srem" => srem(parser, db, dbindex),
        "sismember" => sismember(parser, db, dbindex),
        "smembers" => smembers(parser, db, dbindex),
        "srandmember" => srandmember(parser, db, dbindex),
        "spop" => spop(parser, db, dbindex),
        "smove" => smove(parser, db, dbindex),
        "scard" => scard(parser, db, dbindex),
        "sdiff" => sdiff(parser, db, dbindex),
        "sdiffstore" => sdiffstore(parser, db, dbindex),
        "sinter" => sinter(parser, db, dbindex),
        "sinterstore" => sinterstore(parser, db, dbindex),
        "sunion" => sunion(parser, db, dbindex),
        "sunionstore" => sunionstore(parser, db, dbindex),
        "zadd" => zadd(parser, db, dbindex),
        "zincrby" => zincrby(parser, db, dbindex),
        "zrem" => zrem(parser, db, dbindex),
        "zcount" => zcount(parser, db, dbindex),
        "zrange" => zrange(parser, db, dbindex),
        "zrevrange" => zrevrange(parser, db, dbindex),
        "zrangebyscore" => zrangebyscore(parser, db, dbindex),
        "zrevrangebyscore" => zrevrangebyscore(parser, db, dbindex),
        "zrank" => zrank(parser, db, dbindex),
        "subscribe" => return subscribe(parser, db, subscriptions.unwrap(), pattern_subscriptions.unwrap().len(), sender.unwrap()),
        "unsubscribe" => return unsubscribe(parser, db, subscriptions.unwrap(), pattern_subscriptions.unwrap().len(), sender.unwrap()),
        "psubscribe" => return psubscribe(parser, db, subscriptions.unwrap().len(), pattern_subscriptions.unwrap(), sender.unwrap()),
        "punsubscribe" => return punsubscribe(parser, db, subscriptions.unwrap().len(), pattern_subscriptions.unwrap(), sender.unwrap()),
        "publish" => publish(parser, db),
        _ => Response::Error("Unknown command".to_owned()),
    });
}

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
        &Value::String(ref value) => from_utf8(&*value.to_vec()).unwrap().to_owned(),
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
    assert_eq!(db.get(0, &b"key".to_vec()).unwrap().get().unwrap(), b"valuevalue".to_vec());
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
fn zrevrange_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"zadd key 1 a 2 b 3 c 4 d"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(4));
    assert_eq!(command(&parser!(b"zrevrange key 0 0"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data(b"d".to_vec()),
                ]));
    assert_eq!(command(&parser!(b"zrevrange key 0 0 withscoreS"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data(b"d".to_vec()),
                Response::Data(b"4".to_vec()),
                ]));
    assert_eq!(command(&parser!(b"zrevrange key -2 -1 WITHSCORES"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data(b"b".to_vec()),
                Response::Data(b"2".to_vec()),
                Response::Data(b"a".to_vec()),
                Response::Data(b"1".to_vec()),
                ]));
}

#[test]
fn zrangebyscore_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"zadd key 1 a 2 b 3 c 4 d"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(4));
    assert_eq!(command(&parser!(b"zrangebyscore key 1 (2"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data(b"a".to_vec()),
                ]));
    assert_eq!(command(&parser!(b"zrangebyscore key 1 1 withscoreS"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data(b"a".to_vec()),
                Response::Data(b"1".to_vec()),
                ]));
    assert_eq!(command(&parser!(b"zrangebyscore key (2 inf WITHSCORES"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data(b"c".to_vec()),
                Response::Data(b"3".to_vec()),
                Response::Data(b"d".to_vec()),
                Response::Data(b"4".to_vec()),
                ]));
    assert_eq!(command(&parser!(b"zrangebyscore key -inf inf withscores LIMIT 2 10"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data(b"c".to_vec()),
                Response::Data(b"3".to_vec()),
                Response::Data(b"d".to_vec()),
                Response::Data(b"4".to_vec()),
                ]));
}

#[test]
fn zrevrangebyscore_command() {
    let mut db = Database::mock();
    assert_eq!(command(&parser!(b"zadd key 1 a 2 b 3 c 4 d"), &mut db, &mut 0, None, None, None).unwrap(), Response::Integer(4));
    assert_eq!(command(&parser!(b"zrevrangebyscore key (2 1"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data(b"a".to_vec()),
                ]));
    assert_eq!(command(&parser!(b"zrevrangebyscore key 1 1 withscoreS"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data(b"a".to_vec()),
                Response::Data(b"1".to_vec()),
                ]));
    assert_eq!(command(&parser!(b"zrevrangebyscore key inf (2 WITHSCORES"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data(b"d".to_vec()),
                Response::Data(b"4".to_vec()),
                Response::Data(b"c".to_vec()),
                Response::Data(b"3".to_vec()),
                ]));
    assert_eq!(command(&parser!(b"zrevrangebyscore key inf -inf withscores LIMIT 2 10"), &mut db, &mut 0, None, None, None).unwrap(), Response::Array(vec![
                Response::Data(b"b".to_vec()),
                Response::Data(b"2".to_vec()),
                Response::Data(b"a".to_vec()),
                Response::Data(b"1".to_vec()),
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
