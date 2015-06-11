use std::ascii::AsciiExt;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter, Error};
use std::sync::mpsc::Sender;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::channel;
use std::thread;

use super::database::PubsubEvent;
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

pub enum ResponseError {
    NoReply,
    Wait(Receiver<bool>),
}

impl Debug for ResponseError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        match self {
            &ResponseError::NoReply => write!(f, "NoReply"),
            &ResponseError::Wait(_) => write!(f, "Wait"),
        }
    }
}

impl Response {
    pub fn as_bytes(&self) -> Vec<u8> {
        return match *self {
            Response::Nil => b"$-1\r\n".to_vec(),
            Response::Data(ref d) => [&b"$"[..], &format!("{}\r\n", d.len()).into_bytes()[..], &d[..], &"\r\n".to_owned().into_bytes()[..]].concat(),
            Response::Integer(ref i) => [&b":"[..], &format!("{}\r\n", i).into_bytes()[..]].concat(),
            Response::Error(ref d) => [&b"-"[..], (*d).as_bytes(), &"\r\n".to_owned().into_bytes()[..]].concat(),
            Response::Status(ref d) => [&b"+"[..], (*d).as_bytes(), &"\r\n".to_owned().into_bytes()[..]].concat(),
            Response::Array(ref a) => [&b"*"[..],  &format!("{}\r\n", a.len()).into_bytes()[..],
                &(a.iter().map(|el| el.as_bytes()).collect::<Vec<_>>()[..].concat())[..]
                ].concat()
        }
    }
}

macro_rules! opt_validate {
    ($expr: expr, $err: expr) => (
        if !($expr) {
            return Ok(Response::Error($err.to_string()));
        }
    )
}

macro_rules! try_opt_validate {
    ($expr: expr, $err: expr) => ({
        let res = $expr;
        opt_validate!(res.is_ok(), $err);
        res.unwrap()
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
        let res = $expr;
        validate!(res.is_ok(), $err);
        res.unwrap()
    })
}

fn set(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argc == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let val = try_validate!(parser.get_vec(2), "Invalid value");
    let r = match db.get_or_create(dbindex, &key).set(val) {
        Ok(_) => Response::Status("OK".to_owned()),
        Err(err) => Response::Error(err.to_string()),
    };
    db.key_publish(&key);
    r
}

fn exists(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argc == 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    Response::Integer(match db.get(dbindex, &key) {
        Some(_) => 1,
        None => 0,
    })
}

fn del(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argc >= 2, "Wrong number of parameters");
    let mut c = 0;
    for i in 1..parser.argc {
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

fn flushdb(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argc == 1, "Wrong number of parameters");
    db.clear(dbindex);
    return Response::Status("OK".to_owned());
}

fn flushall(parser: &Parser, db: &mut Database, _: usize) -> Response {
    validate!(parser.argc == 1, "Wrong number of parameters");
    db.clearall();
    return Response::Status("OK".to_owned());
}

fn append(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argc == 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let val = try_validate!(parser.get_vec(2), "Invalid value");
    let r = match db.get_or_create(dbindex, &key).append(val) {
        Ok(len) => Response::Integer(len as i64),
        Err(err) => Response::Error(err.to_string()),
    };
    db.key_publish(&key);
    r
}

fn get(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    validate!(parser.argc == 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let obj = db.get(dbindex, &key);
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
    validate!(parser.argc == 2, "Wrong number of parameters");
    return generic_incr(parser, db, dbindex, 1);
}

fn decr(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argc == 2, "Wrong number of parameters");
    return generic_incr(parser, db, dbindex, -1);
}

fn incrby(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argc == 3, "Wrong number of parameters");
    let try_increment = parser.get_i64(2);
    if try_increment.is_err() { return Response::Error("Invalid increment".to_owned()); }
    return generic_incr(parser, db, dbindex, try_increment.unwrap());
}

fn decrby(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argc == 3, "Wrong number of parameters");
    let try_increment = parser.get_i64(2);
    if try_increment.is_err() { return Response::Error("Invalid decrement".to_owned()); }
    return generic_incr(parser, db, dbindex, -try_increment.unwrap());
}

fn generic_push(parser: &Parser, db: &mut Database, dbindex: usize, right: bool, create: bool) -> Response {
    // TODO variadic
    validate!(parser.argc == 3, "Wrong number of parameters");
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
    validate!(parser.argc == 2, "Wrong number of parameters");
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
    validate!(parser.argc == 3, "Wrong number of parameters");
    let source = try_validate!(parser.get_vec(1), "Invalid source");
    let destination = try_validate!(parser.get_vec(2), "Invalid destination");
    generic_rpoplpush(db, dbindex, &source, &destination)
}

fn brpoplpush(parser: &Parser, db: &mut Database, dbindex: usize) -> Result<Response, ResponseError> {
    #![allow(unused_must_use)]
    opt_validate!(parser.argc == 4, "Wrong number of parameters");

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

fn lindex(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    validate!(parser.argc == 3, "Wrong number of parameters");
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
    validate!(parser.argc == 5, "Wrong number of parameters");
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
    validate!(parser.argc == 2, "Wrong number of parameters");
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
    validate!(parser.argc == 4, "Wrong number of parameters");
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
    validate!(parser.argc == 4, "Wrong number of parameters");
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
    validate!(parser.argc == 4, "Wrong number of parameters");
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
    validate!(parser.argc == 4, "Wrong number of parameters");
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
    validate!(parser.argc > 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut count = 0;
    {
        let el = db.get_or_create(dbindex, &key);
        for i in 2..parser.argc {
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

fn scard(parser: &Parser, db: &Database, dbindex: usize) -> Response {
    validate!(parser.argc == 2, "Wrong number of parameters");
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
    validate!(parser.argc >= 2, "Wrong number of parameters");
    let mut sets = Vec::with_capacity(parser.argc - 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(vec![]),
    };
    for i in 2..parser.argc {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        match db.get(dbindex, &key) {
            Some(e) => sets.push(e),
            None => (),
        };
    }
    return match el.sdiff(&sets) {
        Ok(set) => {
            Response::Array(set.iter().map(|x| Response::Data(x.clone())).collect::<Vec<_>>())
        },
        Err(err) => Response::Error(err.to_string()),
    }
}

fn sdiffstore(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argc >= 3, "Wrong number of parameters");
    let destination_key = try_validate!(parser.get_vec(1), "Invalid destination");
    let key = try_validate!(parser.get_vec(2), "Invalid key");
    let set = {
        let mut sets = Vec::with_capacity(parser.argc - 3);
        let el = match db.get(dbindex, &key) {
            Some(e) => e,
            None => return Response::Array(vec![]),
        };
        for i in 3..parser.argc {
            let key = try_validate!(parser.get_vec(i), "Invalid key");
            match db.get(dbindex, &key) {
                Some(e) => sets.push(e),
                None => (),
            };
        }
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

fn ping(parser: &Parser, db: &mut Database, dbindex: usize) -> Response {
    #![allow(unused_variables)]
    validate!(parser.argc <= 2, "Wrong number of parameters");
    if parser.argc == 2 {
        return Response::Data(parser.get_vec(1).unwrap());
    }
    return Response::Data(b"PONG".to_vec());
}

fn subscribe(
        parser: &Parser,
        db: &mut Database,
        subscriptions: &mut HashMap<Vec<u8>, usize>,
        pattern_subscriptions_len: usize,
        sender: &Sender<PubsubEvent>
        ) -> Result<Response, ResponseError> {
    opt_validate!(parser.argc >= 2, "Wrong number of parameters");
    for i in 1..parser.argc {
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
    opt_validate!(parser.argc >= 2, "Wrong number of parameters");
    for i in 1..parser.argc {
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
    opt_validate!(parser.argc >= 2, "Wrong number of parameters");
    for i in 1..parser.argc {
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
    opt_validate!(parser.argc >= 2, "Wrong number of parameters");
    for i in 1..parser.argc {
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
    validate!(parser.argc == 3, "Wrong number of parameters");
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
    opt_validate!(parser.argc > 0, "Not enough arguments");
    let command = try_opt_validate!(parser.get_str(0), "Invalid command");
    if command == "select" {
        opt_validate!(parser.argc == 2, "Wrong number of parameters");
        let dbindex = try_opt_validate!(parser.get_i64(1), "Invalid dbindex") as usize;
        if dbindex > db.size {
            return Ok(Response::Error("dbindex out of range".to_owned()));
        }
        *_dbindex = dbindex;
        return Ok(Response::Status("OK".to_owned()));
    }
    let dbindex = _dbindex.clone();
    return Ok(match command {
        "set" => set(parser, db, dbindex),
        "del" => del(parser, db, dbindex),
        "append" => append(parser, db, dbindex),
        "get" => get(parser, db, dbindex),
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
        "sadd" => sadd(parser, db, dbindex),
        "scard" => scard(parser, db, dbindex),
        "sdiff" => sdiff(parser, db, dbindex),
        "sdiffstore" => sdiffstore(parser, db, dbindex),
        "subscribe" => return subscribe(parser, db, subscriptions.unwrap(), pattern_subscriptions.unwrap().len(), sender.unwrap()),
        "unsubscribe" => return unsubscribe(parser, db, subscriptions.unwrap(), pattern_subscriptions.unwrap().len(), sender.unwrap()),
        "psubscribe" => return psubscribe(parser, db, subscriptions.unwrap().len(), pattern_subscriptions.unwrap(), sender.unwrap()),
        "punsubscribe" => return punsubscribe(parser, db, subscriptions.unwrap().len(), pattern_subscriptions.unwrap(), sender.unwrap()),
        "publish" => publish(parser, db),
        _ => Response::Error("Unknown command".to_owned()),
    });
}
