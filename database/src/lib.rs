#![feature(collections_bound)]
#![feature(drain)]
#![feature(vecmap)]

extern crate config;
extern crate rand;
extern crate rehashinghashmap;
extern crate response;
extern crate skiplist;
extern crate util;

pub mod dbutil;
pub mod error;
pub mod list;
pub mod set;
pub mod string;
pub mod zset;

use std::usize;
use std::collections::Bound;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::mpsc::{Sender, channel};

use config::Config;
use rehashinghashmap::RehashingHashMap;
use response::Response;
use util::glob_match;
use util::mstime;

use error::OperationError;
use list::ValueList;
use set::ValueSet;
use string::ValueString;
use zset::SortedSetMember;
use zset::ValueSortedSet;

#[derive(PartialEq, Debug)]
pub enum Value {
    Nil,
    String(ValueString),
    List(ValueList),
    Set(ValueSet),
    SortedSet(ValueSortedSet),
}

#[derive(PartialEq, Debug)]
pub enum PubsubEvent {
    Subscription(Vec<u8>, usize),
    Unsubscription(Vec<u8>, usize),
    PatternSubscription(Vec<u8>, usize),
    PatternUnsubscription(Vec<u8>, usize),
    Message(Vec<u8>, Option<Vec<u8>>, Vec<u8>),
}

impl PubsubEvent {
    pub fn as_response(&self) -> Response {
        match self {
            &PubsubEvent::Message(ref channel, ref pattern, ref message) => match pattern {
                &Some(ref pattern) => Response::Array(vec![
                        Response::Data(b"message".to_vec()),
                        Response::Data(channel.clone()),
                        Response::Data(pattern.clone()),
                        Response::Data(message.clone()),
                        ]),
                &None => Response::Array(vec![
                        Response::Data(b"message".to_vec()),
                        Response::Data(channel.clone()),
                        Response::Data(message.clone()),
                        ]),
            },
            &PubsubEvent::Subscription(ref channel, ref subscriptions) => Response::Array(vec![
                    Response::Data(b"subscribe".to_vec()),
                    Response::Data(channel.clone()),
                    Response::Integer(subscriptions.clone() as i64),
                    ]),
            &PubsubEvent::Unsubscription(ref channel, ref subscriptions) => Response::Array(vec![
                    Response::Data(b"unsubscribe".to_vec()),
                    Response::Data(channel.clone()),
                    Response::Integer(subscriptions.clone() as i64),
                    ]),
            &PubsubEvent::PatternSubscription(ref pattern, ref subscriptions) => Response::Array(vec![
                    Response::Data(b"psubscribe".to_vec()),
                    Response::Data(pattern.clone()),
                    Response::Integer(subscriptions.clone() as i64),
                    ]),
            &PubsubEvent::PatternUnsubscription(ref pattern, ref subscriptions) => Response::Array(vec![
                    Response::Data(b"punsubscribe".to_vec()),
                    Response::Data(pattern.clone()),
                    Response::Integer(subscriptions.clone() as i64),
                    ]),
        }
    }
}

impl Value {
    pub fn is_nil(&self) -> bool {
        match self {
            &Value::Nil => true,
            _ => false,
        }
    }

    pub fn is_string(&self) -> bool {
        match self {
            &Value::String(_) => true,
            _ => false,
        }
    }

    pub fn is_list(&self) -> bool {
        match self {
            &Value::List(_) => true,
            _ => false,
        }
    }

    pub fn is_set(&self) -> bool {
        match self {
            &Value::Set(_) => true,
            _ => false,
        }
    }

    pub fn set(&mut self, newvalue: Vec<u8>) -> Result<(), OperationError> {
        *self = Value::String(ValueString::new(newvalue));
        return Ok(());
    }

    pub fn get(&self) -> Result<Vec<u8>, OperationError> {
        match self {
            &Value::String(ref value) => Ok(value.to_vec()),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn strlen(&self) -> Result<usize, OperationError> {
        match self {
            &Value::String(ref val) => Ok(val.strlen()),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn append(&mut self, newvalue: Vec<u8>) -> Result<usize, OperationError> {
        match self {
            &mut Value::Nil => {
                let len = newvalue.len();
                *self = Value::String(ValueString::new(newvalue));
                Ok(len)
            },
            &mut Value::String(ref mut val) => { val.append(newvalue); Ok(val.strlen()) },
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn incr(&mut self, incr: i64) -> Result<i64, OperationError> {
        let mut newval:i64;
        match self {
            &mut Value::Nil => {
                newval = incr;
                *self = Value::String(ValueString::Integer(newval.clone()));
                return Ok(newval);
            },
            &mut Value::String(ref mut value) => value.incr(incr),
            _ => return Err(OperationError::WrongTypeError),
        }
    }

    pub fn getrange(&self, start: i64, stop: i64) -> Result<Vec<u8>, OperationError> {
        match self {
            &Value::Nil => Ok(Vec::new()),
            &Value::String(ref value) => Ok(value.getrange(start, stop)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn setrange(&mut self, index: i64, data: Vec<u8>) -> Result<usize, OperationError> {
        match self {
            &mut Value::Nil => *self = Value::String(ValueString::Data(Vec::new())),
            &mut Value::String(_) => (),
            _ => return Err(OperationError::WrongTypeError),
        };

        match self {
            &mut Value::String(ref mut value) => Ok(value.setrange(index, data)),
            _ => panic!("Expected value to be a string"),
        }
    }

    pub fn setbit(&mut self, bitoffset: usize, on: bool) -> Result<bool, OperationError> {
        match self {
            &mut Value::Nil => *self = Value::String(ValueString::Data(Vec::new())),
            &mut Value::String(_) => (),
            _ => return Err(OperationError::WrongTypeError),
        }

        match self {
            &mut Value::String(ref mut value) => Ok(value.setbit(bitoffset, on)),
            _ => panic!("Value must be a string")
        }
    }

    pub fn getbit(&self, bitoffset: usize) -> Result<bool, OperationError> {
        match self {
            &Value::Nil => return Ok(false),
            &Value::String(ref value) => Ok(value.getbit(bitoffset)),
            _ => return Err(OperationError::WrongTypeError),
        }
    }

    pub fn push(&mut self, el: Vec<u8>, right: bool) -> Result<usize, OperationError> {
        Ok(match self {
            &mut Value::Nil => {
                let mut list = ValueList::new();
                list.push(el, right);
                *self = Value::List(list);
                1
            },
            &mut Value::List(ref mut list) => { list.push(el, right); list.llen() },
            _ => return Err(OperationError::WrongTypeError),
        })
    }

    pub fn pop(&mut self, right: bool) -> Result<Option<Vec<u8>>, OperationError> {
        Ok(match self {
            &mut Value::Nil => None,
            &mut Value::List(ref mut list) => list.pop(right),
            _ => return Err(OperationError::WrongTypeError),
        })
    }

    pub fn lindex(&self, index: i64) -> Result<Option<&Vec<u8>>, OperationError> {
        match *self {
            Value::List(ref value) => Ok(value.lindex(index)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn linsert(&mut self, before: bool, pivot: Vec<u8>, newvalue: Vec<u8>) -> Result<Option<usize>, OperationError> {
        match self {
            &mut Value::Nil => Ok(None),
            &mut Value::List(ref mut value) => Ok(value.linsert(before, pivot, newvalue)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn llen(&self) -> Result<usize, OperationError> {
        return match self {
            &Value::Nil => Ok(0),
            &Value::List(ref value) => Ok(value.llen()),
            _ => Err(OperationError::WrongTypeError),
        };
    }

    pub fn lrange(&self, start: i64, stop: i64) -> Result<Vec<&Vec<u8>>, OperationError> {
        match self {
            &Value::Nil => Ok(Vec::new()),
            &Value::List(ref value) => Ok(value.lrange(start, stop)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn lrem(&mut self, left: bool, limit: usize, newvalue: Vec<u8>) -> Result<usize, OperationError> {
        match self {
            &mut Value::List(ref mut value) => Ok(value.lrem(left, limit, newvalue)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn lset(&mut self, index: i64, newvalue: Vec<u8>) -> Result<(), OperationError> {
        match self {
            &mut Value::Nil => Err(OperationError::UnknownKeyError),
            &mut Value::List(ref mut value) => value.lset(index, newvalue),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn ltrim(&mut self, start: i64, stop: i64) -> Result<(), OperationError> {
        match self {
            &mut Value::List(ref mut value) => value.ltrim(start, stop),
            _ => return Err(OperationError::WrongTypeError),
        }
    }

    pub fn sadd(&mut self, el: Vec<u8>) -> Result<bool, OperationError> {
        match self {
            &mut Value::Nil => {
                let mut value = ValueSet::new();
                value.sadd(el);
                *self = Value::Set(value);
                Ok(true)
            },
            &mut Value::Set(ref mut value) => Ok(value.sadd(el)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn srem(&mut self, el: &Vec<u8>) -> Result<bool, OperationError> {
        match self {
            &mut Value::Nil => Ok(false),
            &mut Value::Set(ref mut value) => Ok(value.srem(el)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn sismember(&self, el: &Vec<u8>) -> Result<bool, OperationError> {
        match self {
            &Value::Nil => Ok(false),
            &Value::Set(ref value) => Ok(value.sismember(el)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn scard(&self) -> Result<usize, OperationError> {
        match self {
            &Value::Nil => Ok(0),
            &Value::Set(ref value) => Ok(value.scard()),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn smembers(&self) -> Result<Vec<Vec<u8>>, OperationError> {
        match self {
            &Value::Nil => Ok(vec![]),
            &Value::Set(ref value) => Ok(value.smembers()),
            _ => Err(OperationError::WrongTypeError),
        }
    }


    pub fn srandmember(&self, count: usize, allow_duplicates: bool) -> Result<Vec<Vec<u8>>, OperationError> {
        match self {
            &Value::Nil => Ok(Vec::new()),
            &Value::Set(ref value) => Ok(value.srandmember(count, allow_duplicates)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn spop(&mut self, count: usize) -> Result<Vec<Vec<u8>>, OperationError> {
        match self {
            &mut Value::Nil => Ok(Vec::new()),
            &mut Value::Set(ref mut value) => Ok(value.spop(count)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    fn get_set_list<'a>(&'a self, set_values: &Vec<&'a Value>, default: &'a ValueSet) -> Result<Vec<&ValueSet>, OperationError> {
        let mut sets = Vec::with_capacity(set_values.len());
        for value in set_values {
            sets.push(match value {
                &&Value::Nil => default,
                &&Value::Set(ref value) => value,
                _ => return Err(OperationError::WrongTypeError),
            });
        }
        Ok(sets)
    }

    pub fn sdiff(&self, set_values: &Vec<&Value>) -> Result<HashSet<Vec<u8>>, OperationError> {
        let emptyset = ValueSet::new();
        let sets = try!(self.get_set_list(set_values, &emptyset));

        match self {
            &Value::Nil => Ok(HashSet::new()),
            &Value::Set(ref value) => Ok(value.sdiff(sets)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn sinter(&self, set_values: &Vec<&Value>) -> Result<HashSet<Vec<u8>>, OperationError> {
        let emptyset = ValueSet::new();
        let sets = try!(self.get_set_list(set_values, &emptyset));

        match self {
            &Value::Nil => Ok(HashSet::new()),
            &Value::Set(ref value) => Ok(value.sinter(sets)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn sunion(&self, set_values: &Vec<&Value>) -> Result<HashSet<Vec<u8>>, OperationError> {
        let emptyset = ValueSet::new();
        let sets = try!(self.get_set_list(set_values, &emptyset));

        match self {
            &Value::Nil => Ok(HashSet::new()),
            &Value::Set(ref value) => Ok(value.sunion(sets)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn create_set(&mut self, set: HashSet<Vec<u8>>) {
        *self = Value::Set(ValueSet::create_with_hashset(set));
    }

    pub fn zrem(&mut self, member: Vec<u8>) -> Result<bool, OperationError> {
        let (skiplist, hmap) = match self {
            &mut Value::Nil => return Ok(false),
            &mut Value::SortedSet(ref mut value) => match value {
                &mut ValueSortedSet::Data(ref mut skiplist, ref mut hmap) => (skiplist, hmap),
            },
            _ => return Err(OperationError::WrongTypeError),
        };
        let score = match hmap.remove(&member) {
            Some(val) => val,
            None => return Ok(false),
        };
        skiplist.remove(&SortedSetMember::new(score, member));
        Ok(true)
    }

    pub fn zadd(&mut self, s: f64, el: Vec<u8>, nx: bool, xx: bool, ch: bool, incr: bool) -> Result<bool, OperationError> {
        match self {
            &mut Value::Nil => {
                if xx {
                    return Ok(false);
                }
                let mut value = ValueSortedSet::new();
                let r = value.zadd(s, el, nx, xx, ch, incr);
                *self = Value::SortedSet(value);
                Ok(r)
            },
            &mut Value::SortedSet(ref mut value) => Ok(value.zadd(s, el, nx, xx, ch, incr)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn zincrby(&mut self, increment: f64, member: Vec<u8>) -> Result<f64, OperationError> {
        match self {
            &mut Value::Nil => {
                match self.zadd(increment.clone(), member, false, false, false, false) {
                    Ok(_) => Ok(increment),
                    Err(err) => Err(err),
                }
            },
            &mut Value::SortedSet(ref mut value) => Ok(value.zincrby(increment, member)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn zcount(&self, min: Bound<f64>, max: Bound<f64>) -> Result<usize, OperationError> {
        match self {
            &Value::Nil => Ok(0),
            &Value::SortedSet(ref value) => Ok(value.zcount(min, max)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn zrange(&self, start: i64, stop: i64, withscores: bool, rev: bool) -> Result<Vec<Vec<u8>>, OperationError> {
        match self {
            &Value::Nil => Ok(vec![]),
            &Value::SortedSet(ref value) => Ok(value.zrange(start, stop, withscores, rev)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn zrangebyscore(&self, min: Bound<f64>, max: Bound<f64>, withscores: bool, offset: usize, count: usize, rev: bool) -> Result<Vec<Vec<u8>>, OperationError> {
        match self {
            &Value::Nil => Ok(vec![]),
            &Value::SortedSet(ref value) => Ok(value.zrangebyscore(min, max, withscores, offset, count, rev)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn zrank(&self, el: Vec<u8>) -> Result<Option<usize>, OperationError> {
        match self {
            &Value::Nil => Ok(None),
            &Value::SortedSet(ref value) => Ok(value.zrank(el)),
            _ => Err(OperationError::WrongTypeError),
        }
    }
}

pub struct Database {
    data: Vec<RehashingHashMap<Vec<u8>, Value>>,
    data_expiration_ns: Vec<RehashingHashMap<Vec<u8>, i64>>,
    pub size: usize,
    subscribers: HashMap<Vec<u8>, HashMap<usize, Sender<PubsubEvent>>>,
    pattern_subscribers: HashMap<Vec<u8>, HashMap<usize, Sender<PubsubEvent>>>,
    key_subscribers: Vec<RehashingHashMap<Vec<u8>, HashMap<usize, Sender<bool>>>>,
    subscriber_id: usize,
    active_rehashing: bool,
}

fn create_database(size: usize, active_rehashing: bool) -> Database {
    let mut data = Vec::with_capacity(size);
    let mut data_expiration_ns = Vec::with_capacity(size);
    let mut key_subscribers = Vec::with_capacity(size);
    for _ in 0..size {
        data.push(RehashingHashMap::new());
        data_expiration_ns.push(RehashingHashMap::new());
        key_subscribers.push(RehashingHashMap::new());
    }
    return Database {
        data: data,
        data_expiration_ns: data_expiration_ns,
        size: size,
        subscribers: HashMap::new(),
        pattern_subscribers: HashMap::new(),
        key_subscribers: key_subscribers,
        subscriber_id: 0,
        active_rehashing: active_rehashing,
    };
}

impl Database {
    pub fn mock() -> Database {
        create_database(16, true)
    }

    pub fn new(config: &Config) -> Database {
        create_database(config.databases as usize, config.active_rehashing)
    }

    fn is_expired(&self, index: usize, key: &Vec<u8>) -> bool {
        match self.data_expiration_ns[index].get(key) {
            Some(t) => t <= &mstime(),
            None => false,
        }
    }

    pub fn get(&self, index: usize, key: &Vec<u8>) -> Option<&Value> {
        if self.is_expired(index, key) {
            None
        } else {
            self.data[index].get(key)
        }
    }

    pub fn get_mut(&mut self, index: usize, key: &Vec<u8>) -> Option<&mut Value> {
        if self.is_expired(index, key) {
            self.remove(index, key);
            None
        } else {
            self.data[index].get_mut(key)
        }
    }

    pub fn remove(&mut self, index: usize, key: &Vec<u8>) -> Option<Value> {
        let mut r = self.data[index].remove(key);
        if self.is_expired(index, key) {
            r = None;
        }
        self.data_expiration_ns[index].remove(key);
        if self.active_rehashing {
            if self.data[index].len() * 10 / 12 < self.data[index].capacity() {
                self.data[index].shrink_to_fit();
            }
            if self.data_expiration_ns[index].len() * 10 / 12 < self.data_expiration_ns[index].capacity() {
                self.data_expiration_ns[index].shrink_to_fit();
            }
            if self.key_subscribers[index].len() * 10 / 12 < self.key_subscribers[index].capacity() {
                self.key_subscribers[index].shrink_to_fit();
            }
        }
        r
    }

    pub fn set_msexpiration(&mut self, index: usize, key: Vec<u8>, msexpiration: i64) {
        self.data_expiration_ns[index].insert(key, msexpiration);
    }

    pub fn get_msexpiration(&mut self, index: usize, key: &Vec<u8>) -> Option<&i64> {
        self.data_expiration_ns[index].get(key)
    }

    pub fn remove_msexpiration(&mut self, index: usize, key: &Vec<u8>) -> Option<i64> {
        self.data_expiration_ns[index].remove(key)
    }

    pub fn clear(&mut self, index: usize) {
        self.data[index].clear()
    }

    pub fn get_or_create(&mut self, index: usize, key: &Vec<u8>) -> &mut Value {
        if self.get(index, key).is_some() {
            return self.get_mut(index, key).unwrap();
        }
        let val = Value::Nil;
        self.data[index].insert(Vec::clone(key), val);
        return self.data[index].get_mut(key).unwrap();
    }

    fn ensure_key_subscribers(&mut self, index: usize, key: &Vec<u8>) {
        if !self.key_subscribers[index].contains_key(key) {
            self.key_subscribers[index].insert(key.clone(), HashMap::new());
        }
    }

    pub fn key_subscribe(&mut self, index: usize, key: &Vec<u8>, sender: Sender<bool>) -> usize {
        self.ensure_key_subscribers(index, key);
        let mut key_subscribers = self.key_subscribers[index].get_mut(key).unwrap();
        let subscriber_id = self.subscriber_id;
        key_subscribers.insert(subscriber_id, sender);
        self.subscriber_id += 1;
        subscriber_id
    }

    pub fn key_publish(&mut self, index: usize, key: &Vec<u8>) {
        if self.active_rehashing {
            self.data[index].rehash();
            self.data_expiration_ns[index].rehash();
            self.key_subscribers[index].rehash();
        }

        let mut torem = Vec::new();
        match self.key_subscribers[index].get_mut(key) {
            Some(mut channels) => {
                for (subscriber_id, channel) in channels.iter() {
                    match channel.send(true) {
                        Ok(_) => (),
                        Err(_) => { torem.push(subscriber_id.clone()); () },
                    }
                }
                for subscriber_id in torem {
                    channels.remove(&subscriber_id);
                }
            }
            None => (),
        }
    }

    fn ensure_channel(&mut self, channel: &Vec<u8>) {
        if !self.subscribers.contains_key(channel) {
            self.subscribers.insert(channel.clone(), HashMap::new());
        }
    }

    pub fn subscribe(&mut self, channel: Vec<u8>, sender: Sender<PubsubEvent>) -> usize {
        self.ensure_channel(&channel);
        let mut channelsubscribers = self.subscribers.get_mut(&channel).unwrap();
        let subscriber_id = self.subscriber_id;
        channelsubscribers.insert(subscriber_id, sender);
        self.subscriber_id += 1;
        subscriber_id
    }

    pub fn unsubscribe(&mut self, channel: Vec<u8>, subscriber_id: usize) -> bool {
        if !self.subscribers.contains_key(&channel) {
            return false;
        }
        let mut channelsubscribers = self.subscribers.get_mut(&channel).unwrap();
        channelsubscribers.remove(&subscriber_id).is_some()
    }

    fn pensure_channel(&mut self, pattern: &Vec<u8>) {
        if !self.pattern_subscribers.contains_key(pattern) {
            self.pattern_subscribers.insert(pattern.clone(), HashMap::new());
        }
    }

    pub fn psubscribe(&mut self, pattern: Vec<u8>, sender: Sender<PubsubEvent>) -> usize {
        self.pensure_channel(&pattern);
        let mut channelsubscribers = self.pattern_subscribers.get_mut(&pattern).unwrap();
        let subscriber_id = self.subscriber_id;
        channelsubscribers.insert(subscriber_id, sender);
        self.subscriber_id += 1;
        subscriber_id
    }

    pub fn punsubscribe(&mut self, pattern: Vec<u8>, subscriber_id: usize) -> bool {
        if !self.pattern_subscribers.contains_key(&pattern) {
            return false;
        }
        let mut channelsubscribers = self.pattern_subscribers.get_mut(&pattern).unwrap();
        channelsubscribers.remove(&subscriber_id).is_some()
    }

    pub fn publish(&self, channel_name: &Vec<u8>, message: &Vec<u8>) -> usize {
        let mut c = 0;
        match self.subscribers.get(channel_name) {
            Some(channels) => {
                for (_, channel) in channels {
                    match channel.send(PubsubEvent::Message(channel_name.clone(), None, message.clone())) {
                        Ok(_) => c += 1,
                        Err(_) => (),
                    }
                }
            }
            None => (),
        }
        for (pattern, channels) in self.pattern_subscribers.iter() {
            if glob_match(&pattern, &channel_name, false) {
                for (_, channel) in channels {
                    match channel.send(PubsubEvent::Message(channel_name.clone(), Some(pattern.clone()), message.clone())) {
                        Ok(_) => c += 1,
                        Err(_) => (),
                    }
                }
            }
        }
        c
    }

    pub fn clearall(&mut self) {
        for index in 0..self.size {
            self.data[index].clear();
        }
    }
}

#[test]
fn lpush() {
    let v1 = vec![1u8, 2, 3, 4];
    let v2 = vec![1u8, 5, 6, 7];
    let mut value = Value::List(ValueList::new());

    value.push(v1.clone(), false).unwrap();
    {
        let list = match value { Value::List(ref value) => match value { &ValueList::Data(ref l) => l }, _ => panic!("Expected list") };
        assert_eq!(list.len(), 1);
        assert_eq!(list.front(), Some(&v1));
    }

    value.push(v2.clone(), false).unwrap();
    {
        let list = match value { Value::List(ref value) => match value { &ValueList::Data(ref l) => l }, _ => panic!("Expected list") };
        assert_eq!(list.len(), 2);
        assert_eq!(list.back(), Some(&v1));
        assert_eq!(list.front(), Some(&v2));
    }
}

#[test]
fn lpop() {
    let mut value = Value::List(ValueList::new());
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    value.push(v1.clone(), false).unwrap();
    value.push(v2.clone(), false).unwrap();
    assert_eq!(value.pop(false).unwrap(), Some(v2));
    assert_eq!(value.pop(false).unwrap(), Some(v1));
    assert_eq!(value.pop(false).unwrap(), None);
}

#[test]
fn lindex() {
    let mut value = Value::List(ValueList::new());
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    value.push(v1.clone(), false).unwrap();
    value.push(v2.clone(), false).unwrap();

    assert_eq!(value.lindex(0).unwrap(), Some(&v2));
    assert_eq!(value.lindex(1).unwrap(), Some(&v1));
    assert_eq!(value.lindex(2).unwrap(), None);

    assert_eq!(value.lindex(-2).unwrap(), Some(&v2));
    assert_eq!(value.lindex(-1).unwrap(), Some(&v1));
    assert_eq!(value.lindex(-3).unwrap(), None);
}

#[test]
fn linsert() {
    let mut value = Value::List(ValueList::new());
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 0, 1, 2];
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();

    assert_eq!(value.linsert(true, v2.clone(), v3.clone()).unwrap().unwrap(), 3);
    assert_eq!(value.lindex(0).unwrap(), Some(&v1));
    assert_eq!(value.lindex(1).unwrap(), Some(&v3));
    assert_eq!(value.lindex(2).unwrap(), Some(&v2));

    assert_eq!(value.linsert(true, vec![], v3.clone()).unwrap(), None);
}

#[test]
fn llen() {
    let mut value = Value::List(ValueList::new());
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 0, 1, 2];
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();
    assert_eq!(value.llen().unwrap(), 2);

    value.push(v3.clone(), true).unwrap();
    assert_eq!(value.llen().unwrap(), 3);
}

#[test]
fn lrange() {
    let mut value = Value::List(ValueList::new());
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 0, 1, 2];
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();
    value.push(v3.clone(), true).unwrap();

    assert_eq!(value.lrange(-100, 100).unwrap(), vec![&v1, &v2, &v3]);
    assert_eq!(value.lrange(0, 1).unwrap(), vec![&v1, &v2]);
    assert_eq!(value.lrange(0, 0).unwrap(), vec![&v1]);
    assert_eq!(value.lrange(1, -1).unwrap(), vec![&v2, &v3]);
}

#[test]
fn lrem_left_unlimited() {
    let mut value = Value::List(ValueList::new());
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 0, 1, 2];
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();
    value.push(v3.clone(), true).unwrap();

    assert_eq!(value.lrem(true, 0, v3.clone()).unwrap(), 1);
    assert_eq!(value.llen().unwrap(), 2);
    assert_eq!(value.lrem(true, 0, v1.clone()).unwrap(), 1);
    assert_eq!(value.llen().unwrap(), 1);
    assert_eq!(value.lrem(true, 0, v2.clone()).unwrap(), 1);
    assert_eq!(value.llen().unwrap(), 0);
}

#[test]
fn lrem_left_limited() {
    let mut value = Value::List(ValueList::new());
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    value.push(v1.clone(), true).unwrap();
    value.push(v1.clone(), true).unwrap();
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();
    value.push(v1.clone(), true).unwrap();

    assert_eq!(value.lrem(true, 3, v1.clone()).unwrap(), 3);
    assert_eq!(value.llen().unwrap(), 2);
    {
        let list = match value { Value::List(ref value) => match value { &ValueList::Data(ref l) => l }, _ => panic!("Expected list") };
        assert_eq!(list.front().unwrap(), &v2);
    }
    assert_eq!(value.lrem(true, 3, v1.clone()).unwrap(), 1);
    assert_eq!(value.llen().unwrap(), 1);
}

#[test]
fn lrem_right_unlimited() {
    let mut value = Value::List(ValueList::new());
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 0, 1, 2];
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();
    value.push(v3.clone(), true).unwrap();

    assert_eq!(value.lrem(false, 0, v3.clone()).unwrap(), 1);
    assert_eq!(value.llen().unwrap(), 2);
    assert_eq!(value.lrem(false, 0, v1.clone()).unwrap(), 1);
    assert_eq!(value.llen().unwrap(), 1);
    assert_eq!(value.lrem(false, 0, v2.clone()).unwrap(), 1);
    assert_eq!(value.llen().unwrap(), 0);
}

#[test]
fn lrem_right_limited() {
    let mut value = Value::List(ValueList::new());
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    value.push(v1.clone(), true).unwrap();
    value.push(v1.clone(), true).unwrap();
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();
    value.push(v1.clone(), true).unwrap();

    assert_eq!(value.lrem(false, 3, v1.clone()).unwrap(), 3);
    assert_eq!(value.llen().unwrap(), 2);
    {
        let list = match value { Value::List(ref value) => match value { &ValueList::Data(ref l) => l }, _ => panic!("Expected list") };
        assert_eq!(list.front().unwrap(), &v1);
    }
    assert_eq!(value.lrem(false, 3, v1.clone()).unwrap(), 1);
    assert_eq!(value.llen().unwrap(), 1);
}

#[test]
fn lset() {
    let mut value = Value::List(ValueList::new());
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 0, 1, 2];
    let v4 = vec![3, 4, 5, 6];
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();
    value.push(v3.clone(), true).unwrap();

    assert_eq!(value.lset(1, v4.clone()).unwrap(), ());
    assert_eq!(value.lrange(0, -1).unwrap(), vec![&v1, &v4, &v3]);
    assert_eq!(value.lset(-1, v2.clone()).unwrap(), ());
    assert_eq!(value.lrange(0, -1).unwrap(), vec![&v1, &v4, &v2]);
}

#[test]
fn ltrim() {
    let mut value = Value::List(ValueList::new());
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 0, 1, 2];
    let v4 = vec![3, 4, 5, 6];
    value.push(v1.clone(), true).unwrap();
    value.push(v2.clone(), true).unwrap();
    value.push(v3.clone(), true).unwrap();
    value.push(v4.clone(), true).unwrap();

    assert_eq!(value.ltrim(1, 2).unwrap(), ());
    assert_eq!(value.llen().unwrap(), 2);
    assert_eq!(value.lrange(0, -1).unwrap(), vec![&v2, &v3]);
}

#[test]
fn sadd() {
    let mut value = Value::Nil;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(value.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value.sadd(v1.clone()).unwrap(), false);
}

#[test]
fn srem() {
    let mut value = Value::Nil;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(value.srem(&v1).unwrap(), false);
    assert_eq!(value.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value.srem(&v1).unwrap(), true);
    assert_eq!(value.srem(&v1).unwrap(), false);
}

#[test]
fn sismember() {
    let mut value = Value::Nil;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(value.sismember(&v1).unwrap(), false);
    assert_eq!(value.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value.sismember(&v1).unwrap(), true);
}

#[test]
fn scard() {
    let mut value = Value::Nil;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(value.scard().unwrap(), 0);
    assert_eq!(value.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value.scard().unwrap(), 1);
}

#[test]
fn srandmember_toomany_nodup() {
    let mut value = Value::Nil;
    let v1 = vec![1];
    let v2 = vec![2];
    let v3 = vec![3];

    value.sadd(v1.clone()).unwrap();
    value.sadd(v2.clone()).unwrap();
    value.sadd(v3.clone()).unwrap();

    let mut v = value.srandmember(10, false).unwrap();
    v.sort_by(|a, b| a.cmp(b));
    assert_eq!(v, [v1, v2, v3]);
}

#[test]
fn srandmember_alot_dup() {
    let mut value = Value::Nil;
    let v1 = vec![1];
    let v2 = vec![2];
    let v3 = vec![3];

    value.sadd(v1.clone()).unwrap();
    value.sadd(v2.clone()).unwrap();
    value.sadd(v3.clone()).unwrap();

    let v = value.srandmember(100, true).unwrap();
    assert_eq!(v.len(), 100);
    // this test _probably_ passes
    assert!(v.contains(&v1));
    assert!(v.contains(&v2));
    assert!(v.contains(&v3));
}

#[test]
fn srandmember_nodup_all() {
    let mut value = Value::Nil;
    let v1 = vec![1];
    let v2 = vec![2];
    value.sadd(v1.clone()).unwrap();
    value.sadd(v2.clone()).unwrap();

    let mut v = value.srandmember(2, false).unwrap();
    v.sort_by(|a, b| a.cmp(b));
    assert!(v == vec![v1.clone(), v1.clone()] ||
            v == vec![v1.clone(), v2.clone()] ||
            v == vec![v2.clone(), v2.clone()]);
}

#[test]
fn srandmember_nodup_some() {
    let mut value = Value::Nil;
    let v1 = vec![1];
    let v2 = vec![2];
    value.sadd(v1.clone()).unwrap();
    value.sadd(v2.clone()).unwrap();

    let mut v = value.srandmember(1, false).unwrap();
    v.sort_by(|a, b| a.cmp(b));
    assert!(v == vec![v1.clone()] ||
            v == vec![v2.clone()]);
}

#[test]
fn srandmember_dup() {
    let mut value = Value::Nil;
    let v1 = vec![1];
    value.sadd(v1.clone()).unwrap();

    let v = value.srandmember(5, true).unwrap();
    assert_eq!(v, vec![v1.clone(), v1.clone(), v1.clone(), v1.clone(), v1.clone()]);
}

#[test]
fn spop_toomany() {
    let mut value = Value::Nil;
    let v1 = vec![1];
    let v2 = vec![2];
    let v3 = vec![3];

    value.sadd(v1.clone()).unwrap();
    value.sadd(v2.clone()).unwrap();
    value.sadd(v3.clone()).unwrap();

    let mut v = value.spop(10).unwrap();
    v.sort_by(|a, b| a.cmp(b));
    assert_eq!(v, [v1, v2, v3]);
}

#[test]
fn spop_some() {
    let mut value = Value::Nil;
    let v1 = vec![1];
    let v2 = vec![2];
    let v3 = vec![3];

    value.sadd(v1.clone()).unwrap();
    value.sadd(v2.clone()).unwrap();
    value.sadd(v3.clone()).unwrap();

    let v = value.spop(1).unwrap();
    assert!(v == [v1] || v == [v2] || v == [v3]);
}

#[test]
fn smembers() {
    let mut value = Value::Nil;
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 10, 11, 12];

    assert_eq!(value.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value.sadd(v2.clone()).unwrap(), true);
    assert_eq!(value.sadd(v3.clone()).unwrap(), true);

    let mut v = value.smembers().unwrap();
    v.sort_by(|a, b| a.cmp(b));
    assert_eq!(v, [v1, v2, v3]);
}

#[test]
fn sdiff() {
    let mut value1 = Value::Nil;
    let mut value2 = Value::Nil;
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![0, 9, 1, 2];

    assert_eq!(value1.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value1.sadd(v2.clone()).unwrap(), true);

    assert_eq!(value2.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value2.sadd(v3.clone()).unwrap(), true);

    assert_eq!(value1.sdiff(&vec![&value2]).unwrap(),
            vec![v2].iter().cloned().collect::<HashSet<_>>());
}

#[test]
fn sinter() {
    let mut value1 = Value::Nil;
    let mut value2 = Value::Nil;
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![0, 9, 1, 2];

    assert_eq!(value1.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value1.sadd(v2.clone()).unwrap(), true);

    assert_eq!(value2.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value2.sadd(v3.clone()).unwrap(), true);

    assert_eq!(value1.sinter(&vec![&value2]).unwrap().iter().collect::<Vec<_>>(),
            vec![&v1]);

    let empty:Vec<&Value> = Vec::new();
    assert_eq!(value1.sinter(&empty).unwrap(),
            vec![v1, v2].iter().cloned().collect::<HashSet<_>>());

    assert_eq!(value1.sinter(&vec![&value2, &Value::Nil]).unwrap().len(), 0);
}

#[test]
fn sunion() {
    let mut value1 = Value::Nil;
    let mut value2 = Value::Nil;
    let v1 = vec![1, 2, 3, 4];
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![0, 9, 1, 2];

    assert_eq!(value1.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value1.sadd(v2.clone()).unwrap(), true);

    assert_eq!(value2.sadd(v1.clone()).unwrap(), true);
    assert_eq!(value2.sadd(v3.clone()).unwrap(), true);

    assert_eq!(value1.sunion(&vec![&value2]).unwrap(),
            vec![&v1, &v2, &v3].iter().cloned().cloned().collect::<HashSet<_>>());

    let empty:Vec<&Value> = Vec::new();
    assert_eq!(value1.sunion(&empty).unwrap(),
            vec![&v1, &v2].iter().cloned().cloned().collect::<HashSet<_>>());

    assert_eq!(value1.sunion(&vec![&value2, &Value::Nil]).unwrap(),
            vec![&v1, &v2, &v3].iter().cloned().cloned().collect::<HashSet<_>>());
}

#[test]
fn set_get() {
    let s = vec![1u8, 2, 3, 4];
    let mut value = Value::Nil;
    value.set(s.clone()).unwrap();
    assert_eq!(value, Value::String(ValueString::Data(s)));
}

#[test]
fn get_empty() {
    let database = Database::mock();
    let key = vec![1u8];
    assert!(database.get(0, &key).is_none());
}

#[test]
fn set_set_get() {
    let s = vec![1, 2, 3, 4];
    let mut value = Value::String(ValueString::Data(vec![0, 0, 0]));
    value.set(s.clone()).unwrap();
    assert_eq!(value, Value::String(ValueString::Data(s)));
}

#[test]
fn append_append_get() {
    let mut value = Value::Nil;
    assert_eq!(value.append(vec![0, 0, 0]).unwrap(), 3);
    assert_eq!(value.append(vec![1, 2, 3, 4]).unwrap(), 7);
    assert_eq!(value, Value::String(ValueString::Data(vec![0u8, 0, 0, 1, 2, 3, 4])));
}

#[test]
fn set_number() {
    let mut value = Value::Nil;
    value.set(b"123".to_vec()).unwrap();
    assert_eq!(value, Value::String(ValueString::Integer(123)));
}

#[test]
fn append_number() {
    let mut value = Value::String(ValueString::Integer(123));
    assert_eq!(value.append(b"asd".to_vec()).unwrap(), 6);
    assert_eq!(value, Value::String(ValueString::Data(b"123asd".to_vec())));
}

#[test]
fn remove_value() {
    let mut database = Database::mock();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];
    assert!(database.get_or_create(0, &key).set(value).is_ok());
    database.remove(0, &key).unwrap();
    assert!(database.remove(0, &key).is_none());
}

#[test]
fn incr_str() {
    let mut value = Value::String(ValueString::Integer(123));
    assert_eq!(value.incr(1).unwrap(), 124);
    assert_eq!(value, Value::String(ValueString::Integer(124)));
}

#[test]
fn incr2_str() {
    let mut value = Value::String(ValueString::Data(b"123".to_vec()));
    assert_eq!(value.incr(1).unwrap(), 124);
    assert_eq!(value, Value::String(ValueString::Integer(124)));
}

#[test]
fn incr_create_update() {
    let mut value = Value::Nil;
    assert_eq!(value.incr(124).unwrap(), 124);
    assert_eq!(value, Value::String(ValueString::Integer(124)));
    assert_eq!(value.incr(1).unwrap(), 125);
    assert_eq!(value, Value::String(ValueString::Integer(125)));
}

#[test]
fn incr_overflow() {
    let mut value = Value::String(ValueString::Integer(std::i64::MAX));
    assert!(value.incr(1).is_err());
}

#[test]
fn set_expire_get() {
    let mut database = Database::mock();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];
    assert!(database.get_or_create(0, &key).set(value).is_ok());
    database.set_msexpiration(0, key.clone(), mstime());
    assert_eq!(database.get(0, &key), None);
}

#[test]
fn set_will_expire_get() {
    let mut database = Database::mock();
    let key = vec![1u8];
    let value = vec![1u8, 2, 3, 4];
    let expected = Vec::clone(&value);
    assert!(database.get_or_create(0, &key).set(value).is_ok());
    database.set_msexpiration(0, key.clone(), mstime() + 10000);
    assert_eq!(database.get(0, &key), Some(&Value::String(ValueString::Data(expected))));
}

#[test]
fn getrange_integer() {
    let value = Value::String(ValueString::Integer(123));
    assert_eq!(value.getrange(0, -1).unwrap(), "123".to_owned().into_bytes());
    assert_eq!(value.getrange(-100, -2).unwrap(), "12".to_owned().into_bytes());
    assert_eq!(value.getrange(1, 1).unwrap(), "2".to_owned().into_bytes());
}

#[test]
fn getrange_data() {
    let value = Value::String(ValueString::Data(vec![1,2,3]));
    assert_eq!(value.getrange(0, -1).unwrap(), vec![1,2,3]);
    assert_eq!(value.getrange(-100, -2).unwrap(), vec![1,2]);
    assert_eq!(value.getrange(1, 1).unwrap(), vec![2]);
}

#[test]
fn setrange_append() {
    let mut value = Value::String(ValueString::Data(vec![1,2,3]));
    assert_eq!(value.setrange(3, vec![4, 5, 6]).unwrap(), 6);
    assert_eq!(value, Value::String(ValueString::Data(vec![1,2,3,4,5,6])));
}

#[test]
fn setrange_create() {
    let mut value = Value::Nil;
    assert_eq!(value.setrange(0, vec![4, 5, 6]).unwrap(), 3);
    assert_eq!(value, Value::String(ValueString::Data(vec![4,5,6])));
}

#[test]
fn setrange_padding() {
    let mut value = Value::String(ValueString::Data(vec![1,2,3]));
    assert_eq!(value.setrange(5, vec![6]).unwrap(), 6);
    assert_eq!(value, Value::String(ValueString::Data(vec![1,2,3,0,0,6])));
}

#[test]
fn setrange_intermediate() {
    let mut value = Value::String(ValueString::Data(vec![1,2,3,4,5]));
    assert_eq!(value.setrange(2, vec![13, 14]).unwrap(), 5);
    assert_eq!(value, Value::String(ValueString::Data(vec![1,2,13,14,5])));
}

#[test]
fn setbit() {
    let mut value = Value::Nil;
    assert_eq!(value.setbit(23, false).unwrap(), false);
    assert_eq!(value.getrange(0, -1).unwrap(), [0u8, 0, 0]);
    assert_eq!(value.setbit(23, true).unwrap(), false);
    assert_eq!(value.getrange(0, -1).unwrap(), [0u8, 0, 1]);
}

#[test]
fn getbit() {
    let value = Value::String(ValueString::Data(vec![1,2,3,4,5]));
    assert_eq!(value.getbit(0).unwrap(), false);
    assert_eq!(value.getbit(23).unwrap(), true);
    assert_eq!(value.getbit(500).unwrap(), false);
}

macro_rules! zadd {
    ($value: expr, $score: expr, $member: expr) => (
        $value.zadd($score, $member.clone(), false, false, false, false).unwrap()
    )
}

#[test]
fn zadd_basic() {
    let mut value = Value::Nil;
    let s1 = 1.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 1.0;
    let v2 = vec![5, 6, 7, 8];

    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(zadd!(value, s1, v1), false);
    assert_eq!(zadd!(value, s2, v2), true);
    assert_eq!(zadd!(value, s1, v2), false);
    match value {
        Value::SortedSet(value) => match value {
            ValueSortedSet::Data(_, hs) => {
                assert_eq!(hs.get(&v1).unwrap(), &s1);
                assert_eq!(hs.get(&v2).unwrap(), &s1);
            },
        },
        _ => panic!("Expected zset"),
    }
}

#[test]
fn zadd_nx() {
    let mut value = Value::Nil;
    let s1 = 1.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 1.0;
    let v2 = vec![5, 6, 7, 8];

    assert_eq!(value.zadd(s1, v1.clone(), true, false, false, false).unwrap(), true);
    assert_eq!(value.zadd(s1, v1.clone(), true, false, false, false).unwrap(), false);
    assert_eq!(value.zadd(s2, v2.clone(), true, false, false, false).unwrap(), true);
    assert_eq!(value.zadd(s1, v2.clone(), true, false, false, false).unwrap(), false);
    match value {
        Value::SortedSet(value) => match value {
            ValueSortedSet::Data(_, hs) => {
                assert_eq!(hs.get(&v1).unwrap(), &s1);
                assert_eq!(hs.get(&v2).unwrap(), &s2);
            },
        },
        _ => panic!("Expected zset"),
    }
}

#[test]
fn zadd_xx() {
    let mut value = Value::Nil;
    let s1 = 1.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 2.0;

    assert_eq!(value.zadd(s1, v1.clone(), false, true, false, false).unwrap(), false);
    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(value.zadd(s2, v1.clone(), false, true, false, false).unwrap(), false);
    match value {
        Value::SortedSet(value) => match value {
            ValueSortedSet::Data(_, hs) => {
                assert_eq!(hs.get(&v1).unwrap(), &s2);
            },
        },
        _ => panic!("Expected zset"),
    }
}

#[test]
fn zadd_ch() {
    let mut value = Value::Nil;
    let s1 = 1.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 2.0;

    assert_eq!(value.zadd(s1, v1.clone(), false, false, true, false).unwrap(), true);
    assert_eq!(zadd!(value, s1, v1), false);
    assert_eq!(value.zadd(s2, v1.clone(), false, false, true, false).unwrap(), true);
    match value {
        Value::SortedSet(value) => match value {
            ValueSortedSet::Data(_, hs) => {
                assert_eq!(hs.get(&v1).unwrap(), &s2);
            },
        },
        _ => panic!("Expected zset"),
    }
}

#[test]
fn zcount() {
    let mut value = Value::Nil;
    let s1 = 1.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 2.0;
    let v2 = vec![5, 6, 7, 8];

    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(zadd!(value, s2, v2), true);
    assert_eq!(value.zcount(Bound::Included(0.0), Bound::Included(5.0)).unwrap(), 2);
    assert_eq!(value.zcount(Bound::Included(1.0), Bound::Included(2.0)).unwrap(), 2);
    assert_eq!(value.zcount(Bound::Excluded(1.0), Bound::Excluded(2.0)).unwrap(), 0);
    assert_eq!(value.zcount(Bound::Included(1.5), Bound::Included(2.0)).unwrap(), 1);
    assert_eq!(value.zcount(Bound::Included(5.0), Bound::Included(10.0)).unwrap(), 0);
}

#[test]
fn zrange() {
    let mut value = Value::Nil;
    let s1 = 0.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 0.0;
    let v2 = vec![5, 6, 7, 8];
    let s3 = 0.0;
    let v3 = vec![9, 10, 11, 12];

    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(zadd!(value, s3, v3), true);
    assert_eq!(zadd!(value, s2, v2), true);
    assert_eq!(value.zrange(0, -1, true, false).unwrap(), vec![
            vec![1, 2, 3, 4], b"0".to_vec(),
            vec![5, 6, 7, 8], b"0".to_vec(),
            vec![9, 10, 11, 12], b"0".to_vec(),
            ]);
    assert_eq!(value.zrange(1, 1, true, false).unwrap(), vec![
            vec![5, 6, 7, 8], b"0".to_vec(),
            ]);
    assert_eq!(value.zrange(2, 0, true, false).unwrap().len(), 0);
}

#[test]
fn zrevrange() {
    let mut value = Value::Nil;
    let s1 = 0.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 0.0;
    let v2 = vec![5, 6, 7, 8];
    let s3 = 0.0;
    let v3 = vec![9, 10, 11, 12];

    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(zadd!(value, s3, v3), true);
    assert_eq!(zadd!(value, s2, v2), true);
    assert_eq!(value.zrange(0, -1, true, true).unwrap(), vec![
            v3.clone(), b"0".to_vec(),
            v2.clone(), b"0".to_vec(),
            v1.clone(), b"0".to_vec(),
            ]);
    assert_eq!(value.zrange(1, 1, true, true).unwrap(), vec![
            v2.clone(), b"0".to_vec(),
            ]);
    assert_eq!(value.zrange(2, 0, true, true).unwrap().len(), 0);
    assert_eq!(value.zrange(2, 2, true, true).unwrap(), vec![
            v1.clone(), b"0".to_vec(),
            ]);
}

#[test]
fn zrangebyscore() {
    let mut value = Value::Nil;
    let s1 = 10.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 20.0;
    let v2 = vec![5, 6, 7, 8];
    let s3 = 30.0;
    let v3 = vec![9, 10, 11, 12];

    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(zadd!(value, s3, v3), true);
    assert_eq!(zadd!(value, s2, v2), true);
    assert_eq!(value.zrangebyscore(Bound::Unbounded, Bound::Unbounded, true, 0, usize::MAX, false).unwrap(), vec![
            vec![1, 2, 3, 4], b"10".to_vec(),
            vec![5, 6, 7, 8], b"20".to_vec(),
            vec![9, 10, 11, 12], b"30".to_vec(),
            ]);
    assert_eq!(value.zrangebyscore(Bound::Excluded(10.0), Bound::Included(20.0), true, 0, usize::MAX, false).unwrap(), vec![
            vec![5, 6, 7, 8], b"20".to_vec(),
            ]);
    assert_eq!(value.zrangebyscore(Bound::Included(20.0), Bound::Excluded(30.0), true, 0, usize::MAX, false).unwrap(), vec![
            vec![5, 6, 7, 8], b"20".to_vec(),
            ]);
    assert_eq!(value.zrangebyscore(Bound::Unbounded, Bound::Unbounded, true, 1, 1, false).unwrap(), vec![
            vec![5, 6, 7, 8], b"20".to_vec(),
            ]);
    assert_eq!(value.zrangebyscore(Bound::Excluded(30.0), Bound::Included(20.0), false, 0, usize::MAX, false).unwrap().len(), 0);
    assert_eq!(value.zrangebyscore(Bound::Excluded(30.0), Bound::Excluded(30.0), false, 0, usize::MAX, false).unwrap().len(), 0);
    assert_eq!(value.zrangebyscore(Bound::Included(30.0), Bound::Included(30.0), false, 0, usize::MAX, false).unwrap().len(), 1);
    assert_eq!(value.zrangebyscore(Bound::Included(30.0), Bound::Excluded(30.0), false, 0, usize::MAX, false).unwrap().len(), 0);
    assert_eq!(value.zrangebyscore(Bound::Included(21.0), Bound::Included(22.0), false, 0, usize::MAX, false).unwrap().len(), 0);
}

#[test]
fn zrevrangebyscore() {
    let mut value = Value::Nil;
    let s1 = 10.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 20.0;
    let v2 = vec![5, 6, 7, 8];
    let s3 = 30.0;
    let v3 = vec![9, 10, 11, 12];

    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(zadd!(value, s3, v3), true);
    assert_eq!(zadd!(value, s2, v2), true);
    assert_eq!(value.zrangebyscore(Bound::Unbounded, Bound::Unbounded, true, 0, usize::MAX, true).unwrap(), vec![
            vec![9, 10, 11, 12], b"30".to_vec(),
            vec![5, 6, 7, 8], b"20".to_vec(),
            vec![1, 2, 3, 4], b"10".to_vec(),
            ]);
    assert_eq!(value.zrangebyscore(Bound::Included(20.0), Bound::Excluded(10.0), true, 0, usize::MAX, true).unwrap(), vec![
            vec![5, 6, 7, 8], b"20".to_vec(),
            ]);
    assert_eq!(value.zrangebyscore(Bound::Excluded(30.0), Bound::Included(20.0), true, 0, usize::MAX, true).unwrap(), vec![
            vec![5, 6, 7, 8], b"20".to_vec(),
            ]);
    assert_eq!(value.zrangebyscore(Bound::Unbounded, Bound::Unbounded, true, 1, 1, true).unwrap(), vec![
            vec![5, 6, 7, 8], b"20".to_vec(),
            ]);
    assert_eq!(value.zrangebyscore(Bound::Included(20.0), Bound::Excluded(30.0), false, 0, usize::MAX, true).unwrap().len(), 0);
    assert_eq!(value.zrangebyscore(Bound::Excluded(30.0), Bound::Excluded(30.0), false, 0, usize::MAX, true).unwrap().len(), 0);
    assert_eq!(value.zrangebyscore(Bound::Included(30.0), Bound::Included(30.0), false, 0, usize::MAX, true).unwrap().len(), 1);
    assert_eq!(value.zrangebyscore(Bound::Excluded(30.0), Bound::Included(30.0), false, 0, usize::MAX, true).unwrap().len(), 0);
    assert_eq!(value.zrangebyscore(Bound::Included(22.0), Bound::Included(21.0), false, 0, usize::MAX, true).unwrap().len(), 0);
}

#[test]
fn zrank() {
    let mut value = Value::Nil;
    let s1 = 0.0;
    let v1 = vec![1, 2, 3, 4];
    let s2 = 0.0;
    let v2 = vec![5, 6, 7, 8];
    let v3 = vec![9, 10, 11, 12];

    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(zadd!(value, s2, v2), true);
    assert_eq!(value.zrank(v1.clone()).unwrap(), Some(0));
    assert_eq!(value.zrank(v2.clone()).unwrap(), Some(1));
    assert_eq!(value.zrank(v3.clone()).unwrap(), None);
}

#[test]
fn zadd_update() {
    let mut value = Value::Nil;
    let s1 = 0.0;
    let s2 = 1.0;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(zadd!(value, s2, v1), false);
    assert_eq!(value.zrange(0, -1, true, false).unwrap(), vec![
            vec![1, 2, 3, 4], b"1".to_vec(),
            ]);
}

#[test]
fn zadd_incr() {
    let mut value = Value::Nil;
    let s1 = 1.0;
    let incr = 2.0;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(value.zadd(s1, v1.clone(), false, false, false, true).unwrap(), true);
    assert_eq!(value.zrange(0, -1, true, false).unwrap(), vec![v1.clone(), b"1".to_vec()]);
    assert_eq!(value.zadd(incr, v1.clone(), false, false, false, true).unwrap(), false);
    assert_eq!(value.zrange(0, -1, true, false).unwrap(), vec![v1.clone(), b"3".to_vec()]);
}

#[test]
fn zadd_incr_ch() {
    let mut value = Value::Nil;
    let s1 = 1.0;
    let incr = 2.0;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(value.zadd(s1, v1.clone(), false, false, true, true).unwrap(), true);
    assert_eq!(value.zrange(0, -1, true, false).unwrap(), vec![v1.clone(), b"1".to_vec()]);
    assert_eq!(value.zadd(incr, v1.clone(), false, false, true, true).unwrap(), true);
    assert_eq!(value.zrange(0, -1, true, false).unwrap(), vec![v1.clone(), b"3".to_vec()]);
}

#[test]
fn zrem() {
    let mut value = Value::Nil;
    let s1 = 0.0;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(zadd!(value, s1, v1), true);
    assert_eq!(value.zrem(vec![8u8]).unwrap(), false);
    assert_eq!(value.zrem(v1.clone()).unwrap(), true);
    assert_eq!(value.zrem(v1.clone()).unwrap(), false);
}

#[test]
fn zincrby() {
    let mut value = Value::Nil;
    let s1 = 1.0;
    let s2 = 2.0;
    let v1 = vec![1, 2, 3, 4];

    assert_eq!(value.zincrby(s1.clone(), v1.clone()).unwrap(), s1);
    assert_eq!(value.zincrby(s2.clone(), v1.clone()).unwrap(), s1 + s2);
    assert_eq!(value.zincrby(- s1.clone(), v1.clone()).unwrap(), s2);
}

#[test]
fn pubsub_basic() {
    let mut database = Database::mock();
    let channel_name = vec![1u8, 2, 3];
    let message = vec![2u8, 3, 4, 5, 6];
    let (tx, rx) = channel();
    database.subscribe(channel_name.clone(), tx);
    database.publish(&channel_name, &message);
    assert_eq!(rx.recv().unwrap(), PubsubEvent::Message(channel_name, None, message));
}

#[test]
fn unsubscribe() {
    let mut database = Database::mock();
    let channel_name = vec![1u8, 2, 3];
    let message = vec![2u8, 3, 4, 5, 6];
    let (tx, rx) = channel();
    let subscriber_id = database.subscribe(channel_name.clone(), tx);
    database.unsubscribe(channel_name.clone(), subscriber_id);
    database.publish(&channel_name, &message);
    assert!(rx.try_recv().is_err());
}

#[test]
fn pubsub_pattern() {
    let mut database = Database::mock();
    let channel_name = vec![1u8, 2, 3];
    let message = vec![2u8, 3, 4, 5, 6];
    let (tx, rx) = channel();
    database.psubscribe(channel_name.clone(), tx);
    database.publish(&channel_name, &message);
    assert_eq!(rx.recv().unwrap(), PubsubEvent::Message(channel_name.clone(), Some(channel_name.clone()), message));
}

#[test]
fn rehashing() {
    let mut database = Database::mock();
    for i in 0u32..1000 {
        let key = vec![(i % 256) as u8, (i / 256) as u8];
        database.get_or_create(0, &key).set(key.clone()).unwrap();
    }
    assert_eq!(database.data[0].len(), 1000);
    assert!(database.data[0].capacity() >= 1000);
    for i in 0u32..1000 {
        let key = vec![(i % 256) as u8, (i / 256) as u8];
        database.remove(0, &key).unwrap();
    }
    // freeing memory
    assert!(database.data[0].capacity() < 1000);
}

#[test]
fn no_rehashing() {
    let mut database = create_database(1, false);
    for i in 0u32..1000 {
        let key = vec![(i % 256) as u8, (i / 256) as u8];
        database.get_or_create(0, &key).set(key.clone()).unwrap();
    }
    assert_eq!(database.data[0].len(), 1000);
    assert!(database.data[0].capacity() >= 1000);
    for i in 0u32..1000 {
        let key = vec![(i % 256) as u8, (i / 256) as u8];
        database.remove(0, &key).unwrap();
    }
    // no freeing memory
    assert!(database.data[0].capacity() > 1000);
}
