#![feature(collections)]

extern crate config;
extern crate rand;
extern crate response;
extern crate skiplist;
extern crate util;

use std::usize;
use std::fmt;
use std::error::Error;
use std::cmp::Ord;
use std::cmp::Ordering;
use std::collections::Bound;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::LinkedList;
use std::str::from_utf8;
use std::str::Utf8Error;
use std::num::ParseIntError;
use std::sync::mpsc::Sender;

use rand::random;
use skiplist::OrderedSkipList;

use config::Config;
use response::Response;
use util::glob_match;
use util::mstime;


/**
 * SortedSetMember is a wrapper around f64 to implement ordering and equality.
 * f64 does not implement those traits because comparing floats has problems
 * but in the context of rsedis this basic implementation should be enough.
 **/
#[derive(Debug, Clone)]
pub struct SortedSetMember {
    f: f64,
    s: Vec<u8>,
    // this is useful for inclusion/exclusion comparison
    // if true, it will ignore `s` and be the highest possible string
    upper_boundary: bool,
}

impl SortedSetMember {
    pub fn new(f: f64, s: Vec<u8>) -> SortedSetMember {
        SortedSetMember {f: f, s: s, upper_boundary: false}
    }

    pub fn set_upper_boundary(&mut self, upper_boundary: bool) {
        self.upper_boundary = upper_boundary;
    }

    pub fn get_f64(&self) -> &f64 {
        &self.f
    }

    pub fn set_f64(&mut self, f: f64)  {
        self.f = f;
    }

    pub fn get_vec(&self) -> &Vec<u8> {
        &self.s
    }

    pub fn set_vec(&mut self, s: Vec<u8>)  {
        self.s = s;
    }
}

impl Eq for SortedSetMember {}

impl PartialEq for SortedSetMember {
    fn eq(&self, other: &Self) -> bool {
        self.f == other.f && self.s == other.s
    }
}

impl Ord for SortedSetMember {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialOrd for SortedSetMember {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(if self.f < other.f { Ordering::Less }
        else if self.f > other.f { Ordering::Greater }
        else if other.upper_boundary { Ordering::Less }
        else if self.upper_boundary { Ordering::Greater }
        else { return self.s.partial_cmp(&other.s) })
    }

    fn lt(&self, other: &Self) -> bool { self.f < other.f || (self.f == other.f && self.s < other.s) }
    fn le(&self, other: &Self) -> bool { self.f < other.f || (self.f == other.f && self.s <= other.s) }
    fn gt(&self, other: &Self) -> bool { self.f > other.f || (self.f == other.f && self.s > other.s) }
    fn ge(&self, other: &Self) -> bool { self.f > other.f || (self.f == other.f && self.s >= other.s) }
}
#[derive(PartialEq, Debug)]
pub enum Value {
    Nil,
    String(ValueString),
    List(ValueList),
    Set(ValueSet),
    SortedSet(ValueSortedSet),
}

#[derive(PartialEq, Debug, Clone)]
pub enum ValueString {
    Integer(i64),
    Data(Vec<u8>),
}

#[derive(PartialEq, Debug, Clone)]
pub enum ValueList {
    Data(LinkedList<Vec<u8>>),
}

#[derive(PartialEq, Debug, Clone)]
pub enum ValueSet {
    Data(HashSet<Vec<u8>>),
}

#[derive(PartialEq, Debug)]
pub enum ValueSortedSet {
    Data(OrderedSkipList<SortedSetMember>, HashMap<Vec<u8>, f64>),
}

#[derive(Debug)]
pub enum OperationError {
    OverflowError,
    ValueError,
    WrongTypeError,
    OutOfBoundsError,
}

impl fmt::Display for OperationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.description().fmt(f)
    }
}

impl Error for OperationError {
    fn description(&self) -> &str {
        return "oops";
    }
}

impl From<Utf8Error> for OperationError {
    fn from(_: Utf8Error) -> OperationError { OperationError::ValueError }
}

impl From<ParseIntError> for OperationError {
    fn from(_: ParseIntError) -> OperationError { OperationError::ValueError }
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

fn normalize_position(position: i64, _len: usize) -> Result<usize, usize> {
    let len = _len as i64;
    let mut pos = position;
    if pos < 0 {
        pos += len;
    }
    if pos < 0 {
        return Err(0);
    }
    if pos > len {
        return Err(len as usize);
    }
    return Ok(pos as usize);
}

fn is_range_valid<T: Ord>(min: Bound<T>, max: Bound<T>) -> bool {
    let mut both_in = true;
    let v1 = match min {
        Bound::Included(ref v) => v,
        Bound::Excluded(ref v) => { both_in = false; v },
        Bound::Unbounded => return true,
    };

    let v2 = match max {
        Bound::Included(ref v) => v,
        Bound::Excluded(ref v) => { both_in = false; v },
        Bound::Unbounded => return true,
    };

    return v1 < v2 || (v1 == v2 && both_in);
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
        if newvalue.len() < 32 { // ought to be enough!
            if let Ok(utf8) = from_utf8(&*newvalue) {
                if let Ok(i) = utf8.parse::<i64>() {
                    *self = Value::String(ValueString::Integer(i));
                    return Ok(());
                }
            }
        }
        *self = Value::String(ValueString::Data(newvalue));
        return Ok(());
    }

    pub fn get(&self) -> Result<Vec<u8>, OperationError> {
        match self {
            &Value::String(ref value) => match value {
                &ValueString::Data(ref data) => Ok(data.clone()),
                &ValueString::Integer(ref int) => Ok(format!("{}", int).into_bytes()),
            },
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn strlen(&self) -> Result<usize, OperationError> {
        match self {
            &Value::String(ref val) => match val {
                &ValueString::Data(ref data) => Ok(data.len()),
                &ValueString::Integer(ref int) => Ok(format!("{}", int).len()),
            },
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn append(&mut self, newvalue: Vec<u8>) -> Result<usize, OperationError> {
        match self {
            &mut Value::Nil => {
                let len = newvalue.len();
                *self = Value::String(ValueString::Data(newvalue));
                Ok(len)
            },
            &mut Value::String(ref mut value) => match value {
                &mut ValueString::Data(ref mut data) => { data.extend(newvalue); Ok(data.len()) },
                &mut ValueString::Integer(i) => {
                    let oldstr = format!("{}", i);
                    let len = oldstr.len() + newvalue.len();
                    *value = ValueString::Data([oldstr.into_bytes(), newvalue].concat());
                    Ok(len)
                },
            },
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn incr(&mut self, incr: i64) -> Result<i64, OperationError> {
        let mut newval:i64;
        match self {
            &mut Value::Nil => {
                newval = incr;
            },
            &mut Value::String(ref mut value) => {
                match value {
                    &mut ValueString::Integer(i) => {
                        let tmp_newval = i.checked_add(incr);
                        match tmp_newval {
                            Some(v) => newval = v,
                            None => return Err(OperationError::OverflowError),
                        }
                    },
                    &mut ValueString::Data(ref data) => {
                        if data.len() > 32 {
                            return Err(OperationError::OverflowError);
                        }
                        let res = try!(from_utf8(&data));
                        let ival = try!(res.parse::<i64>());
                        let tmp_newval = ival.checked_add(incr);
                        match tmp_newval {
                            Some(v) => newval = v,
                            None => return Err(OperationError::OverflowError),
                        }
                    },
                }
            },
            _ => return Err(OperationError::WrongTypeError),
        }
        *self = Value::String(ValueString::Integer(newval));
        return Ok(newval);
    }

    pub fn getrange(&self, _start: i64, _stop: i64) -> Result<Vec<u8>, OperationError> {
        let s = match self {
            &Value::Nil => return Ok(Vec::new()),
            &Value::String(ref value) => match value {
                &ValueString::Integer(ref i) => format!("{}", i).into_bytes(),
                &ValueString::Data(ref s) => s.clone(),
            },
            _ => return Err(OperationError::WrongTypeError),
        };

        let len = s.len();
        let start = match normalize_position(_start, len) {
            Ok(i) => i,
            Err(i) => if i == 0 { 0 } else { return Ok(Vec::new()); }
        } as usize;
        let stop = match normalize_position(_stop, len) {
            Ok(i) => i,
            Err(i) => if i == 0 { return Ok(Vec::new()); } else { len }
        } as usize;
        let mut v = Vec::with_capacity(stop - start + 1);
        v.extend(s[start..stop + 1].iter());
        Ok(v)
    }

    pub fn setbit(&mut self, bitoffset: usize, on: bool) -> Result<bool, OperationError> {
        match self {
            &mut Value::Nil => *self = Value::String(ValueString::Data(Vec::new())),
            &mut Value::String(ref mut value) => {
                match value {
                    &mut ValueString::Integer(i) => *value = ValueString::Data(format!("{}", i).into_bytes()),
                    &mut ValueString::Data(_) => (),
                }
            }
            _ => return Err(OperationError::WrongTypeError),
        };
        let mut d = match self {
            &mut Value::String(ref mut value) => match value {
                &mut ValueString::Data(ref mut d) => d,
                _ => panic!("Value should be data"),
            },
            _ => panic!("Value should be string"),
        };

        let byte = bitoffset >> 3;

        while byte + 1 > d.len() {
            d.push(0);
        }

        let mut byteval = d[byte];
        let bit = 7 - (bitoffset & 0x7);
        let bitval = byteval & (1 << bit);

        byteval &= !(1 << bit);
        byteval |= (if on { 1 } else { 0 } & 0x1) << bit;
        d[byte] = byteval;

        Ok(bitval != 0)
    }

    pub fn getbit(&self, bitoffset: usize) -> Result<bool, OperationError> {
        let tmp;
        let d = match self {
            &Value::Nil => return Ok(false),
            &Value::String(ref value) => match value {
                &ValueString::Integer(i) => { tmp = format!("{}", i).into_bytes(); &tmp },
                &ValueString::Data(ref d) => d,
            },
            _ => return Err(OperationError::WrongTypeError),
        };

        let byte = bitoffset >> 3;
        if byte >= d.len() {
            return Ok(false);
        }

        let bit = 7 - (bitoffset & 0x7);;
        let bitval = d[byte] & (1 << bit);

        Ok(bitval != 0)
    }

    pub fn setrange(&mut self, _index: i64, data: Vec<u8>) -> Result<usize, OperationError> {
        match self {
            &mut Value::Nil => *self = Value::String(ValueString::Data(Vec::new())),
            &mut Value::String(ref mut value) => match value {
                &mut ValueString::Integer(i) => *value = ValueString::Data(format!("{}", i).into_bytes()),
                &mut ValueString::Data(_) => (),
            },
            _ => return Err(OperationError::WrongTypeError),
        };
        let mut d = match self {
            &mut Value::String(ref mut value) => match value {
                &mut ValueString::Data(ref mut s) => s,
                _ => panic!("Value should be data"),
            },
            _ => panic!("Value should be string"),
        };
        let mut index = match normalize_position(_index, d.len()) {
            Ok(i) => i,
            Err(p) => if p == 0 { p } else { _index as usize },
        };
        for _ in d.len()..index {
            d.push(0);
        }
        for c in data {
            d.push(c);
            if index < d.len() - 1 {
                d.swap_remove(index);
            }
            index += 1;
        }
        Ok(d.len())
    }

    pub fn push(&mut self, el: Vec<u8>, right: bool) -> Result<usize, OperationError> {
        let listsize;
        match self {
            &mut Value::Nil => {
                let mut list = LinkedList::new();
                list.push_back(el);
                *self = Value::List(ValueList::Data(list));
                listsize = 1;
            },
            &mut Value::List(ref mut value) => match value {
                &mut ValueList::Data(ref mut list) => {
                    if right {
                        list.push_back(el);
                    } else {
                        list.push_front(el);
                    }
                    listsize = list.len();
                },
            },
            _ => return Err(OperationError::WrongTypeError),
        }
        return Ok(listsize);
    }

    pub fn pop(&mut self, right: bool) -> Result<Option<Vec<u8>>, OperationError> {
        let el;
        let mut clear;
        match self {
            &mut Value::Nil => {
                return Ok(None);
            },
            &mut Value::List(ref mut value) => match value {
                &mut ValueList::Data(ref mut list) => {
                    if right {
                        el = list.pop_back();
                    } else {
                        el = list.pop_front();
                    }
                    clear = list.len() == 0;
                },
            },
            _ => return Err(OperationError::WrongTypeError),
        }
        if clear {
            *self = Value::Nil;
        }
        return Ok(el);
    }

    pub fn lindex(&self, _index: i64) -> Result<Option<&Vec<u8>>, OperationError> {
        match self {
            &Value::List(ref value) => match value {
                &ValueList::Data(ref list) => {
                    let index = match normalize_position(_index, list.len()) {
                        Ok(i) => i,
                        Err(_) => return Ok(None),
                    };
                    Ok(list.iter().nth(index as usize))
                },
            },
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn linsert(&mut self, before: bool, pivot: Vec<u8>, newvalue: Vec<u8>) -> Result<Option<usize>, OperationError> {
        match self {
            &mut Value::List(ref mut value) => match value {
                &mut ValueList::Data(ref mut list) => {
                    let pos;
                    match list.iter().position(|x| x == &pivot) {
                        Some(_pos) => {
                            if before {
                                pos = _pos;
                            } else {
                                pos = _pos + 1;
                            }
                        },
                        None => return Ok(None),
                    }
                    let mut right = list.split_off(pos);
                    list.push_back(newvalue);
                    list.append(&mut right);
                    return Ok(Some(list.len()));
                },
            },
            _ => return Err(OperationError::WrongTypeError),
        };
    }

    pub fn llen(&self) -> Result<usize, OperationError> {
        return match self {
            &Value::Nil => Ok(0),
            &Value::List(ref value) => match value {
                &ValueList::Data(ref list) => Ok(list.len()),
            },
            _ => Err(OperationError::WrongTypeError),
        };
    }

    pub fn lrange(&self, _start: i64, _stop: i64) -> Result<Vec<&Vec<u8>>, OperationError> {
        match self {
            &Value::List(ref value) => match value {
                &ValueList::Data(ref list) => {
                    let len = list.len();
                    let start = match normalize_position(_start, len) {
                        Ok(i) => i,
                        Err(i) => if i == 0 { 0 } else { return Ok(Vec::new()); },
                    };
                    let stop = match normalize_position(_stop, len) {
                        Ok(i) => i,
                        Err(i) => if i == 0 { return Ok(Vec::new()); } else { i },
                    };
                    Ok(list.iter().skip(start as usize).take(stop as usize - start as usize + 1).collect::<Vec<_>>())
                },
            },
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn lrem(&mut self, left: bool, limit: usize, newvalue: Vec<u8>) -> Result<usize, OperationError> {
        let mut count = 0;
        let mut newlist = LinkedList::new();
        match self {
            &mut Value::List(ref mut value) => match value {
                &mut ValueList::Data(ref mut list) => {
                    if left {
                        while limit == 0 || count < limit {
                            match list.pop_front() {
                                None => break,
                                Some(el) => {
                                    if el != newvalue {
                                        newlist.push_back(el);
                                    } else {
                                        count += 1;
                                    }
                                }
                            }
                        }
                        newlist.append(list);
                    } else {
                        while limit == 0 || count < limit {
                            match list.pop_back() {
                                None => break,
                                Some(el) => {
                                    if el != newvalue {
                                        newlist.push_front(el);
                                    } else {
                                        count += 1;
                                    }
                                }
                            }
                        }
                        // omg, ugly code, let me explain
                        // append will merge right two lists and clear the parameter
                        // newlist is the one that will survive after lrem
                        // but list needs to be at the beginning, so we are merging
                        // first to list and then to newlist
                        list.append(&mut newlist);
                        newlist.append(list);
                    }
                },
            },
            _ => return Err(OperationError::WrongTypeError),
        };
        if newlist.len() == 0 {
            *self = Value::Nil;
        } else {
            *self = Value::List(ValueList::Data(newlist));
        }
        return Ok(count);
    }

    pub fn lset(&mut self, index: i64, newvalue: Vec<u8>) -> Result<(), OperationError> {
        return match self {
            &mut Value::List(ref mut value) => match value {
                &mut ValueList::Data(ref mut list) => {
                    let i = match normalize_position(index, list.len()) {
                        Ok(i) => i,
                        Err(_) => return Err(OperationError::OutOfBoundsError),
                    };
                    // this unwrap is safe because `i` is already validated to be inside the list
                    let el = list.iter_mut().skip(i).next().unwrap();
                    *el = newvalue;
                    Ok(())
                }
            },
            _ => return Err(OperationError::WrongTypeError),
        }
    }

    pub fn ltrim(&mut self, _start: i64, _stop: i64) -> Result<(), OperationError> {
        let mut newlist;
        match self {
            &mut Value::List(ref mut value) => match value {
                &mut ValueList::Data(ref mut list) => {
                    let len = list.len();
                    let start = match normalize_position(_start, len) {
                        Ok(i) => i,
                        Err(i) => if i == 0 { 0 } else {
                            list.split_off(len);
                            len
                        },
                    };
                    let stop = match normalize_position(_stop, len) {
                        Ok(i) => i,
                        Err(i) => if i == 0 {
                            list.split_off(len);
                            0
                        } else { i },
                    };
                    list.split_off(stop + 1);
                    newlist = list.split_off(start);
                }
            },
            _ => return Err(OperationError::WrongTypeError),
        }
        *self = Value::List(ValueList::Data(newlist));
        return Ok(());
    }

    pub fn sadd(&mut self, el: Vec<u8>) -> Result<bool, OperationError> {
        match self {
            &mut Value::Nil => {
                let mut set = HashSet::new();
                set.insert(el);
                *self = Value::Set(ValueSet::Data(set));
                Ok(true)
            },
            &mut Value::Set(ref mut value) => match value {
                &mut ValueSet::Data(ref mut set) => Ok(set.insert(el)),
            },
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn srem(&mut self, el: &Vec<u8>) -> Result<bool, OperationError> {
        match self {
            &mut Value::Nil => Ok(false),
            &mut Value::Set(ref mut value) => match value {
                &mut ValueSet::Data(ref mut set) => Ok(set.remove(el)),
            },
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn sismember(&self, el: &Vec<u8>) -> Result<bool, OperationError> {
        match self {
            &Value::Nil => Ok(false),
            &Value::Set(ref value) => match value {
                &ValueSet::Data(ref set) => Ok(set.contains(el)),
            },
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn scard(&self) -> Result<usize, OperationError> {
        match self {
            &Value::Nil => Ok(0),
            &Value::Set(ref value) => match value {
                &ValueSet::Data(ref set) => Ok(set.len()),
            },
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn smembers(&self) -> Result<Vec<Vec<u8>>, OperationError> {
        match self {
            &Value::Nil => Ok(vec![]),
            &Value::Set(ref value) => match value {
                &ValueSet::Data(ref set) => Ok(set.iter().map(|x| x.clone()).collect::<Vec<_>>()),
            },
            _ => Err(OperationError::WrongTypeError),
        }
    }


    pub fn srandmember(&self, count: usize, allow_duplicates: bool) -> Result<Vec<Vec<u8>>, OperationError> {
        // TODO: implemented in O(n), should be O(1)
        let set = match self {
            &Value::Nil => return Ok(Vec::new()),
            &Value::Set(ref value) => match value {
                &ValueSet::Data(ref s) => s,
            },
            _ => return Err(OperationError::WrongTypeError),
        };

        if allow_duplicates {
            let mut r = Vec::new();
            for _ in 0..count {
                let pos = random::<usize>() % set.len();
                r.push(set.iter().skip(pos).take(1).next().unwrap().clone());
            }
            return Ok(r);
        } else {
            if count >= set.len() {
                return Ok(set.iter().map(|x| x.clone()).collect::<Vec<_>>());
            }
            let mut s = HashSet::new();
            while s.len() < count {
                let pos = random::<usize>() % set.len();
                s.insert(set.iter().skip(pos).take(1).next().unwrap().clone());
            }
            return Ok(s.iter().map(|x| x.clone()).collect::<Vec<_>>());
        }
    }

    pub fn spop(&mut self, count: usize) -> Result<Vec<Vec<u8>>, OperationError> {
        // TODO: implemented in O(n), should be O(1)

        let len = try!(self.scard());
        if count >= len {
            let r = {
                let set = match self {
                    &mut Value::Nil => return Ok(Vec::new()),
                    &mut Value::Set(ref mut value) => match value {
                        &mut ValueSet::Data(ref s) => s,
                    },
                    _ => return Err(OperationError::WrongTypeError),
                };
                set.iter().map(|x| x.clone()).collect::<Vec<_>>()
            };
            *self = Value::Nil;
            return Ok(r);
        }

        let r = try!(self.srandmember(count, false));

        let mut set = match self {
            &mut Value::Nil => return Ok(Vec::new()),
            &mut Value::Set(ref mut value) => match value {
                &mut ValueSet::Data(ref mut s) => s,
            },
            _ => return Err(OperationError::WrongTypeError),
        };

        for member in r.iter() {
            set.remove(member);
        }

        Ok(r)
    }

    pub fn sdiff(&self, sets: &Vec<&Value>) -> Result<HashSet<Vec<u8>>, OperationError> {
        match self {
            &Value::Nil => Ok(HashSet::new()),
            &Value::Set(ref value) => match value {
                &ValueSet::Data(ref original_set) => {
                    let mut elements: HashSet<Vec<u8>> = original_set.clone();
                    for newvalue in sets {
                        match newvalue {
                            &&Value::Nil => {},
                            &&Value::Set(ref value) => match value {
                                &ValueSet::Data(ref set) => {
                                    for el in set {
                                        elements.remove(el);
                                    }
                                },
                            },
                            _ => return Err(OperationError::WrongTypeError),
                        }
                    }
                    Ok(elements)
                },
            },
            _ => Err(OperationError::WrongTypeError),
        }
    }

    fn get_set<'a>(&'a self, val: &'a Value) -> Result<Option<&HashSet<Vec<u8>>>, OperationError> {
        match val {
            &Value::Nil => Ok(None),
            &Value::Set(ref value) => match value {
                &ValueSet::Data(ref original_set) => {
                    Ok(Some(original_set))
                },
            },
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn sinter(&self, sets: &Vec<&Value>) -> Result<HashSet<Vec<u8>>, OperationError> {
        let mut result = match try!(self.get_set(self)) {
            Some(s) => s.clone(),
            None => return Ok(HashSet::new()),
        };

        for s in sets.iter() {
            result = match try!(self.get_set(s)) {
                Some(set) => result.intersection(set).cloned().collect(),
                None => return Ok(HashSet::new()),
            };
            if result.len() == 0 {
                break;
            }
        }
        Ok(result)
    }

    pub fn sunion(&self, sets: &Vec<&Value>) -> Result<HashSet<Vec<u8>>, OperationError> {
        let mut result = match try!(self.get_set(self)) {
            Some(s) => s.clone(),
            None => HashSet::new(),
        };

        for s in sets.iter() {
            result = match try!(self.get_set(s)) {
                Some(set) => result.union(set).cloned().collect(),
                None => result,
            };
        }
        Ok(result)
    }

    pub fn create_set(&mut self, set: HashSet<Vec<u8>>) {
        *self = Value::Set(ValueSet::Data(set));
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
                let mut skiplist = OrderedSkipList::new();
                let mut hmap = HashMap::new();
                skiplist.insert(SortedSetMember::new(s.clone(), el.clone()));
                hmap.insert(el, s);
                *self = Value::SortedSet(ValueSortedSet::Data(skiplist, hmap));
                Ok(true)
            },
            &mut Value::SortedSet(ref mut value) => match value {
                &mut ValueSortedSet::Data(ref mut skiplist, ref mut hmap) => {
                    let mut score = s.clone();
                    let contains = hmap.contains_key(&el);
                    if contains && nx {
                        return Ok(false);
                    }
                    if !contains && xx {
                        return Ok(false);
                    }
                    if contains {
                        let val = hmap.get(&el).unwrap();
                        if ch && !incr && val == &s {
                            return Ok(false);
                        }
                        skiplist.remove(&SortedSetMember::new(val.clone(), el.clone()));
                        if incr {
                            score += val.clone();
                        }
                    }
                    skiplist.insert(SortedSetMember::new(score.clone(), el.clone()));
                    hmap.insert(el, score);
                    if ch {
                        Ok(true)
                    } else {
                        Ok(!contains)
                    }
                },
            },
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
            &mut Value::SortedSet(ref mut value) => match value {
                &mut ValueSortedSet::Data(ref mut skiplist, ref mut hmap) => {
                    let mut val = match hmap.get(&member) {
                        Some(val) => {
                            skiplist.remove(&SortedSetMember::new(val.clone(), member.clone()));
                            val.clone()
                        },
                        None => 0.0,
                    };
                    val += increment;
                    skiplist.insert(SortedSetMember::new(val.clone(), member.clone()));
                    hmap.insert(member, val.clone());
                    Ok(val)
                },
            },
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn zcount(&self, min: Bound<f64>, max: Bound<f64>) -> Result<usize, OperationError> {
        let skiplist = match self {
            &Value::Nil => return Ok(0),
            &Value::SortedSet(ref value) => match value {
                &ValueSortedSet::Data(ref skiplist, _) => skiplist,
            },
            _ => return Err(OperationError::WrongTypeError),
        };
        let mut f1 = SortedSetMember::new(0.0, vec![]);
        let mut f2 = SortedSetMember::new(0.0, vec![]);
        let m1 = match min {
            Bound::Included(f) => { f1.set_f64(f); Bound::Included(&f1) },
            Bound::Excluded(f) => { f1.set_f64(f); f1.set_upper_boundary(true); Bound::Excluded(&f1) },
            Bound::Unbounded => Bound::Unbounded,
        };

        let m2 = match max {
            Bound::Included(f) => { f2.set_f64(f); f2.set_upper_boundary(true); Bound::Included(&f2) },
            Bound::Excluded(f) => { f2.set_f64(f); Bound::Excluded(&f2) },
            Bound::Unbounded => Bound::Unbounded,
        };

        Ok(skiplist.range(m1, m2).collect::<Vec<_>>().len())
    }

    pub fn zrange(&self, _start: i64, _stop: i64, withscores: bool, rev: bool) -> Result<Vec<Vec<u8>>, OperationError> {
        let skiplist = match self {
            &Value::Nil => return Ok(vec![]),
            &Value::SortedSet(ref value) => match value {
                &ValueSortedSet::Data(ref skiplist, _) => skiplist,
            },
            _ => return Err(OperationError::WrongTypeError),
        };

        let len = skiplist.len();
        let (start, stop) = if rev {
            (match normalize_position(- _stop - 1, len) {
                Ok(i) => i,
                Err(i) => if i == 0 { 0 } else { return Ok(vec![]); },
            },
            match normalize_position(- _start - 1, len) {
                Ok(i) => i,
                Err(i) => if i == 0 { return Ok(vec![]); } else { i },
            })
        } else {
            (match normalize_position(_start, len) {
                Ok(i) => i,
                Err(i) => if i == 0 { 0 } else { return Ok(vec![]); },
            },
            match normalize_position(_stop, len) {
                Ok(i) => i,
                Err(i) => if i == 0 { return Ok(vec![]); } else { i },
            })
        };

        if stop < start {
            return Ok(vec![]);
        }
        let first = skiplist.get(&start).unwrap();
        let mut r = vec![];
        if rev {
            for member in skiplist.range(Bound::Included(first), Bound::Unbounded).take(stop - start + 1) {
                if withscores {
                    r.push(format!("{}", member.get_f64()).into_bytes());
                }
                r.push(member.get_vec().clone());
            }
            r = r.iter().rev().cloned().collect::<Vec<_>>();
        } else {
            for member in skiplist.range(Bound::Included(first), Bound::Unbounded).take(stop - start + 1) {
                r.push(member.get_vec().clone());
                if withscores {
                    r.push(format!("{}", member.get_f64()).into_bytes());
                }
            }
        }
        Ok(r)
    }

    pub fn zrangebyscore(&self, _min: Bound<f64>, _max: Bound<f64>, withscores: bool, offset: usize, count: usize, rev: bool) -> Result<Vec<Vec<u8>>, OperationError> {
        let skiplist = match self {
            &Value::Nil => return Ok(vec![]),
            &Value::SortedSet(ref value) => match value {
                &ValueSortedSet::Data(ref skiplist, _) => skiplist,
            },
            _ => return Err(OperationError::WrongTypeError),
        };

        // FIXME: duplicated code from ZCOUNT. Trying to create a factory
        // function for this, but I failed because allocation was going
        // out of scope.
        // Probably more function will copy this until I can figure out
        // a better way.
        let mut f1 = SortedSetMember::new(0.0, vec![]);
        let mut f2 = SortedSetMember::new(0.0, vec![]);

        let (min, max) = if rev {
            (_max, _min)
        } else {
            (_min, _max)
        };

        let m1 = match min {
            Bound::Included(f) => { f1.set_f64(f); Bound::Included(&f1) },
            Bound::Excluded(f) => { f1.set_f64(f); f1.set_upper_boundary(true); Bound::Excluded(&f1) },
            Bound::Unbounded => Bound::Unbounded,
        };

        let m2 = match max {
            Bound::Included(f) => { f2.set_f64(f); f2.set_upper_boundary(true); Bound::Included(&f2) },
            Bound::Excluded(f) => { f2.set_f64(f); Bound::Excluded(&f2) },
            Bound::Unbounded => Bound::Unbounded,
        };

        if !is_range_valid(m1, m2) {
            return Ok(vec![])
        }

        // FIXME: skiplist crashes when using .range().rev()
        let mut r = vec![];
        if rev {
            let len = skiplist.len();
            let mut c = count;
            if c + offset > len {
                c = len - offset;
            }
            for member in skiplist.range(m1, m2).skip(skiplist.len() - offset - c).take(c) {
                if withscores {
                    r.push(format!("{}", member.get_f64()).into_bytes());
                }
                r.push(member.get_vec().clone());
            }
            Ok(r.iter().cloned().rev().collect::<Vec<_>>())
        } else {
            for member in skiplist.range(m1, m2).skip(offset).take(count) {
                r.push(member.get_vec().clone());
                if withscores {
                    r.push(format!("{}", member.get_f64()).into_bytes());
                }
            }
            Ok(r)
        }
    }

    pub fn zrank(&self, el: Vec<u8>) -> Result<Option<usize>, OperationError> {
        let (skiplist, hashmap) = match self {
            &Value::Nil => return Ok(None),
            &Value::SortedSet(ref value) => match value {
                &ValueSortedSet::Data(ref skiplist, ref hashmap) => (skiplist, hashmap),
            },
            _ => return Err(OperationError::WrongTypeError),
        };

        let score = match hashmap.get(&el) {
            Some(s) => s,
            None => return Ok(None),
        };

        let member = SortedSetMember::new(score.clone(), el);
        return Ok(Some(skiplist.range(Bound::Unbounded, Bound::Included(&member)).collect::<Vec<_>>().len() - 1));
    }
}

pub struct Database {
    data: Vec<HashMap<Vec<u8>, Value>>,
    data_expiration_ns: Vec<HashMap<Vec<u8>, i64>>,
    pub size: usize,
    subscribers: HashMap<Vec<u8>, HashMap<usize, Sender<PubsubEvent>>>,
    pattern_subscribers: HashMap<Vec<u8>, HashMap<usize, Sender<PubsubEvent>>>,
    key_subscribers: HashMap<Vec<u8>, HashMap<usize, Sender<bool>>>,
    subscriber_id: usize,
}

fn create_database(size: usize) -> Database {
    let mut data = Vec::with_capacity(size);
    let mut data_expiration_ns = Vec::with_capacity(size);
    for _ in 0..size {
        data.push(HashMap::new());
        data_expiration_ns.push(HashMap::new());
    }
    return Database {
        data: data,
        data_expiration_ns: data_expiration_ns,
        size: size,
        subscribers: HashMap::new(),
        pattern_subscribers: HashMap::new(),
        key_subscribers: HashMap::new(),
        subscriber_id: 0,
    };
}

impl Database {
    pub fn mock() -> Database {
        create_database(16)
    }

    pub fn new(config: &Config) -> Database {
        create_database(config.databases as usize)
    }

    fn is_expired(&self, index: usize, key: &Vec<u8>) -> bool {
        match self.data_expiration_ns[index].get(key) {
            Some(t) => t < &mstime(),
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

    fn ensure_key_subscribers(&mut self, key: &Vec<u8>) {
        if !self.key_subscribers.contains_key(key) {
            self.key_subscribers.insert(key.clone(), HashMap::new());
        }
    }

    pub fn key_subscribe(&mut self, key: &Vec<u8>, sender: Sender<bool>) -> usize {
        self.ensure_key_subscribers(key);
        let mut key_subscribers = self.key_subscribers.get_mut(key).unwrap();
        let subscriber_id = self.subscriber_id;
        key_subscribers.insert(subscriber_id, sender);
        self.subscriber_id += 1;
        subscriber_id
    }

    pub fn key_publish(&mut self, key: &Vec<u8>) {
        let mut torem = Vec::new();
        match self.key_subscribers.get_mut(key) {
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
    let mut value = Value::List(ValueList::Data(LinkedList::new()));

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
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
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
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
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
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
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
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
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
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
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
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
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
    assert_eq!(value, Value::Nil);
}

#[test]
fn lrem_left_limited() {
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
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
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
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
    assert_eq!(value, Value::Nil);
}

#[test]
fn lrem_right_limited() {
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
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
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
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
    let mut value = Value::List(ValueList::Data(LinkedList::new()));
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
