use std::fmt;
use std::error::Error;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::LinkedList;
use std::str::from_utf8;
use std::str::Utf8Error;
use std::num::ParseIntError;

#[derive(PartialEq)]
#[derive(Debug)]
pub enum Value {
    Nil,
    Integer(i64),
    Data(Vec<u8>),
    List(LinkedList<Vec<u8>>),
    Set(HashSet<Vec<u8>>),
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

impl Value {
    pub fn set(&mut self, value: Vec<u8>) -> Result<(), OperationError> {
        if value.len() < 32 { // ought to be enough!
            let try_utf8 = from_utf8(&*value);
            if try_utf8.is_ok() {
                let try_parse = try_utf8.unwrap().parse::<i64>();
                if try_parse.is_ok() {
                    *self = Value::Integer(try_parse.unwrap());
                    return Ok(());
                }
            }
        }
        *self = Value::Data(value);
        return Ok(());
    }

    pub fn append(&mut self, value: Vec<u8>) -> Result<usize, OperationError> {
        match self {
            &mut Value::Nil => {
                let len = value.len();
                *self = Value::Data(value);
                return Ok(len);
            },
            &mut Value::Data(ref mut data) => { data.extend(value); return Ok(data.len()); },
            &mut Value::Integer(i) => {
                let oldstr = format!("{}", i);
                let len = oldstr.len() + value.len();
                *self = Value::Data([oldstr.into_bytes(), value].concat());
                return Ok(len);
            },
            _ => return Err(OperationError::WrongTypeError),
        }
    }

    pub fn incr(&mut self, incr: i64) -> Result<i64, OperationError> {
        let mut newval:i64;
        match self {
            &mut Value::Nil => {
                newval = incr;
            },
            &mut Value::Integer(i) => {
                let tmp_newval = i.checked_add(incr);
                match tmp_newval {
                    Some(v) => newval = v,
                    None => return Err(OperationError::OverflowError),
                }
            },
            &mut Value::Data(ref data) => {
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
            _ => return Err(OperationError::WrongTypeError),
        }
        *self = Value::Integer(newval);
        return Ok(newval);
    }

    pub fn push(&mut self, el: Vec<u8>, right: bool) -> Result<usize, OperationError> {
        let listsize;
        match self {
            &mut Value::Nil => {
                let mut list = LinkedList::new();
                list.push_back(el);
                *self = Value::List(list);
                listsize = 1;
            },
            &mut Value::List(ref mut list) => {
                if right {
                    list.push_back(el);
                } else {
                    list.push_front(el);
                }
                listsize = list.len();
            }
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
            &mut Value::List(ref mut list) => {
                if right {
                    el = list.pop_back();
                } else {
                    el = list.pop_front();
                }
                clear = list.len() == 0;
            }
            _ => return Err(OperationError::WrongTypeError),
        }
        if clear {
            *self = Value::Nil;
        }
        return Ok(el);
    }

    pub fn lindex(&self, _index: i64) -> Result<Option<&Vec<u8>>, OperationError> {
        return match self {
            &Value::List(ref list) => {
                let index = match normalize_position(_index, list.len()) {
                    Ok(i) => i,
                    Err(_) => return Ok(None),
                };
                return Ok(list.iter().nth(index as usize));
            },
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn linsert(&mut self, before: bool, pivot: Vec<u8>, value: Vec<u8>) -> Result<Option<usize>, OperationError> {
        match self {
            &mut Value::List(ref mut list) => {
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
                list.push_back(value);
                list.append(&mut right);
                return Ok(Some(list.len()));
            },
            _ => return Err(OperationError::WrongTypeError),
        };
    }

    pub fn llen(&self) -> Result<usize, OperationError> {
        return match self {
            &Value::List(ref list) => Ok(list.len()),
            &Value::Nil => Ok(0),
            _ => Err(OperationError::WrongTypeError),
        };
    }

    pub fn lrange(&self, _start: i64, _stop: i64) -> Result<Vec<&Vec<u8>>, OperationError> {
        match self {
            &Value::List(ref list) => {
                let len = list.len();
                let start = match normalize_position(_start, len) {
                    Ok(i) => i,
                    Err(i) => if i == 0 { 0 } else { return Ok(Vec::new()); },
                };
                let stop = match normalize_position(_stop, len) {
                    Ok(i) => i,
                    Err(i) => if i == 0 { return Ok(Vec::new()); } else { i },
                };
                return Ok(list.iter().skip(start as usize).take(stop as usize - start as usize + 1).collect());
            },
            _ => return Err(OperationError::WrongTypeError),
        };
    }

    pub fn lrem(&mut self, left: bool, limit: usize, value: Vec<u8>) -> Result<usize, OperationError> {
        let mut count = 0;
        let mut newlist = LinkedList::new();
        match self {
            &mut Value::List(ref mut list) => {
                if left {
                    while limit == 0 || count < limit {
                        match list.pop_front() {
                            None => break,
                            Some(el) => {
                                if el != value {
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
                                if el != value {
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
            _ => return Err(OperationError::WrongTypeError),
        };
        if newlist.len() == 0 {
            *self = Value::Nil;
        } else {
            *self = Value::List(newlist);
        }
        return Ok(count);
    }

    pub fn lset(&mut self, index: i64, value: Vec<u8>) -> Result<(), OperationError> {
        return match self {
            &mut Value::List(ref mut list) => {
                let i = match normalize_position(index, list.len()) {
                    Ok(i) => i,
                    Err(_) => return Err(OperationError::OutOfBoundsError),
                };
                let el = list.iter_mut().skip(i).next().unwrap();
                *el = value;
                return Ok(());
            },
            _ => return Err(OperationError::WrongTypeError),
        }
    }

    pub fn ltrim(&mut self, _start: i64, _stop: i64) -> Result<(), OperationError> {
        let mut newlist;
        match self {
            &mut Value::List(ref mut list) => {
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
            },
            _ => return Err(OperationError::WrongTypeError),
        }
        *self = Value::List(newlist);
        return Ok(());
    }

    pub fn sadd(&mut self, el: Vec<u8>) -> Result<bool, OperationError> {
        match self {
            &mut Value::Nil => {
                let mut set = HashSet::new();
                set.insert(el);
                *self = Value::Set(set);
                Ok(true)
            },
            &mut Value::Set(ref mut set) => Ok(set.insert(el)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn scard(&self) -> Result<usize, OperationError> {
        match self {
            &Value::Nil => Ok(0),
            &Value::Set(ref set) => Ok(set.len()),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    pub fn sdiff(&self, sets: &Vec<&Value>) -> Result<HashSet<Vec<u8>>, OperationError> {
        match self {
            &Value::Nil => Ok(HashSet::new()),
            &Value::Set(ref original_set) => {
                let mut elements: HashSet<Vec<u8>> = original_set.clone();
                for value in sets {
                    match value {
                        &&Value::Nil => {},
                        &&Value::Set(ref set) => {
                            for el in set {
                                elements.remove(el);
                            }
                        },
                        _ => return Err(OperationError::WrongTypeError),
                    }
                }
                Ok(elements)
            },
            _ => Err(OperationError::WrongTypeError),
        }
    }
}

pub struct Database {
    data: HashMap<Vec<u8>, Value>,
}

impl Database {
    pub fn new() -> Database {
        return Database {
            data: HashMap::new(),
        };
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<&Value> {
        return self.data.get(key);
    }

    pub fn get_mut(&mut self, key: &Vec<u8>) -> Option<&mut Value> {
        return self.data.get_mut(key);
    }

    pub fn remove(&mut self, key: &Vec<u8>) -> Option<Value> {
        return self.data.remove(key);
    }

    pub fn clear(&mut self) {
        return self.data.clear();
    }

    pub fn get_or_create(&mut self, key: &Vec<u8>) -> &mut Value {
        if self.data.contains_key(key) {
            return self.data.get_mut(key).unwrap();
        }
        let val = Value::Nil;
        self.data.insert(Vec::clone(key), val);
        return self.data.get_mut(key).unwrap();
    }
}
