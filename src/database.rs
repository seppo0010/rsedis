use std::fmt;
use std::error::Error;
use std::collections::HashMap;
use std::collections::LinkedList;
use std::str::from_utf8;
use std::str::Utf8Error;
use std::num::ParseIntError;

pub enum Value {
    Nil,
    Integer(i64),
    Data(Vec<u8>),
    List(LinkedList<Vec<u8>>),
}

#[derive(Debug)]
pub enum OperationError {
    OverflowError,
    ValueError,
    WrongTypeError,
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
                let index;
                let len = list.len() as i64;
                if _index < 0 {
                    index = len + _index;
                } else {
                    index = _index;
                }
                if index < 0 || index >= len {
                    return Ok(None);
                }
                return Ok(list.iter().nth(index as usize));
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
