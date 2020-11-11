extern crate basichll;
extern crate config;
#[macro_use(log)]
extern crate logger;
extern crate crc64;
extern crate parser;
extern crate persistence;
extern crate rand;
extern crate rdbutil;
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

use std::collections::Bound;
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::io::Write;
use std::iter::FromIterator;
use std::ops::RangeFull;
use std::path::Path;
use std::sync::mpsc::Sender;

use config::Config;
use crc64::crc64;
use logger::{Level, Logger};
use parser::ParsedCommand;
use persistence::aof::Aof;
use rehashinghashmap::RehashingHashMap;
use response::Response;
use util::{get_random_hex_chars, glob_match, mstime};

use error::OperationError;
use list::ValueList;
use rdbutil::encode_u64_to_slice_u8;
use set::ValueSet;
use string::ValueString;
use zset::ValueSortedSet;

const ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP: usize = 20;

/// Any value storable in the database
#[derive(PartialEq, Debug)]
pub enum Value {
    /// Nil should not be stored, but it is used as a default for initialized values
    Nil,
    String(ValueString),
    List(ValueList),
    Set(ValueSet),
    SortedSet(ValueSortedSet),
}

/// Events relevant for clients in pubsub mode
#[derive(PartialEq, Debug)]
pub enum PubsubEvent {
    /// Client subscribe to channel and its the subscription number.
    Subscription(Vec<u8>, usize),
    /// Client unsubscribe from channel and the number of remaining subscriptions.
    Unsubscription(Vec<u8>, usize),
    /// Client subscribe to pattern and its the subscription number.
    PatternSubscription(Vec<u8>, usize),
    /// Client unsubscribe from pattern and the number of remaining subscriptions.
    PatternUnsubscription(Vec<u8>, usize),
    /// A message was received, it may have matched a pattern and it was sent in a channel.
    Message(Vec<u8>, Option<Vec<u8>>, Vec<u8>),
}

impl PubsubEvent {
    /// Serialize the event into a Response object.
    pub fn as_response(&self) -> Response {
        match self {
            PubsubEvent::Message(channel, pattern, message) => match pattern {
                Some(pattern) => Response::Array(vec![
                    Response::Data(b"pmessage".to_vec()),
                    Response::Data(pattern.clone()),
                    Response::Data(channel.clone()),
                    Response::Data(message.clone()),
                ]),
                None => Response::Array(vec![
                    Response::Data(b"message".to_vec()),
                    Response::Data(channel.clone()),
                    Response::Data(message.clone()),
                ]),
            },
            PubsubEvent::Subscription(channel, subscriptions) => Response::Array(vec![
                Response::Data(b"subscribe".to_vec()),
                Response::Data(channel.clone()),
                Response::Integer(*subscriptions as i64),
            ]),
            PubsubEvent::Unsubscription(channel, subscriptions) => Response::Array(vec![
                Response::Data(b"unsubscribe".to_vec()),
                Response::Data(channel.clone()),
                Response::Integer(*subscriptions as i64),
            ]),
            PubsubEvent::PatternSubscription(pattern, subscriptions) => Response::Array(vec![
                Response::Data(b"psubscribe".to_vec()),
                Response::Data(pattern.clone()),
                Response::Integer(*subscriptions as i64),
            ]),
            PubsubEvent::PatternUnsubscription(pattern, subscriptions) => Response::Array(vec![
                Response::Data(b"punsubscribe".to_vec()),
                Response::Data(pattern.clone()),
                Response::Integer(*subscriptions as i64),
            ]),
        }
    }
}

/// Gets all ValueSet references from a list of Value references.
/// If a Value is nil, `default` is used.
/// If any of the values is not a set, an error is returned instead.
fn get_set_list<'a>(
    set_values: &[&'a Value],
    default: &'a ValueSet,
) -> Result<Vec<&'a ValueSet>, OperationError> {
    let mut sets = Vec::with_capacity(set_values.len());
    for value in set_values {
        sets.push(match value {
            Value::Nil => default,
            Value::Set(value) => value,
            _ => return Err(OperationError::WrongTypeError),
        });
    }
    Ok(sets)
}

/// Gets all ValueSortedSet references from a list of Value references.
/// If a Value is nil, `default` is used.
/// If any of the values is not a zset, an error is returned instead.
fn get_zset_list<'a>(
    zset_values: &[&'a Value],
    default: &'a ValueSortedSet,
) -> Result<Vec<&'a ValueSortedSet>, OperationError> {
    let mut zsets = Vec::with_capacity(zset_values.len());
    for value in zset_values {
        zsets.push(match value {
            Value::Nil => default,
            Value::SortedSet(value) => value,
            _ => return Err(OperationError::WrongTypeError),
        });
    }
    Ok(zsets)
}

impl Value {
    /// Returns true if the value is uninitialized.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use database::string::ValueString;
    ///
    /// assert!(Value::Nil.is_nil());
    /// assert!(!Value::String(ValueString::Integer(1)).is_nil());
    /// ```
    pub fn is_nil(&self) -> bool {
        match self {
            Value::Nil => true,
            _ => false,
        }
    }

    /// Returns true if the value is a string.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use database::string::ValueString;
    ///
    /// assert!(!Value::Nil.is_string());
    /// assert!(Value::String(ValueString::Integer(1)).is_string());
    /// ```
    pub fn is_string(&self) -> bool {
        match self {
            Value::String(_) => true,
            _ => false,
        }
    }

    /// Returns true if the value is a list.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use database::list::ValueList;
    ///
    /// assert!(!Value::Nil.is_list());
    /// assert!(Value::List(ValueList::new()).is_list());
    /// ```
    pub fn is_list(&self) -> bool {
        match self {
            Value::List(_) => true,
            _ => false,
        }
    }

    /// Returns true if the value is a set.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use database::set::ValueSet;
    ///
    /// assert!(!Value::Nil.is_set());
    /// assert!(Value::Set(ValueSet::new()).is_set());
    /// ```
    pub fn is_set(&self) -> bool {
        match self {
            Value::Set(_) => true,
            _ => false,
        }
    }

    /// Sets the value to a string.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// val.set(vec![1, 245, 3]);
    /// assert_eq!(val.get().unwrap(), vec![1, 245, 3]);
    /// ```
    pub fn set(&mut self, newvalue: Vec<u8>) -> Result<(), OperationError> {
        *self = Value::String(ValueString::new(newvalue));

        Ok(())
    }

    /// Gets the string value. Fails if the value is not a string.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.get().unwrap(), vec![]);
    /// val.set(vec![1, 245, 3]).unwrap();
    /// assert_eq!(val.get().unwrap(), vec![1, 245, 3]);
    /// ```
    ///
    /// ```
    /// use database::Value;
    /// use database::list::ValueList;
    ///
    /// assert!(Value::List(ValueList::new()).get().is_err());
    /// ```
    pub fn get(&self) -> Result<Vec<u8>, OperationError> {
        match self {
            Value::Nil => Ok(vec![]),
            Value::String(value) => Ok(value.to_vec()),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Gets the number of bytes in the string. Fails if the value is not a string.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.strlen().unwrap(), 0);
    /// val.set(vec![1, 245, 3]).unwrap();
    /// assert_eq!(val.strlen().unwrap(), 3);
    /// ```
    pub fn strlen(&self) -> Result<usize, OperationError> {
        match self {
            Value::Nil => Ok(0),
            Value::String(val) => Ok(val.strlen()),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Appends the parameter to the string. Creates a new one if it is null.
    /// Fails if the value is not a string.
    /// Returns the number of bytes with the new data appended
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.append(vec![1, 2, 3]).unwrap(), 3);
    /// assert_eq!(val.append(vec![4, 5, 6]).unwrap(), 6);
    /// ```
    pub fn append(&mut self, newvalue: Vec<u8>) -> Result<usize, OperationError> {
        match self {
            Value::Nil => {
                let len = newvalue.len();
                *self = Value::String(ValueString::new(newvalue));
                Ok(len)
            }
            Value::String(val) => {
                val.append(newvalue);
                Ok(val.strlen())
            }
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Increments the ASCII numeric value in the string. Creates a new one if
    /// it didn't exist. Fails if it is not a string.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.incr(3).unwrap(), 3);
    /// assert_eq!(val.incr(3).unwrap(), 6);
    /// assert_eq!(val.get().unwrap(), b"6".to_vec());
    /// ```
    pub fn incr(&mut self, incr: i64) -> Result<i64, OperationError> {
        match self {
            Value::Nil => {
                *self = Value::String(ValueString::Integer(incr));
                Ok(incr)
            }
            Value::String(value) => value.incr(incr),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Increments the ASCII float value in the string. Creates a new one if
    /// it didn't exist. Fails if it is not a string.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.incrbyfloat(3.3).unwrap(), 3.3);
    /// assert_eq!(val.incrbyfloat(3.3).unwrap(), 6.6);
    /// assert_eq!(val.get().unwrap(), b"6.6".to_vec());
    /// ```
    pub fn incrbyfloat(&mut self, incr: f64) -> Result<f64, OperationError> {
        match self {
            Value::Nil => {
                *self = Value::String(ValueString::Data(format!("{}", incr).into_bytes()));
                Ok(incr)
            }
            Value::String(value) => value.incrbyfloat(incr),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Gets the bytes in a range in the string.
    /// Negative positions starts from the end.
    /// If the stop index is lower than the start index, it returns and empty vec.
    /// Fails if the value is not a string.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.getrange(0, -1).unwrap(), b"".to_vec());
    /// val.set(b"foobarbaz".to_vec()).unwrap();
    /// assert_eq!(val.getrange(3, -4).unwrap(), b"bar".to_vec());
    /// assert_eq!(val.getrange(10, 1).unwrap(), b"".to_vec());
    /// ```
    pub fn getrange(&self, start: i64, stop: i64) -> Result<Vec<u8>, OperationError> {
        match self {
            Value::Nil => Ok(Vec::new()),
            Value::String(value) => Ok(value.getrange(start, stop)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Updates the string value overwriting with provided data from the index.
    /// If the string was shorter than the index, it is filled with null bytes.
    /// Fails if the value is not a string.
    /// Returns the number of bytes with the new data appended
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.setrange(1, vec![1, 2]).unwrap(), 3);
    /// assert_eq!(val.get().unwrap(), vec![0, 1, 2]);
    /// ```
    pub fn setrange(&mut self, index: usize, data: Vec<u8>) -> Result<usize, OperationError> {
        match self {
            Value::Nil => *self = Value::String(ValueString::Data(Vec::new())),
            Value::String(_) => (),
            _ => return Err(OperationError::WrongTypeError),
        };

        match self {
            Value::String(value) => Ok(value.setrange(index, data)),
            _ => panic!("Expected value to be a string"),
        }
    }

    /// Updates the string value turning the bit in the index on or off.
    /// Negative index starts from the end.
    /// If the string had less bits than the index, it is filled with null bytes.
    /// Fails if the value is not a string.
    /// Returns the previous bit value.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.setbit(0, true).unwrap(), false);
    /// assert_eq!(val.get().unwrap(), vec![128]);
    /// ```
    pub fn setbit(&mut self, bitoffset: usize, on: bool) -> Result<bool, OperationError> {
        match self {
            Value::Nil => *self = Value::String(ValueString::Data(Vec::new())),
            Value::String(_) => (),
            _ => return Err(OperationError::WrongTypeError),
        }

        match self {
            Value::String(value) => Ok(value.setbit(bitoffset, on)),
            _ => panic!("Value must be a string"),
        }
    }

    /// Gets the status of a given bit by index.
    /// Negative index starts from the end.
    /// Fails if the value is not a string.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.getbit(0).unwrap(), false);
    /// val.setbit(0, true).unwrap();
    /// assert_eq!(val.getbit(1).unwrap(), false);
    /// assert_eq!(val.getbit(0).unwrap(), true);
    /// ```
    pub fn getbit(&self, bitoffset: usize) -> Result<bool, OperationError> {
        match self {
            Value::Nil => Ok(false),
            Value::String(value) => Ok(value.getbit(bitoffset)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Adds elements to an HyperLogLog. Returns true if the element was
    /// modified.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.pfadd(vec![vec![1], vec![2], vec![3]]).unwrap(), true);
    /// assert_eq!(val.pfadd(vec![vec![1], vec![2], vec![3]]).unwrap(), false);
    /// ```
    pub fn pfadd(&mut self, data: Vec<Vec<u8>>) -> Result<bool, OperationError> {
        match self {
            Value::Nil => *self = Value::String(ValueString::Data(Vec::new())),
            Value::String(_) => (),
            _ => return Err(OperationError::WrongTypeError),
        };

        match self {
            Value::String(value) => value.pfadd(data),
            _ => panic!("Expected value to be a string"),
        }
    }

    /// Count the elements in an HyperLogLog.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.pfadd(vec![vec![1], vec![2], vec![3]]).unwrap(), true);
    /// assert_eq!(val.pfcount().unwrap(), 3);
    /// ```
    pub fn pfcount(&self) -> Result<usize, OperationError> {
        match self {
            Value::Nil => Ok(0),
            Value::String(s) => s.pfcount(),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Merge multiple hyperloglog into this value.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val1 = Value::Nil;
    /// assert_eq!(val1.pfadd(vec![vec![1], vec![2], vec![3]]).unwrap(), true);
    /// let val2 = Value::Nil;
    /// let mut val3 = Value::Nil;
    /// assert_eq!(val3.pfadd(vec![vec![1], vec![2], vec![4]]).unwrap(), true);
    /// let mut val = Value::Nil;
    /// assert!(val.pfmerge(vec![&val1, &val2, &val3]).is_ok());
    /// assert_eq!(val.pfcount().unwrap(), 4);
    /// ```
    pub fn pfmerge(&mut self, values: Vec<&Value>) -> Result<(), OperationError> {
        let mut values_string = Vec::with_capacity(values.len());
        for v in values {
            match v {
                Value::Nil => (),
                Value::String(s) => values_string.push(s),
                _ => return Err(OperationError::WrongTypeError),
            }
        }

        if values_string.is_empty() {
            return Ok(());
        }

        match self {
            Value::Nil => *self = Value::String(ValueString::Data(Vec::new())),
            Value::String(_) => (),
            _ => return Err(OperationError::WrongTypeError),
        };

        match self {
            Value::String(value) => value.pfmerge(values_string),
            _ => panic!("Expected value to be a string"),
        }
    }

    /// Adds an element to a list.
    /// Returns the size of the list.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.push(vec![1], true).unwrap(), 1);
    /// assert_eq!(val.push(vec![2], true).unwrap(), 2);
    /// assert_eq!(val.push(vec![0], false).unwrap(), 3);
    /// assert_eq!(val.lrange(0, -1).unwrap(), vec![&[0][..], &[1][..], &[2][..]]);
    /// ```
    pub fn push(&mut self, el: Vec<u8>, right: bool) -> Result<usize, OperationError> {
        Ok(match self {
            Value::Nil => {
                let mut list = ValueList::new();
                list.push(el, right);
                *self = Value::List(list);
                1
            }
            Value::List(list) => {
                list.push(el, right);
                list.llen()
            }
            _ => return Err(OperationError::WrongTypeError),
        })
    }

    /// Takes an element from a list.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// val.push(vec![1], true).unwrap();
    /// val.push(vec![2], true).unwrap();
    /// val.push(vec![3], true).unwrap();
    /// assert_eq!(val.pop(true).unwrap(), Some(vec![3]));
    /// assert_eq!(val.pop(false).unwrap(), Some(vec![1]));
    /// assert_eq!(val.pop(true).unwrap(), Some(vec![2]));
    /// assert_eq!(val.pop(true).unwrap(), None);
    /// ```
    pub fn pop(&mut self, right: bool) -> Result<Option<Vec<u8>>, OperationError> {
        Ok(match self {
            Value::Nil => None,
            Value::List(list) => list.pop(right),
            _ => return Err(OperationError::WrongTypeError),
        })
    }

    /// Gets an element from a list.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// val.push(vec![1], true).unwrap();
    /// val.push(vec![2], true).unwrap();
    /// assert_eq!(val.lindex(0).unwrap(), Some(&[1][..]));
    /// assert_eq!(val.lindex(1).unwrap(), Some(&[2][..]));
    /// assert_eq!(val.lindex(2).unwrap(), None);
    /// ```
    pub fn lindex(&self, index: i64) -> Result<Option<&[u8]>, OperationError> {
        match self {
            Value::List(value) => Ok(value.lindex(index)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Insert an element in a list `before` or after a `pivot`
    /// Returns the new length of the list, or None if the pivot was not found.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// val.push(vec![1], true).unwrap();
    /// val.push(vec![3], true).unwrap();
    /// assert_eq!(val.linsert(true, vec![3], vec![2]).unwrap(), Some(3));
    /// assert_eq!(val.linsert(true, vec![5], vec![4]).unwrap(), None);
    /// assert_eq!(val.lrange(0, -1).unwrap(), vec![&[1][..], &[2][..], &[3][..]]);
    /// ```
    pub fn linsert(
        &mut self,
        before: bool,
        pivot: Vec<u8>,
        newvalue: Vec<u8>,
    ) -> Result<Option<usize>, OperationError> {
        match self {
            Value::Nil => Ok(None),
            Value::List(value) => Ok(value.linsert(before, pivot, newvalue)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Gets the length of the list.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.llen().unwrap(), 0);
    /// val.push(vec![1], true).unwrap();
    /// assert_eq!(val.llen().unwrap(), 1);
    /// val.push(vec![2], true).unwrap();
    /// assert_eq!(val.llen().unwrap(), 2);
    /// ```
    pub fn llen(&self) -> Result<usize, OperationError> {
        match self {
            Value::Nil => Ok(0),
            Value::List(value) => Ok(value.llen()),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Gets the elements in the list in a range.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// val.push(vec![1], true).unwrap();
    /// val.push(vec![2], true).unwrap();
    /// val.push(vec![3], true).unwrap();
    /// assert_eq!(val.lrange(1, 1).unwrap(), vec![&[2][..]]);
    /// assert_eq!(val.lrange(1, -1).unwrap(), vec![&[2][..], &[3][..]]);
    /// ```
    pub fn lrange(&self, start: i64, stop: i64) -> Result<Vec<&[u8]>, OperationError> {
        match self {
            Value::Nil => Ok(Vec::new()),
            Value::List(value) => Ok(value.lrange(start, stop)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Remove up to `limit` elements, starting from either side, that match
    /// `newvalue`.
    /// Returns the number of removed elements.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.lrem(false, 2, vec![3]).unwrap(), 0);
    /// val.push(vec![2], true).unwrap();
    /// val.push(vec![3], true).unwrap();
    /// val.push(vec![2], true).unwrap();
    /// val.push(vec![1], true).unwrap();
    /// val.push(vec![3], true).unwrap();
    /// val.push(vec![3], true).unwrap();
    /// assert_eq!(val.lrem(false, 2, vec![3]).unwrap(), 2);
    /// assert_eq!(val.lrange(0, -1).unwrap(), vec![&[2][..], &[3][..], &[2][..], &[1][..]]);
    /// assert_eq!(val.lrem(true, 1, vec![2]).unwrap(), 1);
    /// assert_eq!(val.lrange(0, -1).unwrap(), vec![&[3][..], &[2][..], &[1][..]]);
    /// assert_eq!(val.lrem(true, 1, vec![4]).unwrap(), 0);
    /// assert_eq!(val.lrange(0, -1).unwrap(), vec![&[3][..], &[2][..], &[1][..]]);
    /// ```
    pub fn lrem(
        &mut self,
        left: bool,
        limit: usize,
        newvalue: Vec<u8>,
    ) -> Result<usize, OperationError> {
        match self {
            Value::Nil => Ok(0),
            Value::List(value) => Ok(value.lrem(left, limit, newvalue)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Updates the `index`-th element in the list to `newvalue`.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// val.push(vec![1], true).unwrap();
    /// val.push(vec![3], true).unwrap();
    /// val.push(vec![3], true).unwrap();
    /// val.lset(1, vec![2]).unwrap();
    /// assert_eq!(val.lrange(0, -1).unwrap(), vec![&[1][..], &[2][..], &[3][..]]);
    /// ```
    pub fn lset(&mut self, index: i64, newvalue: Vec<u8>) -> Result<(), OperationError> {
        match self {
            Value::Nil => Err(OperationError::UnknownKeyError),
            Value::List(value) => value.lset(index, newvalue),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Truncates the list to be just the elements between `start` and `stop`.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// val.push(vec![1], true).unwrap();
    /// val.push(vec![2], true).unwrap();
    /// val.push(vec![3], true).unwrap();
    /// val.push(vec![4], true).unwrap();
    /// val.ltrim(1, -2).unwrap();
    /// assert_eq!(val.lrange(0, -1).unwrap(), vec![&[2][..], &[3][..]]);
    /// ```
    pub fn ltrim(&mut self, start: i64, stop: i64) -> Result<(), OperationError> {
        match self {
            Value::List(value) => value.ltrim(start, stop),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Adds an element to a set.
    /// Returns true if the element was inserted or false if was already in the set.
    /// `set_max_intset_entries` is the maximum number of elements a set can
    /// have while keeping an internal intset encoding.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.sadd(vec![1], 3).unwrap(), true);
    /// assert_eq!(val.sadd(vec![2], 3).unwrap(), true);
    /// assert_eq!(val.sadd(vec![3], 3).unwrap(), true);
    /// assert_eq!(val.sadd(vec![1], 3).unwrap(), false);
    /// ```
    pub fn sadd(
        &mut self,
        el: Vec<u8>,
        set_max_intset_entries: usize,
    ) -> Result<bool, OperationError> {
        match self {
            Value::Nil => {
                let mut value = ValueSet::new();
                value.sadd(el, set_max_intset_entries);
                *self = Value::Set(value);
                Ok(true)
            }
            Value::Set(value) => Ok(value.sadd(el, set_max_intset_entries)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Removes an element from a set.
    /// Returns true if the element was present.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.srem(&vec![0]).unwrap(), false);
    /// val.sadd(vec![1], 3).unwrap();
    /// val.sadd(vec![2], 3).unwrap();
    /// val.sadd(vec![3], 3).unwrap();
    /// assert_eq!(val.srem(&vec![3]).unwrap(), true);
    /// assert_eq!(val.srem(&vec![3]).unwrap(), false);
    /// assert_eq!(val.srem(&vec![4]).unwrap(), false);
    /// ```
    pub fn srem(&mut self, el: &[u8]) -> Result<bool, OperationError> {
        match self {
            Value::Nil => Ok(false),
            Value::Set(value) => Ok(value.srem(el)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Checks if an element is in the set.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.sismember(&vec![0]).unwrap(), false);
    /// val.sadd(vec![1], 3).unwrap();
    /// val.sadd(vec![2], 3).unwrap();
    /// val.sadd(vec![3], 3).unwrap();
    /// assert_eq!(val.sismember(&vec![1]).unwrap(), true);
    /// assert_eq!(val.sismember(&vec![4]).unwrap(), false);
    /// ```
    pub fn sismember(&self, el: &[u8]) -> Result<bool, OperationError> {
        match self {
            Value::Nil => Ok(false),
            Value::Set(value) => Ok(value.sismember(el)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Returns the number of elements in a set.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.scard().unwrap(), 0);
    /// val.sadd(vec![1], 3).unwrap();
    /// assert_eq!(val.scard().unwrap(), 1);
    /// val.sadd(vec![2], 3).unwrap();
    /// assert_eq!(val.scard().unwrap(), 2);
    /// ```
    pub fn scard(&self) -> Result<usize, OperationError> {
        match self {
            Value::Nil => Ok(0),
            Value::Set(value) => Ok(value.scard()),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Returns all elements in the set.
    ///
    /// # Examples
    /// ```
    /// use std::collections::HashSet;
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// val.sadd(vec![1], 3).unwrap();
    /// val.sadd(vec![2], 3).unwrap();
    /// val.sadd(vec![3], 3).unwrap();
    /// let set1 = val.smembers().unwrap().into_iter().collect::<HashSet<_>>();
    /// let set2 = vec![vec![1], vec![2], vec![3]].into_iter().collect::<HashSet<_>>();
    /// assert_eq!(set1, set2);
    /// ```
    pub fn smembers(&self) -> Result<Vec<Vec<u8>>, OperationError> {
        match self {
            Value::Nil => Ok(vec![]),
            Value::Set(value) => Ok(value.smembers()),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Returns `count` random elements from the set. When `allow_duplicates`
    /// is false, it will return up to the number of unique elements in the set.
    ///
    /// # Examples
    /// ```
    /// use std::collections::HashSet;
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// val.sadd(vec![1], 3).unwrap();
    /// val.sadd(vec![2], 3).unwrap();
    /// val.sadd(vec![3], 3).unwrap();
    /// let elements = val.srandmember(10, false).unwrap();
    /// assert_eq!(elements.len(), 3);
    /// let set1 = elements.into_iter().collect::<HashSet<_>>();
    /// let set2 = vec![vec![1], vec![2], vec![3]].into_iter().collect::<HashSet<_>>();
    /// assert_eq!(set1, set2);
    /// ```
    ///
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// val.sadd(vec![1], 3).unwrap();
    /// val.sadd(vec![2], 3).unwrap();
    /// val.sadd(vec![3], 3).unwrap();
    /// let elements = val.srandmember(10, true).unwrap();
    /// assert_eq!(elements.len(), 10);
    /// ```
    pub fn srandmember(
        &self,
        count: usize,
        allow_duplicates: bool,
    ) -> Result<Vec<Vec<u8>>, OperationError> {
        match self {
            Value::Nil => Ok(Vec::new()),
            Value::Set(value) => Ok(value.srandmember(count, allow_duplicates)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Removes and returns `count` elements from the set. If `count` is larger
    /// than the number of elements in the set, it removes up to that number.
    ///
    /// # Examples
    /// ```
    /// use std::collections::HashSet;
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// val.sadd(vec![1], 3).unwrap();
    /// val.sadd(vec![2], 3).unwrap();
    /// val.sadd(vec![3], 3).unwrap();
    /// let elements1 = val.spop(2).unwrap();
    /// assert_eq!(elements1.len(), 2);
    /// let mut set1 = elements1.into_iter().collect::<HashSet<_>>();
    /// let elements2 = val.spop(10).unwrap();
    /// assert_eq!(elements2.len(), 1);
    /// set1.extend(elements2.into_iter());
    /// let set2 = vec![vec![1], vec![2], vec![3]].into_iter().collect::<HashSet<_>>();
    /// assert_eq!(set1, set2);
    /// ```
    pub fn spop(&mut self, count: usize) -> Result<Vec<Vec<u8>>, OperationError> {
        match self {
            Value::Nil => Ok(Vec::new()),
            Value::Set(value) => Ok(value.spop(count)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Returns the elements in one set that are not present in another sets.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use std::collections::HashSet;
    ///
    /// let mut val1 = Value::Nil;
    /// val1.sadd(vec![1], 3).unwrap();
    /// val1.sadd(vec![2], 3).unwrap();
    /// val1.sadd(vec![3], 3).unwrap();
    /// let mut val2 = Value::Nil;
    /// val2.sadd(vec![1], 3).unwrap();
    /// let mut val3 = Value::Nil;
    /// val3.sadd(vec![2], 3).unwrap();
    /// let set = vec![vec![3]].into_iter().collect::<HashSet<_>>();
    /// assert_eq!(val1.sdiff(&vec![&val2, &val3]).unwrap(), set);
    /// ```
    pub fn sdiff(&self, set_values: &[&Value]) -> Result<HashSet<Vec<u8>>, OperationError> {
        match self {
            Value::Nil => Ok(HashSet::new()),
            Value::Set(value) => {
                let emptyset = ValueSet::new();
                let sets = get_set_list(set_values, &emptyset)?;
                Ok(value.sdiff(sets))
            }
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Returns the elements in one set that are present in another sets.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use std::collections::HashSet;
    ///
    /// let mut val1 = Value::Nil;
    /// val1.sadd(vec![1], 3).unwrap();
    /// val1.sadd(vec![2], 3).unwrap();
    /// val1.sadd(vec![3], 3).unwrap();
    /// let mut val2 = Value::Nil;
    /// val2.sadd(vec![1], 3).unwrap();
    /// val2.sadd(vec![2], 3).unwrap();
    /// let mut val3 = Value::Nil;
    /// val3.sadd(vec![1], 3).unwrap();
    /// val3.sadd(vec![3], 3).unwrap();
    /// let set = vec![vec![1]].into_iter().collect::<HashSet<_>>();
    /// assert_eq!(val1.sinter(&vec![&val2, &val3]).unwrap(), set);
    /// ```
    pub fn sinter(&self, set_values: &[&Value]) -> Result<HashSet<Vec<u8>>, OperationError> {
        match self {
            Value::Nil => Ok(HashSet::new()),
            Value::Set(value) => {
                let emptyset = ValueSet::new();
                let sets = get_set_list(set_values, &emptyset)?;
                Ok(value.sinter(sets))
            }
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Returns the elements in any of the sets
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use std::collections::HashSet;
    ///
    /// let mut val1 = Value::Nil;
    /// val1.sadd(vec![1], 3).unwrap();
    /// let mut val2 = Value::Nil;
    /// val2.sadd(vec![1], 3).unwrap();
    /// val2.sadd(vec![2], 3).unwrap();
    /// let mut val3 = Value::Nil;
    /// val3.sadd(vec![1], 3).unwrap();
    /// val3.sadd(vec![3], 3).unwrap();
    /// let set = vec![vec![1], vec![2], vec![3]].into_iter().collect::<HashSet<_>>();
    /// assert_eq!(val1.sunion(&vec![&val2, &val3]).unwrap(), set);
    /// ```
    pub fn sunion(&self, set_values: &[&Value]) -> Result<HashSet<Vec<u8>>, OperationError> {
        let emptyset = ValueSet::new();
        let sets = get_set_list(set_values, &emptyset)?;

        match self {
            Value::Nil => Ok(emptyset.sunion(sets)),
            Value::Set(value) => Ok(value.sunion(sets)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Turns the value into a set using an existing HashSet.
    ///
    /// # Examples
    /// ```
    /// use std::collections::HashSet;
    /// use database::Value;
    ///
    /// let mut val1 = Value::Nil;
    /// val1.create_set(vec![vec![1], vec![2], vec![3]].into_iter().collect::<HashSet<_>>());
    /// assert_eq!(val1.scard().unwrap(), 3);
    /// ```
    pub fn create_set(&mut self, set: HashSet<Vec<u8>>) {
        *self = Value::Set(ValueSet::create_with_hashset(set));
    }

    /// Removes an element from a sorted set. Returns true if the element existed.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.zrem(vec![1]).unwrap(), false);
    /// val.zadd(1.0, vec![1], false, false, false, false).unwrap();
    /// val.zadd(2.0, vec![2], false, false, false, false).unwrap();
    /// val.zadd(3.0, vec![3], false, false, false, false).unwrap();
    /// assert_eq!(val.zrem(vec![1]).unwrap(), true);
    /// assert_eq!(val.zrem(vec![1]).unwrap(), false);
    /// assert_eq!(val.zcard().unwrap(), 2);
    /// ```
    pub fn zrem(&mut self, member: Vec<u8>) -> Result<bool, OperationError> {
        match self {
            Value::Nil => Ok(false),
            Value::SortedSet(value) => Ok(value.zrem(member)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Adds an element to a sorted set.
    /// If `nx` is true, it only adds new element.
    /// If `xx` is true, it only updates an existing element.
    /// If `ch` is true, the return value is whether the element was modified,
    /// otherwise the return value is whether the element was created.
    /// When `incr` is true, the element existing score is added to the provided one,
    /// otherwise it is replaced.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.zadd(1.0, vec![1], false, false, false, false).unwrap(), true);
    /// assert_eq!(val.zscore(vec![1]).unwrap(), Some(1.0));
    /// ```
    ///
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.zadd(1.0, vec![1], true, false, false, false).unwrap(), true);
    /// assert_eq!(val.zscore(vec![1]).unwrap(), Some(1.0));
    /// assert_eq!(val.zadd(2.0, vec![1], true, false, false, false).unwrap(), false);
    /// assert_eq!(val.zscore(vec![1]).unwrap(), Some(1.0));
    /// ```
    ///
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.zadd(1.0, vec![1], false, true, false, false).unwrap(), false);
    /// assert_eq!(val.zscore(vec![1]).unwrap(), None);
    /// assert_eq!(val.zadd(1.0, vec![1], false, false, false, false).unwrap(), true);
    /// assert_eq!(val.zadd(2.0, vec![1], false, true, false, false).unwrap(), false);
    /// assert_eq!(val.zscore(vec![1]).unwrap(), Some(2.0));
    /// ```
    ///
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.zadd(1.0, vec![1], false, false, true, false).unwrap(), true);
    /// assert_eq!(val.zadd(2.0, vec![1], false, false, true, false).unwrap(), true);
    /// assert_eq!(val.zscore(vec![1]).unwrap(), Some(2.0));
    /// ```
    ///
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.zadd(1.0, vec![1], false, false, false, true).unwrap(), true);
    /// assert_eq!(val.zadd(1.0, vec![1], false, false, false, true).unwrap(), false);
    /// assert_eq!(val.zscore(vec![1]).unwrap(), Some(2.0));
    /// ```
    pub fn zadd(
        &mut self,
        s: f64,
        el: Vec<u8>,
        nx: bool,
        xx: bool,
        ch: bool,
        incr: bool,
    ) -> Result<bool, OperationError> {
        match self {
            Value::Nil => {
                if xx {
                    return Ok(false);
                }
                let mut value = ValueSortedSet::new();
                let r = value.zadd(s, el, nx, xx, ch, incr, false)?;
                *self = Value::SortedSet(value);
                Ok(r)
            }
            Value::SortedSet(value) => value.zadd(s, el, nx, xx, ch, incr, false),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Returns the number of elements in a sorted set.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.zcard().unwrap(), 0);
    /// val.zadd(1.0, vec![1], false, false, false, false).unwrap();
    /// assert_eq!(val.zcard().unwrap(), 1);
    /// val.zadd(2.0, vec![2], false, false, false, false).unwrap();
    /// assert_eq!(val.zcard().unwrap(), 2);
    /// ```
    pub fn zcard(&self) -> Result<usize, OperationError> {
        match self {
            Value::Nil => Ok(0),
            Value::SortedSet(value) => Ok(value.zcard()),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Returns the score of a given element.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.zscore(vec![1]).unwrap(), None);
    /// val.zadd(1.0, vec![1], false, false, false, false).unwrap();
    /// assert_eq!(val.zscore(vec![1]).unwrap(), Some(1.0));
    /// ```
    pub fn zscore(&self, element: Vec<u8>) -> Result<Option<f64>, OperationError> {
        match self {
            Value::Nil => Ok(None),
            Value::SortedSet(value) => Ok(value.zscore(&element)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Increments the score of an element. It creates the element if it was not
    /// already in the sorted set. Returns the new score.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.zincrby(1.0, vec![1]).unwrap(), 1.0);
    /// assert_eq!(val.zincrby(1.0, vec![1]).unwrap(), 2.0);
    /// ```
    pub fn zincrby(&mut self, increment: f64, member: Vec<u8>) -> Result<f64, OperationError> {
        match self {
            Value::Nil => match self.zadd(increment, member, false, false, false, false) {
                Ok(_) => Ok(increment),
                Err(err) => Err(err),
            },
            Value::SortedSet(value) => value.zincrby(increment, member),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Counts the number of elements in a sorted set within a score range
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use std::collections::Bound;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.zcount(Bound::Unbounded, Bound::Unbounded).unwrap(), 0);
    /// val.zadd(1.0, vec![1], false, false, false, false).unwrap();
    /// val.zadd(2.0, vec![2], false, false, false, false).unwrap();
    /// val.zadd(3.0, vec![3], false, false, false, false).unwrap();
    /// assert_eq!(val.zcount(Bound::Included(2.0), Bound::Excluded(3.0)).unwrap(), 1);
    /// assert_eq!(val.zcount(Bound::Unbounded, Bound::Unbounded).unwrap(), 3);
    /// ```
    pub fn zcount(&self, min: Bound<f64>, max: Bound<f64>) -> Result<usize, OperationError> {
        match self {
            Value::Nil => Ok(0),
            Value::SortedSet(value) => Ok(value.zcount(min, max)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Counts the number of elements in a sorted set within a lex range
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use std::collections::Bound;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.zlexcount(Bound::Unbounded, Bound::Unbounded).unwrap(), 0);
    /// val.zadd(0.0, vec![1], false, false, false, false).unwrap();
    /// val.zadd(0.0, vec![2], false, false, false, false).unwrap();
    /// val.zadd(0.0, vec![3], false, false, false, false).unwrap();
    /// assert_eq!(val.zlexcount(Bound::Included(vec![2]), Bound::Excluded(vec![3])).unwrap(), 1);
    /// assert_eq!(val.zlexcount(Bound::Unbounded, Bound::Unbounded).unwrap(), 3);
    /// ```
    pub fn zlexcount(
        &self,
        min: Bound<Vec<u8>>,
        max: Bound<Vec<u8>>,
    ) -> Result<usize, OperationError> {
        match self {
            Value::Nil => Ok(0),
            Value::SortedSet(value) => Ok(value.zlexcount(min, max)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Returns the elements in a position range. If `withscores` is true, it will also
    /// include their scores' ASCII representation. If `rev` is true, it counts
    /// the positions from the end.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// val.zadd(1.0, vec![1], false, false, false, false).unwrap();
    /// val.zadd(2.0, vec![2], false, false, false, false).unwrap();
    /// val.zadd(3.0, vec![3], false, false, false, false).unwrap();
    /// assert_eq!(val.zrange(0, -1, false, false).unwrap(), vec![
    ///     vec![1],
    ///     vec![2],
    ///     vec![3],
    /// ]);
    /// assert_eq!(val.zrange(0, -1, true, false).unwrap(), vec![
    ///     vec![1],
    ///     b"1".to_vec(),
    ///     vec![2],
    ///     b"2".to_vec(),
    ///     vec![3],
    ///     b"3".to_vec(),
    /// ]);
    /// assert_eq!(val.zrange(0, 0, false, false).unwrap(), vec![
    ///     vec![1],
    /// ]);
    /// assert_eq!(val.zrange(0, 1, false, true).unwrap(), vec![
    ///     vec![3],
    ///     vec![2],
    /// ]);
    /// ```
    pub fn zrange(
        &self,
        start: i64,
        stop: i64,
        withscores: bool,
        rev: bool,
    ) -> Result<Vec<Vec<u8>>, OperationError> {
        match self {
            Value::Nil => Ok(vec![]),
            Value::SortedSet(value) => Ok(value.zrange(start, stop, withscores, rev)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Returns the elements in a score range. If `withscores` is true, it will also
    /// include their scores' ASCII representation.
    /// It will skip the first `offset` elements and return up to `count`.
    /// If `rev` is true, it will reverse the result order and offsets, and
    /// reverse the `min` and `max` parameters.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use std::collections::Bound;
    ///
    /// let mut val = Value::Nil;
    /// val.zadd(1.0, vec![1], false, false, false, false).unwrap();
    /// val.zadd(2.0, vec![2], false, false, false, false).unwrap();
    /// val.zadd(3.0, vec![3], false, false, false, false).unwrap();
    /// assert_eq!(val.zrangebyscore(Bound::Included(2.0), Bound::Unbounded, true, 0, 100, false).unwrap(), vec![
    ///     vec![2],
    ///     b"2".to_vec(),
    ///     vec![3],
    ///     b"3".to_vec(),
    /// ]);
    ///
    /// assert_eq!(val.zrangebyscore(Bound::Unbounded, Bound::Included(2.0), false, 0, 1, true).unwrap(), vec![
    ///     vec![3],
    /// ]);
    /// ```
    pub fn zrangebyscore(
        &self,
        min: Bound<f64>,
        max: Bound<f64>,
        withscores: bool,
        offset: usize,
        count: usize,
        rev: bool,
    ) -> Result<Vec<Vec<u8>>, OperationError> {
        match self {
            Value::Nil => Ok(vec![]),
            Value::SortedSet(value) => {
                Ok(value.zrangebyscore(min, max, withscores, offset, count, rev))
            }
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Returns the elements in a lex range. If `withscores` is true, it will also
    /// include their scores' ASCII representation.
    /// It will skip the first `offset` elements and return up to `count`.
    /// If `rev` is true, it will reverse the result order and offsets, and
    /// reverse the `min` and `max` parameters.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use std::collections::Bound;
    ///
    /// let mut val = Value::Nil;
    /// val.zadd(0.0, vec![1], false, false, false, false).unwrap();
    /// val.zadd(0.0, vec![2], false, false, false, false).unwrap();
    /// val.zadd(0.0, vec![3], false, false, false, false).unwrap();
    /// assert_eq!(val.zrangebylex(Bound::Included(vec![2]), Bound::Unbounded, 0, 100, false).unwrap(), vec![
    ///     vec![2],
    ///     vec![3],
    /// ]);
    ///
    /// assert_eq!(val.zrangebylex(Bound::Unbounded, Bound::Included(vec![2]), 0, 1, true).unwrap(), vec![
    ///     vec![3],
    /// ]);
    /// ```
    pub fn zrangebylex(
        &self,
        min: Bound<Vec<u8>>,
        max: Bound<Vec<u8>>,
        offset: usize,
        count: usize,
        rev: bool,
    ) -> Result<Vec<Vec<u8>>, OperationError> {
        match self {
            Value::Nil => Ok(vec![]),
            Value::SortedSet(value) => Ok(value.zrangebylex(min, max, offset, count, rev)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Returns the position in the set for a given element.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// val.zadd(1.0, vec![1], false, false, false, false).unwrap();
    /// val.zadd(2.0, vec![2], false, false, false, false).unwrap();
    /// val.zadd(3.0, vec![3], false, false, false, false).unwrap();
    /// assert_eq!(val.zrank(vec![1]).unwrap(), Some(0));
    /// assert_eq!(val.zrank(vec![2]).unwrap(), Some(1));
    /// assert_eq!(val.zrank(vec![3]).unwrap(), Some(2));
    /// assert_eq!(val.zrank(vec![4]).unwrap(), None);
    /// ```
    pub fn zrank(&self, el: Vec<u8>) -> Result<Option<usize>, OperationError> {
        match self {
            Value::Nil => Ok(None),
            Value::SortedSet(value) => Ok(value.zrank(el)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Removes all elements within a score range. Returns the number of removed elements
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use std::collections::Bound;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.zremrangebyscore(Bound::Unbounded, Bound::Unbounded).unwrap(), 0);
    /// val.zadd(1.0, vec![1], false, false, false, false).unwrap();
    /// val.zadd(2.0, vec![2], false, false, false, false).unwrap();
    /// val.zadd(3.0, vec![3], false, false, false, false).unwrap();
    /// assert_eq!(val.zremrangebyscore(Bound::Included(2.0), Bound::Excluded(3.0)).unwrap(), 1);
    /// assert_eq!(val.zremrangebyscore(Bound::Included(2.0), Bound::Excluded(3.0)).unwrap(), 0);
    /// assert_eq!(val.zcard().unwrap(), 2);
    /// ```
    pub fn zremrangebyscore(
        &mut self,
        min: Bound<f64>,
        max: Bound<f64>,
    ) -> Result<usize, OperationError> {
        match self {
            Value::Nil => Ok(0),
            Value::SortedSet(value) => Ok(value.zremrangebyscore(min, max)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Removes all elements within a lex range. Returns the number of removed elements
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use std::collections::Bound;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.zremrangebylex(Bound::Unbounded, Bound::Unbounded).unwrap(), 0);
    /// val.zadd(0.0, vec![1], false, false, false, false).unwrap();
    /// val.zadd(0.0, vec![2], false, false, false, false).unwrap();
    /// val.zadd(0.0, vec![3], false, false, false, false).unwrap();
    /// assert_eq!(val.zremrangebylex(Bound::Included(vec![2]), Bound::Excluded(vec![3])).unwrap(), 1);
    /// assert_eq!(val.zremrangebylex(Bound::Included(vec![2]), Bound::Excluded(vec![3])).unwrap(), 0);
    /// assert_eq!(val.zcard().unwrap(), 2);
    /// ```
    pub fn zremrangebylex(
        &mut self,
        min: Bound<Vec<u8>>,
        max: Bound<Vec<u8>>,
    ) -> Result<usize, OperationError> {
        match self {
            Value::Nil => Ok(0),
            Value::SortedSet(value) => Ok(value.zremrangebylex(min, max)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Removes all elements within a rank range. Returns the number of removed elements
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use std::collections::Bound;
    ///
    /// let mut val = Value::Nil;
    /// assert_eq!(val.zremrangebyrank(0, -1).unwrap(), 0);
    /// val.zadd(1.0, vec![1], false, false, false, false).unwrap();
    /// val.zadd(2.0, vec![2], false, false, false, false).unwrap();
    /// val.zadd(3.0, vec![3], false, false, false, false).unwrap();
    /// assert_eq!(val.zremrangebyrank(1, 1).unwrap(), 1);
    /// assert_eq!(val.zremrangebyrank(1, 1).unwrap(), 1);
    /// assert_eq!(val.zcard().unwrap(), 1);
    /// ```
    pub fn zremrangebyrank(&mut self, start: i64, stop: i64) -> Result<usize, OperationError> {
        match self {
            Value::Nil => Ok(0),
            Value::SortedSet(value) => Ok(value.zremrangebyrank(start, stop)),
            _ => Err(OperationError::WrongTypeError),
        }
    }

    /// Creates a sorted set by merging existing sorted sets.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use database::zset;
    ///
    /// let mut val1 = Value::Nil;
    /// val1.zadd(1.1, vec![1], false, false, false, false).unwrap();
    /// let mut val2 = Value::Nil;
    /// val2.zadd(1.2, vec![1], false, false, false, false).unwrap();
    /// val2.zadd(2.2, vec![2], false, false, false, false).unwrap();
    /// let mut val3 = Value::Nil;
    /// val3.zadd(1.3, vec![1], false, false, false, false).unwrap();
    /// val3.zadd(3.3, vec![3], false, false, false, false).unwrap();
    /// let mut val4 = Value::Nil;
    /// val4 = val4.zunion(&vec![&val1, &val2, &val3], None, zset::Aggregate::Sum).unwrap();
    /// assert_eq!(val4.zcard().unwrap(), 3);
    /// assert!(((val4.zscore(vec![1]).unwrap().unwrap() - 3.6)).abs() < 0.01);
    /// assert!(((val4.zscore(vec![2]).unwrap().unwrap() - 2.2)).abs() < 0.01);
    /// assert!(((val4.zscore(vec![3]).unwrap().unwrap() - 3.3)).abs() < 0.01);
    /// ```
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use database::zset;
    ///
    /// let mut val1 = Value::Nil;
    /// val1.zadd(1.1, vec![1], false, false, false, false).unwrap();
    /// let mut val2 = Value::Nil;
    /// val2.zadd(1.2, vec![1], false, false, false, false).unwrap();
    /// val2.zadd(2.2, vec![2], false, false, false, false).unwrap();
    /// let mut val3 = Value::Nil;
    /// val3.zadd(1.3, vec![1], false, false, false, false).unwrap();
    /// val3.zadd(3.3, vec![3], false, false, false, false).unwrap();
    /// let mut val4 = Value::Nil;
    /// val4 = val4.zunion(&vec![&val1, &val2, &val3], Some(vec![1.0, 2.0, 3.0]), zset::Aggregate::Min).unwrap();
    /// assert_eq!(val4.zcard().unwrap(), 3);
    /// assert!(((val4.zscore(vec![1]).unwrap().unwrap() - 1.1)).abs() < 0.01);
    /// assert!(((val4.zscore(vec![2]).unwrap().unwrap() - 4.4)).abs() < 0.01);
    /// assert!(((val4.zscore(vec![3]).unwrap().unwrap() - 9.9)).abs() < 0.01);
    /// ```
    pub fn zunion(
        &self,
        zset_values: &[&Value],
        weights: Option<Vec<f64>>,
        aggregate: zset::Aggregate,
    ) -> Result<Value, OperationError> {
        let emptyzset = ValueSortedSet::new();
        let zsets = get_zset_list(zset_values, &emptyzset)?;

        let mut value = ValueSortedSet::new();
        value.zunion(zsets, weights, aggregate);
        Ok(Value::SortedSet(value))
    }

    /// Creates a new sorted set with the intersection of existing sorted sets.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use database::zset;
    ///
    /// let mut val1 = Value::Nil;
    /// val1.zadd(1.1, vec![1], false, false, false, false).unwrap();
    /// let mut val2 = Value::Nil;
    /// val2.zadd(1.2, vec![1], false, false, false, false).unwrap();
    /// val2.zadd(2.2, vec![2], false, false, false, false).unwrap();
    /// let mut val3 = Value::Nil;
    /// val3.zadd(1.3, vec![1], false, false, false, false).unwrap();
    /// val3.zadd(3.3, vec![3], false, false, false, false).unwrap();
    /// let mut val4 = Value::Nil;
    /// val4 = val4.zinter(&vec![&val1, &val2, &val3], None, zset::Aggregate::Sum).unwrap();
    /// assert_eq!(val4.zcard().unwrap(), 1);
    /// assert!(((val4.zscore(vec![1]).unwrap().unwrap() - 3.6)).abs() < 0.01);
    /// ```
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    /// use database::zset;
    ///
    /// let mut val1 = Value::Nil;
    /// val1.zadd(1.1, vec![1], false, false, false, false).unwrap();
    /// let mut val2 = Value::Nil;
    /// val2.zadd(1.2, vec![1], false, false, false, false).unwrap();
    /// val2.zadd(2.2, vec![2], false, false, false, false).unwrap();
    /// let mut val3 = Value::Nil;
    /// val3.zadd(1.3, vec![1], false, false, false, false).unwrap();
    /// val3.zadd(3.3, vec![3], false, false, false, false).unwrap();
    /// let mut val4 = Value::Nil;
    /// val4 = val4.zinter(&vec![&val1, &val2, &val3], Some(vec![1.0, 2.0, 3.0]), zset::Aggregate::Min).unwrap();
    /// assert_eq!(val4.zcard().unwrap(), 1);
    /// assert!(((val4.zscore(vec![1]).unwrap().unwrap() - 1.1)).abs() < 0.01);
    /// ```
    pub fn zinter(
        &self,
        zset_values: &[&Value],
        weights: Option<Vec<f64>>,
        aggregate: zset::Aggregate,
    ) -> Result<Value, OperationError> {
        let emptyzset = ValueSortedSet::new();
        let zsets = get_zset_list(zset_values, &emptyzset)?;

        let mut value = ValueSortedSet::new();
        value.zinter(zsets, weights, aggregate);
        Ok(Value::SortedSet(value))
    }

    /// Serializes and writes into `writer` the object current value.
    /// The serialized version also includes the type, the version and a crc.
    ///
    /// # Examples
    /// ```
    /// use database::Value;
    ///
    /// let mut val = Value::Nil;
    /// val.set(vec![1, 2, 3]).unwrap();
    /// let mut serialized = vec![];
    /// assert_eq!(val.dump(&mut serialized).unwrap(), 15);
    /// assert_eq!(serialized, vec![0, 3, 1, 2, 3, 7, 0, 229, 221, 166, 143, 248, 97, 121, 255]);
    /// ```
    pub fn dump<T: Write>(&self, writer: &mut T) -> Result<usize, OperationError> {
        let mut data = vec![];
        match self {
            Value::Nil => return Ok(0), // maybe panic instead?
            Value::String(s) => s.dump(&mut data)?,
            Value::List(l) => l.dump(&mut data)?,
            Value::Set(s) => s.dump(&mut data)?,
            Value::SortedSet(s) => s.dump(&mut data)?,
        };
        let crc = crc64(0, &*data);
        encode_u64_to_slice_u8(crc, &mut data).unwrap();
        Ok(writer.write(&*data)?)
    }

    pub fn debug_object(&self) -> String {
        match self {
            Value::Nil => "Value at:0x0000000000 refcount:0 encoding:nil serializedlength:0 lru:0 \
                           lru_seconds_idle:0"
                .to_owned(),
            Value::String(s) => s.debug_object(),
            Value::List(l) => l.debug_object(),
            Value::Set(s) => s.debug_object(),
            Value::SortedSet(s) => s.debug_object(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Value::Nil => true,
            Value::String(_) => false,
            Value::List(l) => l.llen() == 0,
            Value::Set(s) => s.scard() == 0,
            Value::SortedSet(s) => s.zcard() == 0,
        }
    }
}

type SenderMap<T> = HashMap<usize, Sender<T>>;

pub struct Database {
    pub config: Config,

    data: Vec<RehashingHashMap<Vec<u8>, Value>>,

    /// Maps a key to an expiration time. Expiration time is in milliseconds.
    data_expiration_ms: Vec<RehashingHashMap<Vec<u8>, i64>>,
    /// Maps a key to a collection of client identifiers.
    /// Every time a key is modified, the watched key client is flushed.
    /// The clients who are subscribed to a key should check whether their id
    /// is still present
    watched_keys: Vec<HashMap<Vec<u8>, HashSet<usize>>>,
    /// Maps a channel to a list of pubsub events listeners.
    /// The `usize` key is used as a client identifier.
    subscribers: HashMap<Vec<u8>, SenderMap<Option<Response>>>,
    /// Maps a pattern to a list of pubsub events listeners.
    /// The `usize` key is used as a client identifier.
    pattern_subscribers: HashMap<Vec<u8>, SenderMap<Option<Response>>>,
    /// Maps a pattern to a list of key listeners. When a key is modified a message
    /// with `true` is published.
    /// The `usize` key is used as a client identifier.
    key_subscribers: Vec<RehashingHashMap<Vec<u8>, SenderMap<bool>>>,
    /// A unique identifier counter to assign to clients
    subscriber_id: usize,
    /// Which database to try to run the active expire cycle next
    active_expire_cycle_db: usize,
    /// Clients who are monitoring commands.
    monitor_senders: Vec<Sender<String>>,
    /// Git version used
    pub git_sha1: &'static str,
    /// Did the code change from the git repository
    pub git_dirty: bool,
    pub version: &'static str,
    pub rustc_version: &'static str,
    /// a random 40 digits hex string
    pub run_id: String,
    /// milliseconds when the database started
    pub start_mstime: i64,
    /// Aof reader/writer
    pub aof: Option<Aof>,
    /// Is it loading data from a file
    pub loading: bool,
}

pub struct Iter<'a> {
    inner: rehashinghashmap::Iter<'a, Vec<u8>, Value>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a Vec<u8>, &'a Value);

    #[inline]
    fn next(&mut self) -> Option<(&'a Vec<u8>, &'a Value)> {
        self.inner.next()
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

macro_rules! random_key {
    ($dict: expr) => {{
        let dict = &$dict;
        let len = dict.len();
        let pos = rand::random::<usize>() % len;
        // FIXME: remove clone
        dict.keys().skip(pos).take(1).next().unwrap().clone()
    }};
}

impl Database {
    /// Creates a new empty `Database` with a mock config.
    pub fn mock() -> Self {
        Database::new(Config::default(0, Logger::new(Level::Warning)))
    }

    /// Creates a new empty `Database`.
    pub fn new(config: Config) -> Self {
        env::set_current_dir(&Path::new(&*config.dir)).unwrap();
        let size = config.databases as usize;
        let mut data = Vec::with_capacity(size);
        let mut data_expiration_ms = Vec::with_capacity(size);
        let mut key_subscribers = Vec::with_capacity(size);
        let mut watched_keys = Vec::with_capacity(size);
        for _ in 0..size {
            data.push(RehashingHashMap::new());
            data_expiration_ms.push(RehashingHashMap::new());
            key_subscribers.push(RehashingHashMap::new());
            watched_keys.push(HashMap::new());
        }
        let aof = if config.appendonly {
            Some(Aof::new(&*config.appendfilename).unwrap())
        } else {
            None
        };

        Database {
            config,
            data,
            data_expiration_ms,
            subscribers: HashMap::new(),
            pattern_subscribers: HashMap::new(),
            key_subscribers,
            subscriber_id: 0,
            watched_keys,
            active_expire_cycle_db: 0,
            monitor_senders: Vec::new(),
            version: "0.0.1",
            rustc_version: "",
            git_sha1: "00000000",
            git_dirty: true,
            run_id: get_random_hex_chars(40),
            start_mstime: mstime(),
            aof,
            loading: false,
        }
    }

    pub fn uptime(&self) -> i64 {
        mstime() - self.start_mstime
    }

    fn is_expired(&self, index: usize, key: &[u8]) -> bool {
        !self.loading
            && match self.data_expiration_ms[index].get(key) {
                Some(t) => t <= &mstime(),
                None => false,
            }
    }

    /// Gets the number of items in a database.
    ///
    /// # Examples
    ///
    /// ```
    /// use database::{Database, Value};
    ///
    /// let mut db = Database::mock();
    ///
    /// assert_eq!(db.dbsize(0), 0);
    /// db.get_or_create(0, &vec![1]).set(vec![1]);
    /// assert_eq!(db.dbsize(0), 1);
    /// ```
    pub fn dbsize(&self, index: usize) -> usize {
        self.data[index].len()
    }

    pub fn db_expire_size(&self, index: usize) -> usize {
        self.data_expiration_ms[index].len()
    }

    /// Gets a value from the database if exists and it is not expired.
    ///
    /// # Examples
    ///
    /// ```
    /// use database::{Database, Value};
    ///
    /// let mut db = Database::mock();
    ///
    /// assert_eq!(db.get(0, &vec![1]), None);
    /// db.get_or_create(0, &vec![1]).set(vec![1]);
    ///
    /// let mut value = Value::Nil;
    /// value.set(vec![1]);
    ///
    /// assert_eq!(db.get(0, &vec![1]), Some(&value));
    /// ```
    pub fn get(&self, index: usize, key: &[u8]) -> Option<&Value> {
        if self.is_expired(index, key) {
            None
        } else {
            self.data[index].get(key)
        }
    }

    /// Gets a mutable reference to a value if exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use database::{Database, Value};
    ///
    /// let mut db = Database::mock();
    ///
    /// assert_eq!(db.get_mut(0, &vec![1]), None);
    /// db.get_or_create(0, &vec![1]).set(vec![1]).unwrap();
    /// db.get_mut(0, &vec![1]).unwrap().set(vec![2]);
    /// ```
    pub fn get_mut(&mut self, index: usize, key: &[u8]) -> Option<&mut Value> {
        if self.is_expired(index, key) {
            self.remove(index, key);
            None
        } else {
            self.data[index].get_mut(key)
        }
    }

    /// Removes and returns a value by key.
    ///
    /// # Examples
    ///
    /// ```
    /// use database::{Database, Value};
    ///
    /// let mut db = Database::mock();
    ///
    /// assert_eq!(db.remove(0, &vec![1]), None);
    /// db.get_or_create(0, &vec![1]).set(vec![1]).unwrap();
    /// assert!(db.remove(0, &vec![1]).is_some());
    /// ```
    pub fn remove(&mut self, index: usize, key: &[u8]) -> Option<Value> {
        let mut r = self.data[index].remove(key);
        if self.is_expired(index, key) {
            r = None;
        }

        self.data_expiration_ms[index].remove(key);
        if self.config.active_rehashing {
            if self.data[index].len() * 10 / 12 < self.data[index].capacity() {
                self.data[index].shrink_to_fit();
            }
            if self.data_expiration_ms[index].len() * 10 / 12
                < self.data_expiration_ms[index].capacity()
            {
                self.data_expiration_ms[index].shrink_to_fit();
            }
            if self.key_subscribers[index].len() * 10 / 12 < self.key_subscribers[index].capacity()
            {
                self.key_subscribers[index].shrink_to_fit();
            }
        }

        r
    }

    /// Sets a key expiration time, in milliseconds.
    pub fn set_msexpiration(&mut self, index: usize, key: Vec<u8>, msexpiration: i64) {
        self.key_updated(index, &key);
        self.data_expiration_ms[index].insert(key, msexpiration);
    }

    /// Gets a key expiration time, in milliseconds.
    pub fn get_msexpiration(&mut self, index: usize, key: &[u8]) -> Option<&i64> {
        self.data_expiration_ms[index].get(key)
    }

    /// Removes a key expiration time.
    pub fn remove_msexpiration(&mut self, index: usize, key: &[u8]) -> Option<i64> {
        self.data_expiration_ms[index].remove(key)
    }

    /// Removes all keys in a database.
    pub fn clear(&mut self, index: usize) {
        // FIXME: remove clone
        let keys = self.watched_keys[index]
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        for key in keys {
            if self.data[index].remove(&key).is_some() {
                self.key_updated(index, &key);
            }
        }
        self.data[index].clear();
        self.data_expiration_ms[index].clear();
    }

    /// Returns a mutable reference to a value for a key. If the value was not
    /// present, it is initialized as Nil.
    ///
    /// # Examples
    ///
    /// ```
    /// use database::{Database, Value};
    ///
    /// let mut db = Database::mock();
    ///
    /// assert_eq!(db.get(0, &vec![1]), None);
    /// db.get_or_create(0, &vec![1]).set(vec![1]).unwrap();
    /// assert_eq!(db.get(0, &vec![1]).unwrap().strlen().unwrap(), 1);
    /// db.get_or_create(0, &vec![1]).append(vec![2]).unwrap();
    /// assert_eq!(db.get(0, &vec![1]).unwrap().strlen().unwrap(), 2);
    /// ```
    pub fn get_or_create(&mut self, index: usize, key: &[u8]) -> &mut Value {
        use std::collections::hash_map::Entry;

        let val = Value::Nil;

        if self.is_expired(index, key) {
            self.remove_msexpiration(index, key);
        }

        match self.data[index].entry(key.to_vec()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(val),
        }
    }

    /// Sets up the hashmap to subscribe clients to a key.
    fn ensure_key_subscribers(&mut self, index: usize, key: &[u8]) {
        if !self.key_subscribers[index].contains_key(key) {
            self.key_subscribers[index].insert(key.to_vec(), HashMap::new());
        }
    }

    /// Subscribes a callback to a key. When the key is modified the callback
    /// is called and automatically unsubscribe.
    pub fn key_subscribe(&mut self, index: usize, key: &[u8], sender: Sender<bool>) -> usize {
        self.ensure_key_subscribers(index, key);
        let key_subscribers = self.key_subscribers[index].get_mut(key).unwrap();
        let subscriber_id = self.subscriber_id;
        key_subscribers.insert(subscriber_id, sender);
        self.subscriber_id += 1;
        subscriber_id
    }

    pub fn key_watch(&mut self, index: usize, key: &[u8], identifier: usize) {
        match self.watched_keys[index].contains_key(key) {
            true => self.watched_keys[index]
                .get_mut(key)
                .unwrap()
                .insert(identifier),
            false => self.watched_keys[index]
                .insert(key.to_vec(), HashSet::from_iter(vec![identifier]))
                .is_some(),
        };
    }

    pub fn key_unwatch(&mut self, index: usize, key: &[u8], identifier: usize) {
        if let Some(s) = self.watched_keys[index].get_mut(key) {
            s.remove(&identifier);
        }
    }

    pub fn key_watch_verify(&self, index: usize, key: &[u8], identifier: usize) -> bool {
        match self.watched_keys[index].get(key) {
            Some(l) => l.contains(&identifier),
            None => false,
        }
    }

    /// Publishes a `true` to all key listeners.
    /// If the value is now empty, it is removed.
    pub fn key_updated(&mut self, index: usize, key: &[u8]) {
        if self.config.active_rehashing {
            self.data[index].rehash();
            self.data_expiration_ms[index].rehash();
            self.key_subscribers[index].rehash();
        }

        let is_empty = match self.data[index].get(key) {
            Some(v) => v.is_empty(),
            None => false,
        };
        if is_empty {
            self.remove(index, key);
        }

        if let Some(callbacks) = self.key_subscribers[index].remove(key) {
            for sender in callbacks.values() {
                let _ = sender.send(true);
            }
        }
        self.watched_keys[index].remove(key);
    }

    /// Sets up the hashmap to subscribe clients to a channel.
    fn ensure_channel(&mut self, channel: &[u8]) {
        if !self.subscribers.contains_key(channel) {
            self.subscribers.insert(channel.to_vec(), HashMap::new());
        }
    }

    /// Subscribes a Sender to a channel. Returns a subscriber_id that can be
    /// used to unsubscribe
    ///
    /// # Examples
    /// ```
    /// use database::Database;
    /// # use database::PubsubEvent;
    /// # use std::sync::mpsc::{channel, TryRecvError};
    ///
    /// let mut db = Database::mock();
    ///
    /// let (tx, rx) = channel();
    /// db.subscribe(vec![1], tx);
    /// db.publish(&vec![1], &vec![0, 1, 2, 3]);
    /// assert_eq!(rx.try_recv().unwrap().unwrap(), PubsubEvent::Message(
    ///     vec![1],
    ///     None,
    ///     vec![0, 1, 2, 3],
    /// ).as_response());
    /// assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
    /// ```
    pub fn subscribe(&mut self, channel: Vec<u8>, sender: Sender<Option<Response>>) -> usize {
        self.ensure_channel(&channel);
        let channelsubscribers = self.subscribers.get_mut(&channel).unwrap();
        let subscriber_id = self.subscriber_id;
        channelsubscribers.insert(subscriber_id, sender);
        self.subscriber_id += 1;
        subscriber_id
    }

    /// Unsubscribes a Sender from a channel.
    /// Returns true if it was subscribed
    ///
    /// # Examples
    /// ```
    /// use database::Database;
    /// # use std::sync::mpsc::{channel, TryRecvError};
    ///
    /// let mut db = Database::mock();
    ///
    /// let (tx, rx) = channel();
    /// let subscriber_id = db.subscribe(vec![1], tx);
    /// assert!(db.unsubscribe(vec![1], subscriber_id));
    /// assert!(!db.unsubscribe(vec![1], subscriber_id));
    /// db.publish(&vec![1], &vec![0, 1, 2, 3]);
    /// assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Disconnected);
    /// ```
    pub fn unsubscribe(&mut self, channel: Vec<u8>, subscriber_id: usize) -> bool {
        if !self.subscribers.contains_key(&channel) {
            return false;
        }
        let channelsubscribers = self.subscribers.get_mut(&channel).unwrap();
        channelsubscribers.remove(&subscriber_id).is_some()
    }

    /// Sets up the hashmap to subscribe clients to a pattern.
    fn pensure_channel(&mut self, pattern: &[u8]) {
        if !self.pattern_subscribers.contains_key(pattern) {
            self.pattern_subscribers
                .insert(pattern.to_vec(), HashMap::new());
        }
    }

    /// Subscribes a Sender to a pattern. Returns a subscriber_id that can be
    /// used to unsubscribe
    ///
    /// # Examples
    /// ```
    /// use database::Database;
    /// # use database::PubsubEvent;
    /// # use std::sync::mpsc::{channel, TryRecvError};
    ///
    /// let mut db = Database::mock();
    ///
    /// let (tx, rx) = channel();
    /// db.psubscribe(b"foo*baz".to_vec(), tx);
    /// db.publish(&b"foobarbaz".to_vec(), &vec![0, 1, 2, 3]);
    /// assert_eq!(rx.try_recv().unwrap().unwrap(), PubsubEvent::Message(
    ///     b"foobarbaz".to_vec(),
    ///     Some(b"foo*baz".to_vec()),
    ///     vec![0, 1, 2, 3],
    /// ).as_response());
    /// assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
    /// ```
    pub fn psubscribe(&mut self, pattern: Vec<u8>, sender: Sender<Option<Response>>) -> usize {
        self.pensure_channel(&pattern);
        let channelsubscribers = self.pattern_subscribers.get_mut(&pattern).unwrap();
        let subscriber_id = self.subscriber_id;
        channelsubscribers.insert(subscriber_id, sender);
        self.subscriber_id += 1;
        subscriber_id
    }

    /// Unsubscribes a Sender from a pattern.
    /// Returns true if it was subscribed
    pub fn punsubscribe(&mut self, pattern: Vec<u8>, subscriber_id: usize) -> bool {
        if !self.pattern_subscribers.contains_key(&pattern) {
            return false;
        }
        let channelsubscribers = self.pattern_subscribers.get_mut(&pattern).unwrap();
        channelsubscribers.remove(&subscriber_id).is_some()
    }

    /// Publishes a message to a channel and all patterns that match the channel name.
    /// Returns the number of recipients who receive the message.
    pub fn publish(&self, channel_name: &[u8], message: &[u8]) -> usize {
        let mut c = 0;
        if let Some(channels) = self.subscribers.get(channel_name) {
            for channel in channels.values() {
                if channel
                    .send(Some(
                        PubsubEvent::Message(channel_name.to_vec(), None, message.to_vec())
                            .as_response(),
                    ))
                    .is_ok()
                {
                    c += 1;
                }
            }
        }

        for (pattern, channels) in self.pattern_subscribers.iter() {
            if glob_match(pattern, channel_name, false) {
                for channel in channels.values() {
                    if channel
                        .send(Some(
                            PubsubEvent::Message(
                                channel_name.to_vec(),
                                Some(pattern.to_vec()),
                                message.to_vec(),
                            )
                            .as_response(),
                        ))
                        .is_ok()
                    {
                        c += 1;
                    }
                }
            }
        }
        c
    }

    /// Removes all data from all databases.
    ///
    /// # Examples
    ///
    /// ```
    /// use database::{Database, Value};
    ///
    /// let mut db = Database::mock();
    ///
    /// assert_eq!(db.get(0, &vec![1]), None);
    /// db.get_or_create(0, &vec![1]).set(vec![1]).unwrap();
    /// db.get_or_create(1, &vec![1]).set(vec![1]).unwrap();
    /// db.clearall();
    /// assert!(!db.get(0, &vec![1]).is_some());
    /// ```
    pub fn clearall(&mut self) {
        for index in 0..(self.config.databases as usize) {
            self.clear(index);
        }
    }

    /// Applies the config command mapping. This mapping allows arbitrary
    /// renaming of commands. If a command is not renamed, it is returned as sent.
    ///
    /// # Examples
    ///
    /// ```
    /// use database::{Database, Value};
    ///
    /// let mut db = Database::mock();
    /// db.config.rename_commands.insert("get".to_owned(), Some("getstring".to_owned()));
    /// db.config.rename_commands.insert("set".to_owned(), None);
    ///
    /// assert_eq!(db.mapped_command(&"get".to_owned()), Some("getstring".to_owned()));
    /// assert_eq!(db.mapped_command(&"set".to_owned()), None);
    /// assert_eq!(db.mapped_command(&"del".to_owned()), Some("del".to_owned()));
    /// ```
    pub fn mapped_command(&self, command: &str) -> Option<String> {
        match self.config.rename_commands.get(command) {
            Some(c) => match c {
                Some(s) => Some(s.clone()),
                None => None,
            },
            None => Some(command.to_owned()),
        }
    }

    /// Iterate over the keys in one database
    pub fn iter_db(&self, dbindex: usize) -> Iter {
        Iter {
            inner: self.data[dbindex].iter(),
        }
    }

    /// Collect all keys from a database matching a pattern.
    pub fn keys(&self, dbindex: usize, pattern: &[u8]) -> Vec<Vec<u8>> {
        let iter = self.iter_db(dbindex);
        let mut responses = Vec::with_capacity(iter.size_hint().1.unwrap_or(1));
        for (k, _) in iter {
            if glob_match(pattern, k, false) {
                responses.push(k.clone())
            }
        }
        responses
    }

    /// Tries to remove items that are already expired.
    pub fn active_expire_cycle(&mut self, duration_ms: i64) {
        let num_dbs = self.data.len();
        let dbs_per_call = num_dbs;
        let start = mstime();
        let mut iteration = 0;

        for _ in 0..dbs_per_call {
            let dbindex = self.active_expire_cycle_db;

            self.active_expire_cycle_db += 1;
            if self.active_expire_cycle_db == num_dbs {
                self.active_expire_cycle_db = 0;
            }

            loop {
                let mut num = self.data_expiration_ms.len();
                if num == 0 {
                    break;
                }

                if num > ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP {
                    num = ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP;
                }

                let mut expired = 0;
                while num > 0 && !self.data_expiration_ms[dbindex].is_empty() {
                    num -= 1;
                    let key = random_key!(self.data_expiration_ms[dbindex]);
                    if self.get_mut(dbindex, &key).is_none() {
                        expired += 1;
                    }
                }

                iteration += 1;
                if (iteration & 16) == 0 {
                    let elapsed = mstime() - start;
                    if elapsed > duration_ms {
                        return;
                    }
                }

                if expired <= ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP / 4 {
                    break;
                }
            }
        }
    }

    pub fn monitor_add(&mut self, sender: Sender<String>) {
        self.monitor_senders.push(sender);
    }

    pub fn log_command(&mut self, dbindex: usize, command: &ParsedCommand, write: bool) {
        // FIXME: unnecessary free/alloc?
        let bcommand = format!("{:?}", command);
        let tmp = self
            .monitor_senders
            .drain(RangeFull)
            .filter(|s| s.send(bcommand.clone()).is_ok())
            .collect::<Vec<_>>();
        self.monitor_senders = tmp;
        if write {
            let mut err = false;
            if let Some(w) = &mut self.aof {
                if let Err(e) = w.write(dbindex, command) {
                    log!(
                        self.config.logger,
                        Warning,
                        "Error writing aof {:?}; stopped writing",
                        e
                    );
                    err = true;
                }
            }
            if err {
                self.aof = None;
            }
        }
    }
}

#[cfg(test)]
mod test_command {
    use std::collections::Bound;
    use std::collections::HashSet;
    use std::i64;
    use std::sync::mpsc::channel;
    use std::usize;

    use util::mstime;

    use config::Config;
    use list::ValueList;
    use logger::{Level, Logger};
    use set::ValueSet;
    use string::ValueString;
    use zset;
    use zset::ValueSortedSet;

    use super::{Database, PubsubEvent, Value};
    use parser::{Argument, ParsedCommand};

    #[test]
    fn lpush() {
        let v1 = vec![1u8, 2, 3, 4];
        let v2 = vec![1u8, 5, 6, 7];
        let mut value = Value::List(ValueList::new());

        value.push(v1.clone(), false).unwrap();
        {
            let list = match &value {
                Value::List(ValueList::Data(l)) => l,
                _ => panic!("Expected list"),
            };
            assert_eq!(list.len(), 1);
            assert_eq!(list.front(), Some(&v1));
        }

        value.push(v2.clone(), false).unwrap();
        {
            let list = match &value {
                Value::List(ValueList::Data(l)) => l,
                _ => panic!("Expected list"),
            };
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

        assert_eq!(value.lindex(0).unwrap(), Some(&v2[..]));
        assert_eq!(value.lindex(1).unwrap(), Some(&v1[..]));
        assert_eq!(value.lindex(2).unwrap(), None);

        assert_eq!(value.lindex(-2).unwrap(), Some(&v2[..]));
        assert_eq!(value.lindex(-1).unwrap(), Some(&v1[..]));
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

        assert_eq!(
            value
                .linsert(true, v2.clone(), v3.clone())
                .unwrap()
                .unwrap(),
            3
        );
        assert_eq!(value.lindex(0).unwrap(), Some(&v1[..]));
        assert_eq!(value.lindex(1).unwrap(), Some(&v3[..]));
        assert_eq!(value.lindex(2).unwrap(), Some(&v2[..]));

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

        assert_eq!(
            value.lrange(-100, 100).unwrap(),
            vec![&v1[..], &v2[..], &v3[..]]
        );
        assert_eq!(value.lrange(0, 1).unwrap(), vec![&v1[..], &v2[..]]);
        assert_eq!(value.lrange(0, 0).unwrap(), vec![&v1[..]]);
        assert_eq!(value.lrange(1, -1).unwrap(), vec![&v2[..], &v3[..]]);
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
            let list = match &value {
                Value::List(ValueList::Data(l)) => l,
                _ => panic!("Expected list"),
            };
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
            let list = match &value {
                Value::List(ValueList::Data(l)) => l,
                _ => panic!("Expected list"),
            };
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
        assert_eq!(
            value.lrange(0, -1).unwrap(),
            vec![&v1[..], &v4[..], &v3[..]]
        );
        assert_eq!(value.lset(-1, v2.clone()).unwrap(), ());
        assert_eq!(
            value.lrange(0, -1).unwrap(),
            vec![&v1[..], &v4[..], &v2[..]]
        );
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
        assert_eq!(value.lrange(0, -1).unwrap(), vec![&v2[..], &v3[..]]);
    }

    #[test]
    fn ltrim_regression() {
        let mut value = Value::List(ValueList::new());
        value.push(vec![1], true).unwrap();
        value.push(vec![2], true).unwrap();
        value.push(vec![3], true).unwrap();
        value.push(vec![4], true).unwrap();

        assert_eq!(value.ltrim(-1000, 1000).unwrap(), ());
        assert_eq!(value.llen().unwrap(), 4);
        assert_eq!(value.ltrim(2, 100).unwrap(), ());
        assert_eq!(value.llen().unwrap(), 2);
        assert_eq!(value.ltrim(0, -1000).unwrap(), ());
        assert_eq!(value.llen().unwrap(), 0);
    }

    #[test]
    fn sadd() {
        let mut value = Value::Nil;
        let v1 = vec![1, 2, 3, 4];

        assert_eq!(value.sadd(v1.clone(), 100).unwrap(), true);
        assert_eq!(value.sadd(v1.clone(), 100).unwrap(), false);
    }

    #[test]
    fn srem() {
        let mut value = Value::Nil;
        let v1 = vec![1, 2, 3, 4];

        assert_eq!(value.srem(&v1).unwrap(), false);
        assert_eq!(value.sadd(v1.clone(), 100).unwrap(), true);
        assert_eq!(value.srem(&v1).unwrap(), true);
        assert_eq!(value.srem(&v1).unwrap(), false);
    }

    #[test]
    fn sismember() {
        let mut value = Value::Nil;
        let v1 = vec![1, 2, 3, 4];

        assert_eq!(value.sismember(&v1).unwrap(), false);
        assert_eq!(value.sadd(v1.clone(), 100).unwrap(), true);
        assert_eq!(value.sismember(&v1).unwrap(), true);
    }

    #[test]
    fn scard() {
        let mut value = Value::Nil;
        let v1 = vec![1, 2, 3, 4];

        assert_eq!(value.scard().unwrap(), 0);
        assert_eq!(value.sadd(v1.clone(), 100).unwrap(), true);
        assert_eq!(value.scard().unwrap(), 1);
    }

    #[test]
    fn srandmember_toomany_nodup() {
        let mut value = Value::Nil;
        let v1 = vec![1];
        let v2 = vec![2];
        let v3 = vec![3];

        value.sadd(v1.clone(), 100).unwrap();
        value.sadd(v2.clone(), 100).unwrap();
        value.sadd(v3.clone(), 100).unwrap();

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

        value.sadd(v1.clone(), 100).unwrap();
        value.sadd(v2.clone(), 100).unwrap();
        value.sadd(v3.clone(), 100).unwrap();

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
        value.sadd(v1.clone(), 100).unwrap();
        value.sadd(v2.clone(), 100).unwrap();

        let mut v = value.srandmember(2, false).unwrap();
        v.sort_by(|a, b| a.cmp(b));
        assert!(
            v == vec![v1.clone(), v1.clone()]
                || v == vec![v1.clone(), v2.clone()]
                || v == vec![v2.clone(), v2.clone()]
        );
    }

    #[test]
    fn srandmember_nodup_some() {
        let mut value = Value::Nil;
        let v1 = vec![1];
        let v2 = vec![2];
        value.sadd(v1.clone(), 100).unwrap();
        value.sadd(v2.clone(), 100).unwrap();

        let mut v = value.srandmember(1, false).unwrap();
        v.sort_by(|a, b| a.cmp(b));
        assert!(v == vec![v1.clone()] || v == vec![v2.clone()]);
    }

    #[test]
    fn srandmember_dup() {
        let mut value = Value::Nil;
        let v1 = vec![1];
        value.sadd(v1.clone(), 100).unwrap();

        let v = value.srandmember(5, true).unwrap();
        assert_eq!(
            v,
            vec![v1.clone(), v1.clone(), v1.clone(), v1.clone(), v1.clone()]
        );
    }

    #[test]
    fn spop_toomany() {
        let mut value = Value::Nil;
        let v1 = vec![1];
        let v2 = vec![2];
        let v3 = vec![3];

        value.sadd(v1.clone(), 100).unwrap();
        value.sadd(v2.clone(), 100).unwrap();
        value.sadd(v3.clone(), 100).unwrap();

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

        value.sadd(v1.clone(), 100).unwrap();
        value.sadd(v2.clone(), 100).unwrap();
        value.sadd(v3.clone(), 100).unwrap();

        let v = value.spop(1).unwrap();
        assert!(v == [v1] || v == [v2] || v == [v3]);
    }

    #[test]
    fn delete_empty() {
        let config = Config::new(Logger::new(Level::Warning));
        let mut database = Database::new(config);

        let key = vec![1u8];
        let value = vec![1u8, 2, 3, 4];
        assert!(database.get_or_create(0, &key).push(value, true).is_ok());
        assert!(database.get_or_create(0, &key).pop(true).is_ok());
        assert!(!database.get(0, &key).is_none());
        database.key_updated(0, &key);
        assert!(database.get(0, &key).is_none());
    }

    #[test]
    fn smembers() {
        let mut value = Value::Nil;
        let v1 = vec![1, 2, 3, 4];
        let v2 = vec![5, 6, 7, 8];
        let v3 = vec![9, 10, 11, 12];

        assert_eq!(value.sadd(v1.clone(), 100).unwrap(), true);
        assert_eq!(value.sadd(v2.clone(), 100).unwrap(), true);
        assert_eq!(value.sadd(v3.clone(), 100).unwrap(), true);

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

        assert_eq!(value1.sadd(v1.clone(), 100).unwrap(), true);
        assert_eq!(value1.sadd(v2.clone(), 100).unwrap(), true);

        assert_eq!(value2.sadd(v1.clone(), 100).unwrap(), true);
        assert_eq!(value2.sadd(v3.clone(), 100).unwrap(), true);

        assert_eq!(
            value1.sdiff(&vec![&value2]).unwrap(),
            vec![v2].iter().cloned().collect::<HashSet<_>>()
        );
    }

    #[test]
    fn sinter() {
        let mut value1 = Value::Nil;
        let mut value2 = Value::Nil;
        let v1 = vec![1, 2, 3, 4];
        let v2 = vec![5, 6, 7, 8];
        let v3 = vec![0, 9, 1, 2];

        assert_eq!(value1.sadd(v1.clone(), 100).unwrap(), true);
        assert_eq!(value1.sadd(v2.clone(), 100).unwrap(), true);

        assert_eq!(value2.sadd(v1.clone(), 100).unwrap(), true);
        assert_eq!(value2.sadd(v3.clone(), 100).unwrap(), true);

        assert_eq!(
            value1
                .sinter(&vec![&value2])
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![&v1]
        );

        let empty: Vec<&Value> = Vec::new();
        assert_eq!(
            value1.sinter(&empty).unwrap(),
            vec![v1, v2].iter().cloned().collect::<HashSet<_>>()
        );

        assert_eq!(value1.sinter(&vec![&value2, &Value::Nil]).unwrap().len(), 0);
    }

    #[test]
    fn sinter_nil() {
        let mut value1 = Value::Nil;
        let value2 = Value::Nil;
        let v1 = vec![1, 2, 3, 4];
        let v2 = vec![5, 6, 7, 8];

        assert_eq!(value1.sadd(v1.clone(), 100).unwrap(), true);
        assert_eq!(value1.sadd(v2.clone(), 100).unwrap(), true);

        assert_eq!(
            value1
                .sinter(&vec![&value2])
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
                .len(),
            0
        );
        assert_eq!(
            value2
                .sinter(&vec![&value1])
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
                .len(),
            0
        );
    }

    #[test]
    fn sunion() {
        let mut value1 = Value::Nil;
        let mut value2 = Value::Nil;
        let v1 = vec![1, 2, 3, 4];
        let v2 = vec![5, 6, 7, 8];
        let v3 = vec![0, 9, 1, 2];

        assert_eq!(value1.sadd(v1.clone(), 100).unwrap(), true);
        assert_eq!(value1.sadd(v2.clone(), 100).unwrap(), true);

        assert_eq!(value2.sadd(v1.clone(), 100).unwrap(), true);
        assert_eq!(value2.sadd(v3.clone(), 100).unwrap(), true);

        assert_eq!(
            value1.sunion(&vec![&value2]).unwrap(),
            vec![&v1, &v2, &v3]
                .iter()
                .cloned()
                .cloned()
                .collect::<HashSet<_>>()
        );

        let empty: Vec<&Value> = Vec::new();
        assert_eq!(
            value1.sunion(&empty).unwrap(),
            vec![&v1, &v2]
                .iter()
                .cloned()
                .cloned()
                .collect::<HashSet<_>>()
        );

        assert_eq!(
            value1.sunion(&vec![&value2, &Value::Nil]).unwrap(),
            vec![&v1, &v2, &v3]
                .iter()
                .cloned()
                .cloned()
                .collect::<HashSet<_>>()
        );
    }

    #[test]
    fn sunion_nil() {
        let value1 = Value::Nil;
        let mut value2 = Value::Nil;
        let v1 = vec![1, 2, 3, 4];
        let v2 = vec![5, 6, 7, 8];
        let v3 = vec![0, 9, 1, 2];

        assert_eq!(value2.sadd(v1.clone(), 100).unwrap(), true);
        assert_eq!(value2.sadd(v2.clone(), 100).unwrap(), true);
        assert_eq!(value2.sadd(v3.clone(), 100).unwrap(), true);

        assert_eq!(
            value1.sunion(&vec![&value2]).unwrap(),
            vec![&v1, &v2, &v3]
                .iter()
                .cloned()
                .cloned()
                .collect::<HashSet<_>>()
        );
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
        let config = Config::new(Logger::new(Level::Warning));
        let database = Database::new(config);
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
        assert_eq!(
            value,
            Value::String(ValueString::Data(vec![0u8, 0, 0, 1, 2, 3, 4]))
        );
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
        let config = Config::new(Logger::new(Level::Warning));
        let mut database = Database::new(config);
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
        let mut value = Value::String(ValueString::Integer(i64::MAX));
        assert!(value.incr(1).is_err());
    }

    #[test]
    fn incrbyfloat_i() {
        let mut value = Value::String(ValueString::Integer(123));
        assert!((value.incrbyfloat(1.2).unwrap() - 124.2) < 0.01);
        assert_eq!(value, Value::String(ValueString::Data(b"124.2".to_vec())));
        let v = value.get().unwrap();
        assert_eq!(v[0], '1' as u8);
        assert_eq!(v[1], '2' as u8);
        assert_eq!(v[2], '4' as u8);
        assert_eq!(v[3], '.' as u8);
        assert!(v[4] == '2' as u8 || v[4] == '1' as u8);
    }

    #[test]
    fn incrbyfloat_s() {
        let mut value = Value::String(ValueString::Data(b"123.4".to_vec()));
        assert!((value.incrbyfloat(1.2).unwrap() - 124.6) < 0.01);
        let v = value.get().unwrap();
        assert_eq!(v[0], '1' as u8);
        assert_eq!(v[1], '2' as u8);
        assert_eq!(v[2], '4' as u8);
        assert_eq!(v[3], '.' as u8);
        assert!(v[4] == '6' as u8 || v[4] == '5' as u8);
    }

    #[test]
    fn set_expire_get() {
        let config = Config::new(Logger::new(Level::Warning));
        let mut database = Database::new(config);
        let key = vec![1u8];
        let value = vec![1u8, 2, 3, 4];
        assert!(database.get_or_create(0, &key).set(value).is_ok());
        database.set_msexpiration(0, key.clone(), mstime());
        assert_eq!(database.get(0, &key), None);
    }

    #[test]
    fn set_will_expire_get() {
        let config = Config::new(Logger::new(Level::Warning));
        let mut database = Database::new(config);
        let key = vec![1u8];
        let value = vec![1u8, 2, 3, 4];
        let expected = value.clone();
        assert!(database.get_or_create(0, &key).set(value).is_ok());
        database.set_msexpiration(0, key.clone(), mstime() + 10000);
        assert_eq!(
            database.get(0, &key),
            Some(&Value::String(ValueString::Data(expected)))
        );
    }

    #[test]
    fn getrange_integer() {
        let value = Value::String(ValueString::Integer(123));
        assert_eq!(
            value.getrange(0, -1).unwrap(),
            "123".to_owned().into_bytes()
        );
        assert_eq!(
            value.getrange(-100, -2).unwrap(),
            "12".to_owned().into_bytes()
        );
        assert_eq!(value.getrange(1, 1).unwrap(), "2".to_owned().into_bytes());
    }

    #[test]
    fn getrange_data() {
        let value = Value::String(ValueString::Data(vec![1, 2, 3]));
        assert_eq!(value.getrange(0, -1).unwrap(), vec![1, 2, 3]);
        assert_eq!(value.getrange(-100, -2).unwrap(), vec![1, 2]);
        assert_eq!(value.getrange(1, 1).unwrap(), vec![2]);
    }

    #[test]
    fn setrange_append() {
        let mut value = Value::String(ValueString::Data(vec![1, 2, 3]));
        assert_eq!(value.setrange(3, vec![4, 5, 6]).unwrap(), 6);
        assert_eq!(
            value,
            Value::String(ValueString::Data(vec![1, 2, 3, 4, 5, 6]))
        );
    }

    #[test]
    fn setrange_create() {
        let mut value = Value::Nil;
        assert_eq!(value.setrange(0, vec![4, 5, 6]).unwrap(), 3);
        assert_eq!(value, Value::String(ValueString::Data(vec![4, 5, 6])));
    }

    #[test]
    fn setrange_padding() {
        let mut value = Value::String(ValueString::Data(vec![1, 2, 3]));
        assert_eq!(value.setrange(5, vec![6]).unwrap(), 6);
        assert_eq!(
            value,
            Value::String(ValueString::Data(vec![1, 2, 3, 0, 0, 6]))
        );
    }

    #[test]
    fn setrange_intermediate() {
        let mut value = Value::String(ValueString::Data(vec![1, 2, 3, 4, 5]));
        assert_eq!(value.setrange(2, vec![13, 14]).unwrap(), 5);
        assert_eq!(
            value,
            Value::String(ValueString::Data(vec![1, 2, 13, 14, 5]))
        );
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
        let value = Value::String(ValueString::Data(vec![1, 2, 3, 4, 5]));
        assert_eq!(value.getbit(0).unwrap(), false);
        assert_eq!(value.getbit(23).unwrap(), true);
        assert_eq!(value.getbit(500).unwrap(), false);
    }

    macro_rules! zadd {
        ($value: expr, $score: expr, $member: expr) => {
            $value
                .zadd($score, $member.clone(), false, false, false, false)
                .unwrap()
        };
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
                }
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

        assert_eq!(
            value
                .zadd(s1, v1.clone(), true, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value
                .zadd(s1, v1.clone(), true, false, false, false)
                .unwrap(),
            false
        );
        assert_eq!(
            value
                .zadd(s2, v2.clone(), true, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value
                .zadd(s1, v2.clone(), true, false, false, false)
                .unwrap(),
            false
        );
        match value {
            Value::SortedSet(value) => match value {
                ValueSortedSet::Data(_, hs) => {
                    assert_eq!(hs.get(&v1).unwrap(), &s1);
                    assert_eq!(hs.get(&v2).unwrap(), &s2);
                }
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

        assert_eq!(
            value
                .zadd(s1, v1.clone(), false, true, false, false)
                .unwrap(),
            false
        );
        assert_eq!(zadd!(value, s1, v1), true);
        assert_eq!(
            value
                .zadd(s2, v1.clone(), false, true, false, false)
                .unwrap(),
            false
        );
        match value {
            Value::SortedSet(value) => match value {
                ValueSortedSet::Data(_, hs) => {
                    assert_eq!(hs.get(&v1).unwrap(), &s2);
                }
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

        assert_eq!(
            value
                .zadd(s1, v1.clone(), false, false, true, false)
                .unwrap(),
            true
        );
        assert_eq!(zadd!(value, s1, v1), false);
        assert_eq!(
            value
                .zadd(s2, v1.clone(), false, false, true, false)
                .unwrap(),
            true
        );
        match value {
            Value::SortedSet(value) => match value {
                ValueSortedSet::Data(_, hs) => {
                    assert_eq!(hs.get(&v1).unwrap(), &s2);
                }
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
        assert_eq!(
            value
                .zcount(Bound::Included(0.0), Bound::Included(5.0))
                .unwrap(),
            2
        );
        assert_eq!(
            value
                .zcount(Bound::Included(1.0), Bound::Included(2.0))
                .unwrap(),
            2
        );
        assert_eq!(
            value
                .zcount(Bound::Excluded(1.0), Bound::Excluded(2.0))
                .unwrap(),
            0
        );
        assert_eq!(
            value
                .zcount(Bound::Included(1.5), Bound::Included(2.0))
                .unwrap(),
            1
        );
        assert_eq!(
            value
                .zcount(Bound::Included(5.0), Bound::Included(10.0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn zlexcount() {
        let mut value = Value::Nil;
        let v1 = vec![1];
        let v2 = vec![2];

        assert_eq!(zadd!(value, 0.0, v1), true);
        assert_eq!(zadd!(value, 0.0, v2), true);
        assert_eq!(
            value
                .zlexcount(Bound::Included(vec![0]), Bound::Included(vec![5]))
                .unwrap(),
            2
        );
        assert_eq!(
            value
                .zlexcount(Bound::Included(vec![1]), Bound::Included(vec![2]))
                .unwrap(),
            2
        );
        assert_eq!(
            value
                .zlexcount(Bound::Excluded(vec![1]), Bound::Excluded(vec![2]))
                .unwrap(),
            0
        );
        assert_eq!(
            value
                .zlexcount(Bound::Included(vec![1, 5]), Bound::Included(vec![2]))
                .unwrap(),
            1
        );
        assert_eq!(
            value
                .zlexcount(Bound::Included(vec![5]), Bound::Included(vec![10]))
                .unwrap(),
            0
        );
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
        assert_eq!(
            value.zrange(0, -1, true, false).unwrap(),
            vec![
                vec![1, 2, 3, 4],
                b"0".to_vec(),
                vec![5, 6, 7, 8],
                b"0".to_vec(),
                vec![9, 10, 11, 12],
                b"0".to_vec(),
            ]
        );
        assert_eq!(
            value.zrange(1, 1, true, false).unwrap(),
            vec![vec![5, 6, 7, 8], b"0".to_vec(),]
        );
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
        assert_eq!(
            value.zrange(0, -1, true, true).unwrap(),
            vec![
                v3.clone(),
                b"0".to_vec(),
                v2.clone(),
                b"0".to_vec(),
                v1.clone(),
                b"0".to_vec(),
            ]
        );
        assert_eq!(
            value.zrange(1, 1, true, true).unwrap(),
            vec![v2.clone(), b"0".to_vec(),]
        );
        assert_eq!(value.zrange(2, 0, true, true).unwrap().len(), 0);
        assert_eq!(
            value.zrange(2, 2, true, true).unwrap(),
            vec![v1.clone(), b"0".to_vec(),]
        );
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
        assert_eq!(
            value
                .zrangebyscore(
                    Bound::Unbounded,
                    Bound::Unbounded,
                    true,
                    0,
                    usize::MAX,
                    false
                )
                .unwrap(),
            vec![
                vec![1, 2, 3, 4],
                b"10".to_vec(),
                vec![5, 6, 7, 8],
                b"20".to_vec(),
                vec![9, 10, 11, 12],
                b"30".to_vec(),
            ]
        );
        assert_eq!(
            value
                .zrangebyscore(
                    Bound::Excluded(10.0),
                    Bound::Included(20.0),
                    true,
                    0,
                    usize::MAX,
                    false
                )
                .unwrap(),
            vec![vec![5, 6, 7, 8], b"20".to_vec(),]
        );
        assert_eq!(
            value
                .zrangebyscore(
                    Bound::Included(20.0),
                    Bound::Excluded(30.0),
                    true,
                    0,
                    usize::MAX,
                    false
                )
                .unwrap(),
            vec![vec![5, 6, 7, 8], b"20".to_vec(),]
        );
        assert_eq!(
            value
                .zrangebyscore(Bound::Unbounded, Bound::Unbounded, true, 1, 1, false)
                .unwrap(),
            vec![vec![5, 6, 7, 8], b"20".to_vec(),]
        );
        assert_eq!(
            value
                .zrangebyscore(
                    Bound::Excluded(30.0),
                    Bound::Included(20.0),
                    false,
                    0,
                    usize::MAX,
                    false
                )
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            value
                .zrangebyscore(
                    Bound::Excluded(30.0),
                    Bound::Excluded(30.0),
                    false,
                    0,
                    usize::MAX,
                    false
                )
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            value
                .zrangebyscore(
                    Bound::Included(30.0),
                    Bound::Included(30.0),
                    false,
                    0,
                    usize::MAX,
                    false
                )
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            value
                .zrangebyscore(
                    Bound::Included(30.0),
                    Bound::Excluded(30.0),
                    false,
                    0,
                    usize::MAX,
                    false
                )
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            value
                .zrangebyscore(
                    Bound::Included(21.0),
                    Bound::Included(22.0),
                    false,
                    0,
                    usize::MAX,
                    false
                )
                .unwrap()
                .len(),
            0
        );
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
        assert_eq!(
            value
                .zrangebyscore(
                    Bound::Unbounded,
                    Bound::Unbounded,
                    true,
                    0,
                    usize::MAX,
                    true
                )
                .unwrap(),
            vec![
                vec![9, 10, 11, 12],
                b"30".to_vec(),
                vec![5, 6, 7, 8],
                b"20".to_vec(),
                vec![1, 2, 3, 4],
                b"10".to_vec(),
            ]
        );
        assert_eq!(
            value
                .zrangebyscore(
                    Bound::Included(20.0),
                    Bound::Excluded(10.0),
                    true,
                    0,
                    usize::MAX,
                    true
                )
                .unwrap(),
            vec![vec![5, 6, 7, 8], b"20".to_vec(),]
        );
        assert_eq!(
            value
                .zrangebyscore(
                    Bound::Excluded(30.0),
                    Bound::Included(20.0),
                    true,
                    0,
                    usize::MAX,
                    true
                )
                .unwrap(),
            vec![vec![5, 6, 7, 8], b"20".to_vec(),]
        );
        assert_eq!(
            value
                .zrangebyscore(Bound::Unbounded, Bound::Unbounded, true, 1, 1, true)
                .unwrap(),
            vec![vec![5, 6, 7, 8], b"20".to_vec(),]
        );
        assert_eq!(
            value
                .zrangebyscore(
                    Bound::Included(20.0),
                    Bound::Excluded(30.0),
                    false,
                    0,
                    usize::MAX,
                    true
                )
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            value
                .zrangebyscore(
                    Bound::Excluded(30.0),
                    Bound::Excluded(30.0),
                    false,
                    0,
                    usize::MAX,
                    true
                )
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            value
                .zrangebyscore(
                    Bound::Included(30.0),
                    Bound::Included(30.0),
                    false,
                    0,
                    usize::MAX,
                    true
                )
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            value
                .zrangebyscore(
                    Bound::Excluded(30.0),
                    Bound::Included(30.0),
                    false,
                    0,
                    usize::MAX,
                    true
                )
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            value
                .zrangebyscore(
                    Bound::Included(22.0),
                    Bound::Included(21.0),
                    false,
                    0,
                    usize::MAX,
                    true
                )
                .unwrap()
                .len(),
            0
        );
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
        assert_eq!(
            value.zrange(0, -1, true, false).unwrap(),
            vec![vec![1, 2, 3, 4], b"1".to_vec(),]
        );
    }

    #[test]
    fn zadd_incr() {
        let mut value = Value::Nil;
        let s1 = 1.0;
        let incr = 2.0;
        let v1 = vec![1, 2, 3, 4];

        assert_eq!(
            value
                .zadd(s1, v1.clone(), false, false, false, true)
                .unwrap(),
            true
        );
        assert_eq!(
            value.zrange(0, -1, true, false).unwrap(),
            vec![v1.clone(), b"1".to_vec()]
        );
        assert_eq!(
            value
                .zadd(incr, v1.clone(), false, false, false, true)
                .unwrap(),
            false
        );
        assert_eq!(
            value.zrange(0, -1, true, false).unwrap(),
            vec![v1.clone(), b"3".to_vec()]
        );
    }

    #[test]
    fn zadd_incr_ch() {
        let mut value = Value::Nil;
        let s1 = 1.0;
        let incr = 2.0;
        let v1 = vec![1, 2, 3, 4];

        assert_eq!(
            value
                .zadd(s1, v1.clone(), false, false, true, true)
                .unwrap(),
            true
        );
        assert_eq!(
            value.zrange(0, -1, true, false).unwrap(),
            vec![v1.clone(), b"1".to_vec()]
        );
        assert_eq!(
            value
                .zadd(incr, v1.clone(), false, false, true, true)
                .unwrap(),
            true
        );
        assert_eq!(
            value.zrange(0, -1, true, false).unwrap(),
            vec![v1.clone(), b"3".to_vec()]
        );
    }

    #[test]
    fn zcard() {
        let mut value = Value::Nil;
        assert_eq!(zadd!(value, 0.0, vec![1, 2, 3, 4]), true);
        assert_eq!(value.zcard().unwrap(), 1);
        assert_eq!(zadd!(value, 1.0, vec![1, 2, 3, 5]), true);
        assert_eq!(value.zcard().unwrap(), 2);
    }

    #[test]
    fn zscore() {
        let mut value = Value::Nil;
        let element = vec![1, 2, 3, 4];
        assert_eq!(zadd!(value, 0.023, element), true);
        assert_eq!(value.zscore(element).unwrap(), Some(0.023));
        assert!(value.zscore(vec![5]).unwrap().is_none());
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
        assert_eq!(value.zincrby(-s1.clone(), v1.clone()).unwrap(), s2);
    }

    #[test]
    fn zunionstore_sum() {
        let mut value1 = Value::Nil;
        let mut value2 = Value::Nil;
        let mut value3 = Value::Nil;
        let v1 = vec![1, 2, 3, 4];
        let v2 = vec![5, 6, 7, 8];
        let v3 = vec![0, 9, 1, 2];

        assert_eq!(
            value1
                .zadd(1.1, v1.clone(), false, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value1
                .zadd(2.1, v2.clone(), false, false, false, false)
                .unwrap(),
            true
        );

        assert_eq!(
            value2
                .zadd(1.2, v1.clone(), false, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value2
                .zadd(3.2, v3.clone(), false, false, false, false)
                .unwrap(),
            true
        );

        value3 = value3
            .zunion(&vec![&value1, &value2], None, zset::Aggregate::Sum)
            .unwrap();
        assert_eq!(value3.zcard().unwrap(), 3);
        assert!((value3.zscore(v1.clone()).unwrap().unwrap() - 2.3).abs() < 0.01);
        assert!((value3.zscore(v2.clone()).unwrap().unwrap() - 2.1).abs() < 0.01);
        assert!((value3.zscore(v3.clone()).unwrap().unwrap() - 3.2).abs() < 0.01);
    }

    #[test]
    fn zunionstore_min() {
        let mut value1 = Value::Nil;
        let mut value2 = Value::Nil;
        let mut value3 = Value::Nil;
        let v1 = vec![1, 2, 3, 4];
        let v2 = vec![5, 6, 7, 8];
        let v3 = vec![0, 9, 1, 2];

        assert_eq!(
            value1
                .zadd(1.1, v1.clone(), false, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value1
                .zadd(2.1, v2.clone(), false, false, false, false)
                .unwrap(),
            true
        );

        assert_eq!(
            value2
                .zadd(1.2, v1.clone(), false, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value2
                .zadd(3.2, v3.clone(), false, false, false, false)
                .unwrap(),
            true
        );

        value3 = value3
            .zunion(&vec![&value1, &value2], None, zset::Aggregate::Min)
            .unwrap();
        assert_eq!(value3.zcard().unwrap(), 3);
        assert!((value3.zscore(v1.clone()).unwrap().unwrap() - 1.1).abs() < 0.01);
        assert!((value3.zscore(v2.clone()).unwrap().unwrap() - 2.1).abs() < 0.01);
        assert!((value3.zscore(v3.clone()).unwrap().unwrap() - 3.2).abs() < 0.01);
    }

    #[test]
    fn zunionstore_max() {
        let mut value1 = Value::Nil;
        let mut value2 = Value::Nil;
        let mut value3 = Value::Nil;
        let v1 = vec![1, 2, 3, 4];
        let v2 = vec![5, 6, 7, 8];
        let v3 = vec![0, 9, 1, 2];

        assert_eq!(
            value1
                .zadd(1.1, v1.clone(), false, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value1
                .zadd(2.1, v2.clone(), false, false, false, false)
                .unwrap(),
            true
        );

        assert_eq!(
            value2
                .zadd(1.2, v1.clone(), false, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value2
                .zadd(3.2, v3.clone(), false, false, false, false)
                .unwrap(),
            true
        );

        value3 = value3
            .zunion(&vec![&value1, &value2], None, zset::Aggregate::Max)
            .unwrap();
        assert_eq!(value3.zcard().unwrap(), 3);
        assert!((value3.zscore(v1.clone()).unwrap().unwrap() - 1.2).abs() < 0.01);
        assert!((value3.zscore(v2.clone()).unwrap().unwrap() - 2.1).abs() < 0.01);
        assert!((value3.zscore(v3.clone()).unwrap().unwrap() - 3.2).abs() < 0.01);
    }

    #[test]
    fn zunionstore_weights() {
        let mut value1 = Value::Nil;
        let mut value2 = Value::Nil;
        let mut value3 = Value::Nil;
        let v1 = vec![1, 2, 3, 4];
        let v2 = vec![5, 6, 7, 8];
        let v3 = vec![0, 9, 1, 2];

        assert_eq!(
            value1
                .zadd(1.1, v1.clone(), false, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value1
                .zadd(2.1, v2.clone(), false, false, false, false)
                .unwrap(),
            true
        );

        assert_eq!(
            value2
                .zadd(1.2, v1.clone(), false, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value2
                .zadd(3.2, v3.clone(), false, false, false, false)
                .unwrap(),
            true
        );

        value3 = value3
            .zunion(
                &vec![&value1, &value2],
                Some(vec![100.0, 200.0]),
                zset::Aggregate::Max,
            )
            .unwrap();
        assert_eq!(value3.zcard().unwrap(), 3);
        assert!((value3.zscore(v1.clone()).unwrap().unwrap() - 240.0).abs() < 0.01);
        assert!((value3.zscore(v2.clone()).unwrap().unwrap() - 210.0).abs() < 0.01);
        assert!((value3.zscore(v3.clone()).unwrap().unwrap() - 640.0).abs() < 0.01);
    }

    #[test]
    fn zinterstore_sum() {
        let mut value1 = Value::Nil;
        let mut value2 = Value::Nil;
        let mut value3 = Value::Nil;
        let v1 = vec![1, 2, 3, 4];
        let v2 = vec![5, 6, 7, 8];
        let v3 = vec![0, 9, 1, 2];

        assert_eq!(
            value1
                .zadd(1.1, v1.clone(), false, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value1
                .zadd(2.1, v2.clone(), false, false, false, false)
                .unwrap(),
            true
        );

        assert_eq!(
            value2
                .zadd(1.2, v1.clone(), false, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value2
                .zadd(3.2, v3.clone(), false, false, false, false)
                .unwrap(),
            true
        );

        value3 = value3
            .zinter(&vec![&value1, &value2], None, zset::Aggregate::Sum)
            .unwrap();
        assert_eq!(value3.zcard().unwrap(), 1);
        assert!((value3.zscore(v1.clone()).unwrap().unwrap() - 2.3).abs() < 0.01);
    }

    #[test]
    fn zinterstore_min() {
        let mut value1 = Value::Nil;
        let mut value2 = Value::Nil;
        let mut value3 = Value::Nil;
        let v1 = vec![1, 2, 3, 4];
        let v2 = vec![5, 6, 7, 8];
        let v3 = vec![0, 9, 1, 2];

        assert_eq!(
            value1
                .zadd(1.1, v1.clone(), false, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value1
                .zadd(2.1, v2.clone(), false, false, false, false)
                .unwrap(),
            true
        );

        assert_eq!(
            value2
                .zadd(1.2, v1.clone(), false, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value2
                .zadd(3.2, v3.clone(), false, false, false, false)
                .unwrap(),
            true
        );

        value3 = value3
            .zinter(&vec![&value1, &value2], None, zset::Aggregate::Min)
            .unwrap();
        assert_eq!(value3.zcard().unwrap(), 1);
        assert!((value3.zscore(v1.clone()).unwrap().unwrap() - 1.1).abs() < 0.01);
    }

    #[test]
    fn zinterstore_max() {
        let mut value1 = Value::Nil;
        let mut value2 = Value::Nil;
        let mut value3 = Value::Nil;
        let v1 = vec![1, 2, 3, 4];
        let v2 = vec![5, 6, 7, 8];
        let v3 = vec![0, 9, 1, 2];

        assert_eq!(
            value1
                .zadd(1.1, v1.clone(), false, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value1
                .zadd(2.1, v2.clone(), false, false, false, false)
                .unwrap(),
            true
        );

        assert_eq!(
            value2
                .zadd(1.2, v1.clone(), false, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value2
                .zadd(3.2, v3.clone(), false, false, false, false)
                .unwrap(),
            true
        );

        value3 = value3
            .zinter(&vec![&value1, &value2], None, zset::Aggregate::Max)
            .unwrap();
        assert_eq!(value3.zcard().unwrap(), 1);
        assert!((value3.zscore(v1.clone()).unwrap().unwrap() - 1.2).abs() < 0.01);
    }

    #[test]
    fn zinterstore_weights() {
        let mut value1 = Value::Nil;
        let mut value2 = Value::Nil;
        let mut value3 = Value::Nil;
        let v1 = vec![1, 2, 3, 4];
        let v2 = vec![5, 6, 7, 8];
        let v3 = vec![0, 9, 1, 2];

        assert_eq!(
            value1
                .zadd(1.1, v1.clone(), false, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value1
                .zadd(2.1, v2.clone(), false, false, false, false)
                .unwrap(),
            true
        );

        assert_eq!(
            value2
                .zadd(1.2, v1.clone(), false, false, false, false)
                .unwrap(),
            true
        );
        assert_eq!(
            value2
                .zadd(3.2, v3.clone(), false, false, false, false)
                .unwrap(),
            true
        );

        value3 = value3
            .zinter(
                &vec![&value1, &value2],
                Some(vec![100.0, 200.0]),
                zset::Aggregate::Max,
            )
            .unwrap();
        assert_eq!(value3.zcard().unwrap(), 1);
        assert!((value3.zscore(v1.clone()).unwrap().unwrap() - 240.0).abs() < 0.01);
    }

    #[test]
    fn pubsub_basic() {
        let config = Config::new(Logger::new(Level::Warning));
        let mut database = Database::new(config);
        let channel_name = vec![1u8, 2, 3];
        let message = vec![2u8, 3, 4, 5, 6];
        let (tx, rx) = channel();
        database.subscribe(channel_name.clone(), tx);
        database.publish(&channel_name, &message);
        assert_eq!(
            rx.recv().unwrap(),
            Some(PubsubEvent::Message(channel_name, None, message).as_response())
        );
    }

    #[test]
    fn unsubscribe() {
        let config = Config::new(Logger::new(Level::Warning));
        let mut database = Database::new(config);
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
        let config = Config::new(Logger::new(Level::Warning));
        let mut database = Database::new(config);
        let channel_name = vec![1u8, 2, 3];
        let message = vec![2u8, 3, 4, 5, 6];
        let (tx, rx) = channel();
        database.psubscribe(channel_name.clone(), tx);
        database.publish(&channel_name, &message);
        assert_eq!(
            rx.recv().unwrap(),
            Some(
                PubsubEvent::Message(channel_name.clone(), Some(channel_name.clone()), message)
                    .as_response()
            )
        );
    }

    #[test]
    fn rehashing() {
        let config = Config::new(Logger::new(Level::Warning));
        let mut database = Database::new(config);
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
        let mut config = Config::new(Logger::new(Level::Warning));
        config.databases = 1;
        config.active_rehashing = false;
        let mut database = Database::new(config);
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

    #[test]
    fn intset() {
        let mut value = Value::Nil;
        let v1 = b"1".to_vec();
        let v2 = b"2".to_vec();
        let v3 = b"3".to_vec();

        assert_eq!(value.sadd(v1.clone(), 2).unwrap(), true);
        assert_eq!(value.sadd(v2.clone(), 2).unwrap(), true);
        match &value {
            Value::Set(set) => match set {
                ValueSet::Integer(_) => (),
                _ => panic!("Must be int set"),
            },
            _ => panic!("Must be set"),
        }
        assert_eq!(value.sadd(v3.clone(), 2).unwrap(), true);
        match value {
            Value::Set(set) => match set {
                ValueSet::Data(_) => (),
                _ => panic!("Must be data set"),
            },
            _ => panic!("Must be set"),
        }
    }

    #[test]
    fn mapcommand() {
        let mut config = Config::new(Logger::new(Level::Warning));
        config.rename_commands.insert("disabled".to_owned(), None);
        config
            .rename_commands
            .insert("source".to_owned(), Some("target".to_owned()));
        let database = Database::new(config);
        assert_eq!(database.mapped_command(&"disabled".to_owned()), None);
        assert_eq!(
            database.mapped_command(&"source".to_owned()),
            Some("target".to_owned())
        );
        assert_eq!(
            database.mapped_command(&"other".to_owned()),
            Some("other".to_owned())
        );
    }

    #[test]
    fn dump_integer() {
        let mut v = vec![];
        Value::String(ValueString::Integer(1)).dump(&mut v).unwrap();
        assert_eq!(&*v, b"\x00\xc0\x01\x07\x00\xd9J2E\xd9\xcb\xc4\xe6");
    }

    #[test]
    fn watch() {
        let config = Config::new(Logger::new(Level::Warning));
        let mut database = Database::new(config);
        database.key_watch(0, &vec![1], 31);
        assert!(database.key_watch_verify(0, &vec![1], 31));
        database.key_updated(0, &vec![2]);
        database.key_updated(1, &vec![1]);
        assert!(database.key_watch_verify(0, &vec![1], 31));
        database.key_updated(0, &vec![1]);
        assert!(!database.key_watch_verify(0, &vec![1], 31));
    }

    #[test]
    fn unwatch() {
        let config = Config::new(Logger::new(Level::Warning));
        let mut database = Database::new(config);
        database.key_watch(0, &vec![1], 31);
        database.key_watch(0, &vec![1], 32);
        database.key_unwatch(0, &vec![1], 31);
        assert!(database.key_watch_verify(0, &vec![1], 32));
        assert!(!database.key_watch_verify(0, &vec![1], 31));
    }

    #[test]
    fn active_expire() {
        let config = Config::new(Logger::new(Level::Warning));
        let mut database = Database::new(config);
        let key = vec![1u8];
        let value = vec![1u8, 2, 3, 4];
        assert!(database.get_or_create(0, &key).set(value).is_ok());
        database.set_msexpiration(0, key.clone(), mstime());
        assert_eq!(database.dbsize(0), 1);
        database.active_expire_cycle(0);
        assert_eq!(database.dbsize(0), 0);
    }

    #[test]
    fn active_expire2() {
        let config = Config::new(Logger::new(Level::Warning));
        let mut database = Database::new(config);
        let key1 = vec![1u8];
        let key2 = vec![2u8];
        let key3 = vec![3u8];
        let value = vec![1u8, 2, 3, 4];
        assert!(database.get_or_create(0, &key1).set(value.clone()).is_ok());
        assert!(database.get_or_create(0, &key2).set(value.clone()).is_ok());
        assert!(database.get_or_create(0, &key3).set(value.clone()).is_ok());
        database.set_msexpiration(0, key1.clone(), mstime());
        database.set_msexpiration(0, key2.clone(), mstime() + 1000);
        assert_eq!(database.dbsize(0), 3);
        database.active_expire_cycle(100);
        assert_eq!(database.dbsize(0), 2);
    }

    #[test]
    fn monitor_log() {
        let config = Config::new(Logger::new(Level::Warning));
        let mut database = Database::new(config);
        let (tx, rx) = channel();
        database.monitor_add(tx.clone());
        database.monitor_add(tx.clone());
        database.log_command(
            0,
            &ParsedCommand::new(b"1", vec![Argument { pos: 0, len: 1 }]),
            true,
        );
        assert_eq!(rx.try_recv().unwrap(), "\"1\" ".to_owned());
        assert_eq!(rx.try_recv().unwrap(), "\"1\" ".to_owned());
        assert!(rx.try_recv().is_err())
    }
}
