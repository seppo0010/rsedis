use std::collections::LinkedList;

use dbutil::normalize_position;
use error::OperationError;

#[derive(PartialEq, Debug, Clone)]
pub enum ValueList {
    Data(LinkedList<Vec<u8>>),
}

impl ValueList {
    pub fn new() -> Self {
        ValueList::Data(LinkedList::new())
    }

    pub fn push(&mut self, el: Vec<u8>, right: bool) {
        match *self {
            ValueList::Data(ref mut list) => if right {
                list.push_back(el);
            } else {
                list.push_front(el);
            },
        };
    }

    pub fn pop(&mut self, right: bool) -> Option<Vec<u8>> {
        match *self {
            ValueList::Data(ref mut list) => {
                if right {
                    list.pop_back()
                } else {
                    list.pop_front()
                }
            }
        }
    }

    pub fn lindex(&self, _index: i64) -> Option<&Vec<u8>> {
        match *self {
            ValueList::Data(ref list) => {
                let index = match normalize_position(_index, list.len()) {
                    Ok(i) => i,
                    Err(_) => return None,
                };
                list.iter().nth(index as usize)
            },
        }
    }

    pub fn linsert(&mut self, before: bool, pivot: Vec<u8>, newvalue: Vec<u8>) -> Option<usize> {
        match *self {
            ValueList::Data(ref mut list) => {
                match list.iter().position(|x| x == &pivot) {
                    Some(_pos) => {
                        let pos = if before { _pos } else { _pos + 1 };
                        let mut right = list.split_off(pos);
                        list.push_back(newvalue);
                        list.append(&mut right);
                        Some(list.len())
                    },
                    None => return None,
                }
            },
        }
    }

    pub fn llen(&self) -> usize {
        match *self {
            ValueList::Data(ref list) => list.len(),
        }
    }

    pub fn lrange(&self, _start: i64, _stop: i64) -> Vec<&Vec<u8>> {
        match *self {
            ValueList::Data(ref list) => {
                let len = list.len();
                let start = match normalize_position(_start, len) {
                    Ok(i) => i,
                    Err(i) => if i == 0 { 0 } else { return Vec::new(); },
                };
                let stop = match normalize_position(_stop, len) {
                    Ok(i) => i,
                    Err(i) => if i == 0 { return Vec::new(); } else { i },
                };
                list.iter().skip(start as usize).take(stop as usize - start as usize + 1).collect::<Vec<_>>()
            },
        }
    }

    pub fn lrem(&mut self, left: bool, limit: usize, newvalue: Vec<u8>) -> usize {
        let mut count = 0;
        let mut newlist = LinkedList::new();
        match *self {
            ValueList::Data(ref mut list) => {
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
        };
        *self = ValueList::Data(newlist);
        count
    }

    pub fn lset(&mut self, index: i64, newvalue: Vec<u8>) -> Result<(), OperationError> {
        match *self {
            ValueList::Data(ref mut list) => {
                let i = match normalize_position(index, list.len()) {
                    Ok(i) => i,
                    Err(_) => return Err(OperationError::OutOfBoundsError),
                };
                // this unwrap is safe because `i` is already validated to be inside the list
                let el = list.iter_mut().skip(i).next().unwrap();
                *el = newvalue;
                Ok(())
            }
        }
    }


    pub fn ltrim(&mut self, _start: i64, _stop: i64) -> Result<(), OperationError> {
        let list = match *self {
            ValueList::Data(ref mut list) => {
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
                list.split_off(start)
            }
        };
        *self = ValueList::Data(list);
        Ok(())
    }
}
