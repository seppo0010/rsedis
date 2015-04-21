use std::collections::HashMap;
use std::str::from_utf8;

pub enum Value {
    Nil,
    Integer(u64),
    Data(Vec<u8>),
}

impl Value {
    pub fn set(&mut self, value: Vec<u8>) {
        if value.len() < 32 { // ought to be enough!
            let try_utf8 = from_utf8(&*value);
            if try_utf8.is_ok() {
                let try_parse = try_utf8.unwrap().parse::<u64>();
                if try_parse.is_ok() {
                    *self = Value::Integer(try_parse.unwrap());
                    return;
                }
            }
        }
        *self = Value::Data(value);
    }

    pub fn append(&mut self, value: Vec<u8>) -> usize {
        match self {
            &mut Value::Nil => {
                let len = value.len();
                *self = Value::Data(value);
                return len;
            },
            &mut Value::Data(ref mut data) => { data.extend(value); return data.len(); },
            &mut Value::Integer(i) => {
                let oldstr = format!("{}", i);
                let len = oldstr.len() + value.len();
                *self = Value::Data(oldstr.into_bytes() + &*value);
                return len;
            },
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
