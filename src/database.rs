use std::collections::HashMap;
use std::str::from_utf8;

pub enum Value {
    Integer(u64),
    Data(Vec<u8>),
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

    pub fn set(&mut self, key: &Vec<u8>, value: Vec<u8>) {
        self.data.remove(key);
        if value.len() < 32 { // ought to be enough!
            let try_utf8 = from_utf8(&*value);
            if try_utf8.is_ok() {
                let try_parse = try_utf8.unwrap().parse::<u64>();
                if try_parse.is_ok() {
                    self.data.insert(Vec::clone(key), Value::Integer(try_parse.unwrap()));
                    return;
                }
            }
        }
        self.data.insert(Vec::clone(key), Value::Data(value));
    }

    pub fn append(&mut self, key: &Vec<u8>, value: Vec<u8>) -> usize {
        match self.data.get_mut(key) {
            Some(oldvalue) => {
                match oldvalue {
                    &mut Value::Data(ref mut data) => { data.extend(value); return data.len(); },
                    &mut Value::Integer(i) => {
                        let oldstr = format!("{}", i);
                        let len = oldstr.len() + value.len();
                        *oldvalue = Value::Data(oldstr.into_bytes() + &*value);
                        return len;
                    },
                }
            }
            None => {}
        }
        let len = value.len();
        self.set(key, value);
        return len;
    }
}
