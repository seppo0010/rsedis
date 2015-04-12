use std::collections::HashMap;

pub enum Value {
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
        self.data.insert(Vec::clone(key), Value::Data(value));
    }
}
