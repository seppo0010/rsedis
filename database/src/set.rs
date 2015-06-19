use std::collections::HashSet;

use rand::random;

#[derive(PartialEq, Debug, Clone)]
pub enum ValueSet {
    Data(HashSet<Vec<u8>>),
}

impl ValueSet {
    pub fn new() -> ValueSet {
        ValueSet::Data(HashSet::new())
    }

    pub fn create_with_hashset(h: HashSet<Vec<u8>>) -> ValueSet {
        ValueSet::Data(h)
    }

    pub fn sadd(&mut self, el: Vec<u8>) -> bool {
        match *self {
            ValueSet::Data(ref mut set) => set.insert(el),
        }
    }

    pub fn srem(&mut self, el: &Vec<u8>) -> bool {
        match *self {
            ValueSet::Data(ref mut set) => set.remove(el),
        }
    }

    pub fn sismember(&self, el: &Vec<u8>) -> bool {
        match *self {
            ValueSet::Data(ref set) => set.contains(el),
        }
    }

    pub fn scard(&self) -> usize {
        match *self {
            ValueSet::Data(ref set) => set.len(),
        }
    }

    pub fn smembers(&self) -> Vec<Vec<u8>> {
        match *self {
            ValueSet::Data(ref set) => set.iter().map(|x| x.clone()).collect::<Vec<_>>(),
        }
    }

    pub fn srandmember(&self, count: usize, allow_duplicates: bool) -> Vec<Vec<u8>> {
        // TODO: implemented in O(n), should be O(1)
        let set = match self {
            &ValueSet::Data(ref s) => s,
        };

        if allow_duplicates {
            let mut r = Vec::new();
            for _ in 0..count {
                let pos = random::<usize>() % set.len();
                r.push(set.iter().skip(pos).take(1).next().unwrap().clone());
            }
            r
        } else {
            if count >= set.len() {
                return set.iter().map(|x| x.clone()).collect::<Vec<_>>();
            }
            let mut s = HashSet::new();
            while s.len() < count {
                let pos = random::<usize>() % set.len();
                s.insert(set.iter().skip(pos).take(1).next().unwrap().clone());
            }
            s.iter().map(|x| x.clone()).collect::<Vec<_>>()
        }
    }

    pub fn spop(&mut self, count: usize) -> Vec<Vec<u8>> {
        // TODO: implemented in O(n), should be O(1)

        let len = self.scard();
        if count >= len {
            return match self {
                &mut ValueSet::Data(ref mut set) => set.drain().collect::<Vec<_>>(),
            };
        }

        let r = self.srandmember(count, false);

        let mut set = match *self {
            ValueSet::Data(ref mut s) => s,
        };

        for member in r.iter() {
            set.remove(member);
        }

        r
    }

    pub fn sdiff(&self, sets: Vec<&ValueSet>) -> HashSet<Vec<u8>> {
        match *self {
            ValueSet::Data(ref original_set) => {
                let mut elements: HashSet<Vec<u8>> = original_set.clone();
                for newvalue in sets {
                    match newvalue {
                        &ValueSet::Data(ref set) => {
                            for el in set {
                                elements.remove(el);
                            }
                        },
                    }
                }
                elements
            },
        }
    }

    pub fn sinter(&self, sets: Vec<&ValueSet>) -> HashSet<Vec<u8>> {
        match *self {
            ValueSet::Data(ref original_set) => {
                let mut result: HashSet<Vec<u8>> = original_set.clone();
                for newvalue in sets {
                    match newvalue {
                        &ValueSet::Data(ref set) => {
                            result = result.intersection(set).cloned().collect();
                            if result.len() == 0 { break; }
                        },
                    }
                }
                result
            },
        }
    }

    pub fn sunion(&self, sets: Vec<&ValueSet>) -> HashSet<Vec<u8>> {
        match *self {
            ValueSet::Data(ref original_set) => {
                let mut result: HashSet<Vec<u8>> = original_set.clone();
                for newvalue in sets {
                    match newvalue {
                        &ValueSet::Data(ref set) => {
                            result = result.union(set).cloned().collect();
                            if result.len() == 0 { break; }
                        },
                    }
                }
                result
            },
        }
    }
}
