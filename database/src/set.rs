use std::collections::VecMap;
use std::collections::HashSet;

use dbutil::usize_to_vec;
use dbutil::vec_to_usize;

use rand::thread_rng;
use rand::distributions::{Sample, IndependentSample, Range};

#[derive(PartialEq, Debug, Clone)]
pub enum ValueSet {
    Integer(VecMap<()>),
    Data(HashSet<Vec<u8>>),
}

impl ValueSet {
    pub fn new() -> ValueSet {
        ValueSet::Integer(VecMap::new())
    }

    pub fn create_with_hashset(h: HashSet<Vec<u8>>) -> ValueSet {
        ValueSet::Data(h)
    }

    pub fn is_intset(&self) -> bool {
        match *self {
            ValueSet::Integer(_) => true,
            _ => false,
        }
    }

    fn into_data(&mut self) {
        let mut h = HashSet::new();
        match *self {
            ValueSet::Integer(ref mut set) => {
                for i in set.keys() {
                    h.insert(usize_to_vec(i));
                }
            },
            ValueSet::Data(_) => return,
        }
        *self = ValueSet::Data(h);
    }

    pub fn sadd(&mut self, el: Vec<u8>) -> bool {
        match *self {
            ValueSet::Integer(ref mut set) => match vec_to_usize(&el) {
                Ok(v) => return set.insert(v, ()).is_none(),
                Err(_) => (), // no return
            },
            ValueSet::Data(ref mut set) => return set.insert(el),
        }

        // convert to a data set and insert
        self.into_data();
        self.sadd(el)
    }

    pub fn srem(&mut self, el: &Vec<u8>) -> bool {
        match *self {
            ValueSet::Data(ref mut set) => set.remove(el),
            ValueSet::Integer(ref mut set) => match vec_to_usize(&el) {
                Ok(v) => set.remove(&v).is_some(),
                Err(_) => false, // only have usize, removing not a usize
            }
        }
    }

    pub fn sismember(&self, el: &Vec<u8>) -> bool {
        match *self {
            ValueSet::Data(ref set) => set.contains(el),
            ValueSet::Integer(ref set) => match vec_to_usize(&el) {
                Ok(v) => set.contains_key(&v),
                Err(_) => false, // only have usize, removing not a usize
            }
        }
    }

    pub fn scard(&self) -> usize {
        match *self {
            ValueSet::Data(ref set) => set.len(),
            ValueSet::Integer(ref set) => set.len(),
        }
    }

    pub fn smembers(&self) -> Vec<Vec<u8>> {
        match *self {
            ValueSet::Data(ref set) => set.iter().map(|x| x.clone()).collect::<Vec<_>>(),
            ValueSet::Integer(ref set) => set.keys().map(|x| usize_to_vec(x)).collect::<Vec<_>>()
        }
    }

    fn get_random_positions(&self, len: usize, count: usize, allow_duplicates: bool) -> Vec<usize> {
        // TODO: turn this into an iterator
        let mut range = Range::new(0, len);
        let mut rng = thread_rng();
        if allow_duplicates {
            let mut r = Vec::new();
            for _ in 0..count {
                r.push(range.ind_sample(&mut rng));
            }
            r.sort();
            r
        } else {
            let min = if len < count { len } else { count };
            let mut r = HashSet::new();
            while r.len() < min {
                r.insert(range.sample(&mut rng));
            }
            let mut vec = r.iter().cloned().collect::<Vec<_>>();
            vec.sort();
            vec
        }
    }

    fn srandmember_data(&self, set: &HashSet<Vec<u8>>, count: usize, allow_duplicates: bool) -> Vec<Vec<u8>> {
        // TODO: implemented in O(n), should be O(1)
        let mut r = Vec::new();
        for pos in self.get_random_positions(set.len(), count, allow_duplicates) {
            r.push(set.iter().skip(pos).take(1).next().unwrap().clone());
        }
        r
    }

    fn srandmember_integer(&self, set: &VecMap<()>, count: usize, allow_duplicates: bool) -> Vec<Vec<u8>> {
        // TODO: implemented in O(n), should be O(1)
        let mut r = Vec::new();
        for pos in self.get_random_positions(set.len(), count, allow_duplicates) {
            r.push(usize_to_vec(set.keys().skip(pos).take(1).next().unwrap()));
        }
        r
    }

    pub fn srandmember(&self, count: usize, allow_duplicates: bool) -> Vec<Vec<u8>> {
        match *self {
            ValueSet::Data(ref set) => self.srandmember_data(set, count, allow_duplicates),
            ValueSet::Integer(ref set) => self.srandmember_integer(set, count, allow_duplicates),
        }
    }

    pub fn spop(&mut self, count: usize) -> Vec<Vec<u8>> {
        // TODO: implemented in O(n), should be O(1)

        let len = self.scard();
        if count >= len {
            return match *self {
                ValueSet::Data(ref mut set) => set.drain().collect::<Vec<_>>(),
                ValueSet::Integer(ref mut set) => set.drain().map(|(x, _)| usize_to_vec(x)).collect::<Vec<_>>(),
            };
        }

        let positions = self.get_random_positions(self.scard(), count, false);
        match *self {
            ValueSet::Data(ref mut set) => {
                let mut r = Vec::new();
                for pos in positions {
                    let el = set.iter().skip(pos).take(1).next().unwrap().clone();
                    set.remove(&el);
                    r.push(el);
                }
                r
            },
            ValueSet::Integer(ref mut set) => {
                let mut r = Vec::new();
                for pos in positions {
                    let el = set.keys().skip(pos).take(1).next().unwrap();
                    set.remove(&el);
                    r.push(usize_to_vec(el));
                }
                r
            },
        }
    }

    pub fn sdiff(&self, sets: Vec<&ValueSet>) -> HashSet<Vec<u8>> {
        match *self {
            ValueSet::Data(ref original_set) => {
                let mut elements: HashSet<Vec<u8>> = original_set.clone();
                for newvalue in sets {
                    match *newvalue {
                        ValueSet::Integer(ref set) => {
                            for el in set.keys() {
                                elements.remove(&usize_to_vec(el));
                            }
                        }
                        ValueSet::Data(ref set) => {
                            for el in set {
                                elements.remove(el);
                            }
                        },
                    }
                }
                elements
            },
            ValueSet::Integer(ref original_set) => {
                let mut elements: VecMap<_> = original_set.clone();
                for newvalue in sets {
                    match *newvalue {
                        ValueSet::Integer(ref set) => {
                            for el in set.keys() {
                                elements.remove(&el);
                            }
                        }
                        ValueSet::Data(ref set) => {
                            for el in set {
                                match vec_to_usize(el) {
                                    Ok(i) => elements.remove(&i),
                                    Err(_) => None,
                                };
                            }
                        },
                    }
                }
                elements.iter().map(|(x, _)| usize_to_vec(x)).collect()
            }
        }
    }

    pub fn sinter(&self, sets: Vec<&ValueSet>) -> HashSet<Vec<u8>> {
        match *self {
            ValueSet::Data(ref original_set) => {
                let mut result: HashSet<Vec<u8>> = original_set.clone();
                for newvalue in sets {
                    match *newvalue {
                        ValueSet::Integer(ref set) => {
                            result = result.intersection(&set.keys().map(|x| usize_to_vec(x)).collect::<HashSet<_>>()).cloned().collect();
                        },
                        ValueSet::Data(ref set) => {
                            result = result.intersection(set).cloned().collect();
                        },
                    }
                    if result.len() == 0 { break; }
                }
                result
            },
            ValueSet::Integer(ref original_set) => {
                let mut result: HashSet<usize> = original_set.keys().collect::<HashSet<_>>();
                for newvalue in sets {
                    match *newvalue {
                        ValueSet::Integer(ref set) => {
                            result = result.intersection(&set.keys().collect::<HashSet<_>>()).cloned().collect();
                        },
                        ValueSet::Data(ref set) => {
                            result = result.intersection(&set.iter().filter_map(|x| vec_to_usize(x).ok()).collect::<HashSet<_>>()).cloned().collect();
                        },
                    }
                    if result.len() == 0 { break; }
                }
                result.into_iter().map(|x| usize_to_vec(x)).collect()
            }
        }
    }

    pub fn sunion(&self, sets: Vec<&ValueSet>) -> HashSet<Vec<u8>> {
        let mut result: HashSet<Vec<u8>> = match *self {
            ValueSet::Data(ref original_set) => original_set.clone(),
            ValueSet::Integer(ref set) => set.keys().map(|x| usize_to_vec(x)).collect::<HashSet<_>>(),
        };
        for newvalue in sets {
            match *newvalue {
                ValueSet::Integer(ref set) => {
                    result = result.union(&set.keys().map(|x| usize_to_vec(x)).collect::<HashSet<_>>()).cloned().collect();
                },
                ValueSet::Data(ref set) => {
                    result = result.union(set).cloned().collect();
                },
            }
        }
        result
    }
}

#[cfg(test)]
mod test_set {
    use std::iter::FromIterator;
    use std::collections::HashSet;

    use super::ValueSet;

    #[test]
    fn intset() {
        let mut set = ValueSet::new();
        assert!(set.is_intset());
        assert!(set.sadd(b"123".to_vec()));
        assert!(set.is_intset());
        assert!(set.sadd(b"456".to_vec()));
        assert!(set.is_intset());
        assert!(!set.sadd(b"123".to_vec()));
        assert!(set.is_intset());
        assert!(set.sadd(b"foo".to_vec()));
        assert!(!set.is_intset());
    }

    #[test]
    fn intset_srem() {
        let mut set = ValueSet::new();
        assert!(set.is_intset());
        assert!(set.sadd(b"123".to_vec()));
        assert!(set.is_intset());
        assert!(!set.srem(&b"456".to_vec()));
        assert!(set.is_intset());
        assert!(set.srem(&b"123".to_vec()));
        assert!(set.is_intset());
        assert!(!set.srem(&b"foo".to_vec()));
        assert!(set.is_intset());
    }

    #[test]
    fn intset_sismember() {
        let mut set = ValueSet::new();
        assert!(set.sadd(b"123".to_vec()));
        assert!(set.sadd(b"456".to_vec()));
        assert!(set.sismember(&b"123".to_vec()));
        assert!(set.sismember(&b"456".to_vec()));
        assert!(!set.sismember(&b"foo".to_vec()));
        assert!(!set.sismember(&b"124".to_vec()));
    }

    #[test]
    fn intset_scard() {
        let mut set = ValueSet::new();
        assert_eq!(set.scard(), 0);
        assert!(set.sadd(b"123".to_vec()));
        assert_eq!(set.scard(), 1);
        assert!(set.sadd(b"456".to_vec()));
        assert_eq!(set.scard(), 2);
    }

    #[test]
    fn intset_smembers() {
        let mut set = ValueSet::new();
        assert!(set.sadd(b"123".to_vec()));
        assert!(set.sadd(b"456".to_vec()));
        assert!(set.smembers() == vec![b"123".to_vec(), b"456".to_vec()] ||
                set.smembers() == vec![b"456".to_vec(), b"123".to_vec()]);
    }

    #[test]
    fn intset_spop() {
        let mut set = ValueSet::new();
        assert!(set.sadd(b"123".to_vec()));
        assert!(set.sadd(b"456".to_vec()));
        let mut h = HashSet::new();
        h.insert(set.spop(1).pop().unwrap());
        h.insert(set.spop(1).pop().unwrap());
        assert_eq!(set.spop(1).len(), 0);
        assert_eq!(h, HashSet::from_iter(vec![b"123".to_vec(), b"456".to_vec()]));
    }

    #[test]
    fn intset_sdiff() {
        let mut set = ValueSet::new();
        assert!(set.sadd(b"123".to_vec()));
        assert!(set.sadd(b"456".to_vec()));
        assert!(set.sadd(b"789".to_vec()));

        let mut set2 = ValueSet::new();
        assert!(set2.sadd(b"123".to_vec()));

        let mut set3 = ValueSet::new();
        assert!(set3.sadd(b"456".to_vec()));
        assert!(set3.sadd(b"foo".to_vec()));

        assert_eq!(set.sdiff(vec![&set2, &set3]), HashSet::from_iter(vec![b"789".to_vec()]));
    }

    #[test]
    fn intset_sinter() {
        let mut set = ValueSet::new();
        assert!(set.sadd(b"bar".to_vec()));
        assert!(set.sadd(b"123".to_vec()));
        assert!(set.sadd(b"456".to_vec()));
        assert!(set.sadd(b"789".to_vec()));

        let mut set2 = ValueSet::new();
        assert!(set2.sadd(b"123".to_vec()));
        assert!(set2.sadd(b"456".to_vec()));

        let mut set3 = ValueSet::new();
        assert!(set3.sadd(b"123".to_vec()));
        assert!(set3.sadd(b"456".to_vec()));
        assert!(set3.sadd(b"foo".to_vec()));

        assert_eq!(set.sinter(vec![&set2, &set3]), HashSet::from_iter(vec![
                    b"123".to_vec(),
                    b"456".to_vec(),
                    ]));
    }

    #[test]
    fn intset_sinter2() {
        let mut set = ValueSet::new();
        assert!(set.sadd(b"123".to_vec()));
        assert!(set.sadd(b"456".to_vec()));
        assert!(set.sadd(b"789".to_vec()));

        let mut set2 = ValueSet::new();
        assert!(set2.sadd(b"123".to_vec()));
        assert!(set2.sadd(b"456".to_vec()));

        let mut set3 = ValueSet::new();
        assert!(set3.sadd(b"123".to_vec()));
        assert!(set3.sadd(b"456".to_vec()));
        assert!(set3.sadd(b"foo".to_vec()));

        assert_eq!(set.sinter(vec![&set2, &set3]), HashSet::from_iter(vec![
                    b"123".to_vec(),
                    b"456".to_vec(),
                    ]));
    }

    #[test]
    fn intset_sunion() {
        let mut set = ValueSet::new();
        assert!(set.sadd(b"bar".to_vec()));
        assert!(set.sadd(b"123".to_vec()));

        let mut set2 = ValueSet::new();
        assert!(set2.sadd(b"123".to_vec()));
        assert!(set2.sadd(b"456".to_vec()));

        let mut set3 = ValueSet::new();
        assert!(set3.sadd(b"456".to_vec()));
        assert!(set3.sadd(b"foo".to_vec()));

        assert_eq!(set.sunion(vec![&set2, &set3]), HashSet::from_iter(vec![
                    b"123".to_vec(),
                    b"456".to_vec(),
                    b"foo".to_vec(),
                    b"bar".to_vec(),
                    ]));
    }

    #[test]
    fn intset_sunion2() {
        let mut set = ValueSet::new();
        assert!(set.sadd(b"123".to_vec()));

        let mut set2 = ValueSet::new();
        assert!(set2.sadd(b"456".to_vec()));

        let mut set3 = ValueSet::new();
        assert!(set3.sadd(b"456".to_vec()));

        assert_eq!(set.sunion(vec![&set2, &set3]), HashSet::from_iter(vec![
                    b"123".to_vec(),
                    b"456".to_vec(),
                    ]));
    }
}
