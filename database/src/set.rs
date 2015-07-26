use std::collections::HashSet;
use std::io;
use std::io::Write;

use dbutil::usize_to_vec;
use dbutil::vec_to_usize;
use rdbutil::constants::*;
use rdbutil::{EncodeError, encode_slice_u8, encode_len};
use rdbutil::{encode_u16_to_slice_u8, encode_u32_to_slice_u8, encode_u64_to_slice_u8};

use rand::thread_rng;
use rand::distributions::{Sample, IndependentSample, Range};

#[derive(PartialEq, Debug, Clone)]
pub enum ValueSet {
    Integer(HashSet<usize>),
    Data(HashSet<Vec<u8>>),
}

impl ValueSet {
    pub fn new() -> ValueSet {
        ValueSet::Integer(HashSet::new())
    }

    pub fn create_with_hashset(h: HashSet<Vec<u8>>) -> ValueSet {
        let mut s = HashSet::new();
        for v in h.iter() {
            match vec_to_usize(&v) {
                Ok(n) => { s.insert(n); },
                Err(_) => break,
            }
        }
        if h.len() == s.len() {
            ValueSet::Integer(s)
        } else {
            ValueSet::Data(h)
        }
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
                for i in set.iter() {
                    h.insert(usize_to_vec(i.clone()));
                }
            },
            ValueSet::Data(_) => return,
        }
        *self = ValueSet::Data(h);
    }

    pub fn sadd(&mut self, el: Vec<u8>, max_int_size: usize) -> bool {
        match *self {
            ValueSet::Integer(ref mut set) => {
                if set.len() < max_int_size {
                    match vec_to_usize(&el) {
                        Ok(v) => return set.insert(v),
                        Err(_) => (), // no return
                    }
                }
            },
            ValueSet::Data(ref mut set) => return set.insert(el),
        }

        // convert to a data set and insert
        self.into_data();
        self.sadd(el, max_int_size)
    }

    pub fn srem(&mut self, el: &Vec<u8>) -> bool {
        match *self {
            ValueSet::Data(ref mut set) => set.remove(el),
            ValueSet::Integer(ref mut set) => match vec_to_usize(&el) {
                Ok(v) => set.remove(&v),
                Err(_) => false, // only have usize, removing not a usize
            }
        }
    }

    pub fn sismember(&self, el: &Vec<u8>) -> bool {
        match *self {
            ValueSet::Data(ref set) => set.contains(el),
            ValueSet::Integer(ref set) => match vec_to_usize(&el) {
                Ok(v) => set.contains(&v),
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
            ValueSet::Integer(ref set) => set.iter().map(|x| usize_to_vec(x.clone())).collect::<Vec<_>>()
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
            r.sort_by(|a, b| a.cmp(b).reverse());
            r
        } else {
            let min = if len < count { len } else { count };
            let mut r = HashSet::new();
            while r.len() < min {
                r.insert(range.sample(&mut rng));
            }
            let mut vec = r.iter().cloned().collect::<Vec<_>>();
            vec.sort_by(|a, b| a.cmp(b).reverse());
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

    fn srandmember_integer(&self, set: &HashSet<usize>, count: usize, allow_duplicates: bool) -> Vec<Vec<u8>> {
        // TODO: implemented in O(n), should be O(1)
        let mut r = Vec::new();
        for pos in self.get_random_positions(set.len(), count, allow_duplicates) {
            r.push(usize_to_vec(set.iter().skip(pos).take(1).next().unwrap().clone()));
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
                ValueSet::Integer(ref mut set) => set.drain().map(|x| usize_to_vec(x)).collect::<Vec<_>>(),
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
                    let el = set.iter().skip(pos).take(1).next().unwrap().clone();
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
                            for el in set.iter() {
                                elements.remove(&usize_to_vec(el.clone()));
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
                let mut elements: HashSet<usize> = original_set.clone();
                for newvalue in sets {
                    match *newvalue {
                        ValueSet::Integer(ref set) => {
                            for el in set.iter() {
                                elements.remove(&el);
                            }
                        }
                        ValueSet::Data(ref set) => {
                            for el in set {
                                match vec_to_usize(el) {
                                    Ok(i) => elements.remove(&i),
                                    Err(_) => false,
                                };
                            }
                        },
                    }
                }
                elements.iter().map(|x| usize_to_vec(x.clone())).collect()
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
                            result = result.intersection(&set.iter().map(|x| usize_to_vec(x.clone())).collect::<HashSet<_>>()).cloned().collect();
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
                let mut result: HashSet<usize> = original_set.iter().cloned().collect::<HashSet<_>>();
                for newvalue in sets {
                    match *newvalue {
                        ValueSet::Integer(ref set) => {
                            result = result.intersection(&set.iter().cloned().collect::<HashSet<_>>()).cloned().collect();
                        },
                        ValueSet::Data(ref set) => {
                            result = result.intersection(&set.iter().filter_map(|x| vec_to_usize(x).ok()).collect::<HashSet<_>>()).cloned().collect();
                        },
                    }
                    if result.len() == 0 { break; }
                }
                result.into_iter().map(|x| usize_to_vec(x.clone())).collect()
            }
        }
    }

    pub fn sunion(&self, sets: Vec<&ValueSet>) -> HashSet<Vec<u8>> {
        let mut result: HashSet<Vec<u8>> = match *self {
            ValueSet::Data(ref original_set) => original_set.clone(),
            ValueSet::Integer(ref set) => set.iter().map(|x| usize_to_vec(x.clone())).collect::<HashSet<_>>(),
        };
        for newvalue in sets {
            match *newvalue {
                ValueSet::Integer(ref set) => {
                    result = result.union(&set.iter().map(|x| usize_to_vec(x.clone())).collect::<HashSet<_>>()).cloned().collect();
                },
                ValueSet::Data(ref set) => {
                    result = result.union(set).cloned().collect();
                },
            }
        }
        result
    }

    pub fn dump<T: Write>(&self, writer: &mut T) -> io::Result<usize> {
        let mut v = vec![];
        let settype;
        match *self {
            ValueSet::Integer(ref set) => {
                settype = TYPE_SET_INTSET;
                let max = set.iter().max().unwrap().clone();
                let encoding = if max <= 0xff {
                    2
                } else if max <= 0xffff {
                    4
                } else if max <= 0xffffffff {
                    8
                } else {
                    panic!("Set element is too large")
                };

                let mut tmp = vec![];
                encode_u32_to_slice_u8(encoding, &mut tmp).unwrap();
                encode_u32_to_slice_u8(set.len() as u32, &mut tmp).unwrap();
                for item in set.iter() {
                    let r = match encoding {
                        2 => encode_u16_to_slice_u8(item.clone() as u16, &mut tmp),
                        4 => encode_u32_to_slice_u8(item.clone() as u32, &mut tmp),
                        8 => encode_u64_to_slice_u8(item.clone() as u64, &mut tmp),
                        _ => panic!("Unexpected encoding {}", encoding),
                    };
                    match r {
                        Ok(_) => (),
                        Err(err) => match err {
                            EncodeError::IOError(err) => return Err(err),
                            _ => panic!("Unexpected error {:?}", err),
                        }
                    }
                }
                encode_len(tmp.len(), &mut v).unwrap();
                v.extend(tmp);
            }
            ValueSet::Data(ref set) => {
                settype = TYPE_SET;
                encode_len(set.len(), &mut v).unwrap();
                for ref item in set {
                    try!(encode_slice_u8(&*item, &mut v, true));
                }
            }
        };
        let data = [
            vec![settype],
            v,
            vec![(VERSION & 0xff) as u8],
            vec![((VERSION >> 8) & 0xff) as u8],
        ].concat();
        writer.write(&*data)
    }

    pub fn debug_object(&self) -> String {
        let mut serialized_data = vec![];
        let serialized = self.dump(&mut serialized_data).unwrap();
        let encoding = match *self {
            ValueSet::Integer(_) => "intset",
            ValueSet::Data(_) => "hashtable",
        };
        format!("Value at:0x0000000000 refcount:1 encoding:{} serializedlength:{} lru:0 lru_seconds_idle:0", encoding, serialized).to_owned()
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
        assert!(set.sadd(b"123".to_vec(), 100));
        assert!(set.is_intset());
        assert!(set.sadd(b"456".to_vec(), 100));
        assert!(set.is_intset());
        assert!(!set.sadd(b"123".to_vec(), 100));
        assert!(set.is_intset());
        assert!(set.sadd(b"foo".to_vec(), 100));
        assert!(!set.is_intset());
    }

    #[test]
    fn intset_srem() {
        let mut set = ValueSet::new();
        assert!(set.is_intset());
        assert!(set.sadd(b"123".to_vec(), 100));
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
        assert!(set.sadd(b"123".to_vec(), 100));
        assert!(set.sadd(b"456".to_vec(), 100));
        assert!(set.sismember(&b"123".to_vec()));
        assert!(set.sismember(&b"456".to_vec()));
        assert!(!set.sismember(&b"foo".to_vec()));
        assert!(!set.sismember(&b"124".to_vec()));
    }

    #[test]
    fn intset_scard() {
        let mut set = ValueSet::new();
        assert_eq!(set.scard(), 0);
        assert!(set.sadd(b"123".to_vec(), 100));
        assert_eq!(set.scard(), 1);
        assert!(set.sadd(b"456".to_vec(), 100));
        assert_eq!(set.scard(), 2);
    }

    #[test]
    fn intset_smembers() {
        let mut set = ValueSet::new();
        assert!(set.sadd(b"123".to_vec(), 100));
        assert!(set.sadd(b"456".to_vec(), 100));
        assert!(set.smembers() == vec![b"123".to_vec(), b"456".to_vec()] ||
                set.smembers() == vec![b"456".to_vec(), b"123".to_vec()]);
    }

    #[test]
    fn intset_spop() {
        let mut set = ValueSet::new();
        assert!(set.sadd(b"123".to_vec(), 100));
        assert!(set.sadd(b"456".to_vec(), 100));
        let mut h = HashSet::new();
        h.insert(set.spop(1).pop().unwrap());
        h.insert(set.spop(1).pop().unwrap());
        assert_eq!(set.spop(1).len(), 0);
        assert_eq!(h, HashSet::from_iter(vec![b"123".to_vec(), b"456".to_vec()]));
    }

    #[test]
    fn intset_sdiff() {
        let mut set = ValueSet::new();
        assert!(set.sadd(b"123".to_vec(), 100));
        assert!(set.sadd(b"456".to_vec(), 100));
        assert!(set.sadd(b"789".to_vec(), 100));

        let mut set2 = ValueSet::new();
        assert!(set2.sadd(b"123".to_vec(), 100));

        let mut set3 = ValueSet::new();
        assert!(set3.sadd(b"456".to_vec(), 100));
        assert!(set3.sadd(b"foo".to_vec(), 100));

        assert_eq!(set.sdiff(vec![&set2, &set3]), HashSet::from_iter(vec![b"789".to_vec()]));
    }

    #[test]
    fn intset_sinter() {
        let mut set = ValueSet::new();
        assert!(set.sadd(b"bar".to_vec(), 100));
        assert!(set.sadd(b"123".to_vec(), 100));
        assert!(set.sadd(b"456".to_vec(), 100));
        assert!(set.sadd(b"789".to_vec(), 100));

        let mut set2 = ValueSet::new();
        assert!(set2.sadd(b"123".to_vec(), 100));
        assert!(set2.sadd(b"456".to_vec(), 100));

        let mut set3 = ValueSet::new();
        assert!(set3.sadd(b"123".to_vec(), 100));
        assert!(set3.sadd(b"456".to_vec(), 100));
        assert!(set3.sadd(b"foo".to_vec(), 100));

        assert_eq!(set.sinter(vec![&set2, &set3]), HashSet::from_iter(vec![
                    b"123".to_vec(),
                    b"456".to_vec(),
                    ]));
    }

    #[test]
    fn intset_sinter2() {
        let mut set = ValueSet::new();
        assert!(set.sadd(b"123".to_vec(), 100));
        assert!(set.sadd(b"456".to_vec(), 100));
        assert!(set.sadd(b"789".to_vec(), 100));

        let mut set2 = ValueSet::new();
        assert!(set2.sadd(b"123".to_vec(), 100));
        assert!(set2.sadd(b"456".to_vec(), 100));

        let mut set3 = ValueSet::new();
        assert!(set3.sadd(b"123".to_vec(), 100));
        assert!(set3.sadd(b"456".to_vec(), 100));
        assert!(set3.sadd(b"foo".to_vec(), 100));

        assert_eq!(set.sinter(vec![&set2, &set3]), HashSet::from_iter(vec![
                    b"123".to_vec(),
                    b"456".to_vec(),
                    ]));
    }

    #[test]
    fn intset_sunion() {
        let mut set = ValueSet::new();
        assert!(set.sadd(b"bar".to_vec(), 100));
        assert!(set.sadd(b"123".to_vec(), 100));

        let mut set2 = ValueSet::new();
        assert!(set2.sadd(b"123".to_vec(), 100));
        assert!(set2.sadd(b"456".to_vec(), 100));

        let mut set3 = ValueSet::new();
        assert!(set3.sadd(b"456".to_vec(), 100));
        assert!(set3.sadd(b"foo".to_vec(), 100));

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
        assert!(set.sadd(b"123".to_vec(), 100));

        let mut set2 = ValueSet::new();
        assert!(set2.sadd(b"456".to_vec(), 100));

        let mut set3 = ValueSet::new();
        assert!(set3.sadd(b"456".to_vec(), 100));

        assert_eq!(set.sunion(vec![&set2, &set3]), HashSet::from_iter(vec![
                    b"123".to_vec(),
                    b"456".to_vec(),
                    ]));
    }

    #[test]
    fn dump_string_set() {
        let mut v = vec![];
        let mut set = ValueSet::new();
        for item in [b"a", b"b"].iter() {
            set.sadd(item.to_vec(), 0);
        }
        set.dump(&mut v).unwrap();
        assert!(v == b"\x02\x02\x01a\x01b\x07\x00".to_vec() ||
                v == b"\x02\x02\x01b\x01a\x07\x00".to_vec());
    }

    #[test]
    fn dump_int_set() {
        let mut v = vec![];
        let mut set = ValueSet::new();
        for item in [b"1", b"2"].iter() {
            set.sadd(item.to_vec(), 10);
        }
        set.dump(&mut v).unwrap();
        assert!(v == b"\x0b\x0c\x02\x00\x00\x00\x02\x00\x00\x00\x01\x00\x02\x00\x07\x00".to_vec() ||
                v == b"\x0b\x0c\x02\x00\x00\x00\x02\x00\x00\x00\x02\x00\x01\x00\x07\x00".to_vec());
    }

    #[test]
    fn create_numeric() {
        let s = ValueSet::create_with_hashset(HashSet::from_iter(vec![b"319".to_vec()]));
        assert!(s.is_intset());
    }
}
