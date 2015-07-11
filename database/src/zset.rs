use std::cmp::Ordering;
use std::collections::Bound;
use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::f64::{INFINITY, NEG_INFINITY, NAN};

use skiplist::OrderedSkipList;

use dbutil::normalize_position;
use rdbutil::constants::*;
use rdbutil::{encode_slice_u8, encode_len};

/**
 * SortedSetMember is a wrapper around f64 to implement ordering and equality.
 * f64 does not implement those traits because comparing floats has problems
 * but in the context of rsedis this basic implementation should be enough.
 **/
#[derive(Debug, Clone)]
pub struct SortedSetMember {
    f: f64,
    s: Vec<u8>,
    // this is useful for inclusion/exclusion comparison
    // if true, it will ignore `s` and be the highest possible string
    upper_boundary: bool,
}

impl SortedSetMember {
    pub fn new(f: f64, s: Vec<u8>) -> SortedSetMember {
        SortedSetMember {f: f, s: s, upper_boundary: false}
    }

    pub fn set_upper_boundary(&mut self, upper_boundary: bool) {
        self.upper_boundary = upper_boundary;
    }

    pub fn get_f64(&self) -> &f64 {
        &self.f
    }

    pub fn set_f64(&mut self, f: f64)  {
        self.f = f;
    }

    pub fn get_vec(&self) -> &Vec<u8> {
        &self.s
    }

    pub fn set_vec(&mut self, s: Vec<u8>)  {
        self.s = s;
    }
}

impl Eq for SortedSetMember {}

impl PartialEq for SortedSetMember {
    fn eq(&self, other: &Self) -> bool {
        self.f == other.f && self.s == other.s
    }
}

impl Ord for SortedSetMember {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialOrd for SortedSetMember {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(if self.f < other.f { Ordering::Less }
        else if self.f > other.f { Ordering::Greater }
        else if other.upper_boundary { Ordering::Less }
        else if self.upper_boundary { Ordering::Greater }
        else { return self.s.partial_cmp(&other.s) })
    }

    fn lt(&self, other: &Self) -> bool { self.f < other.f || (self.f == other.f && self.s < other.s) }
    fn le(&self, other: &Self) -> bool { self.f < other.f || (self.f == other.f && self.s <= other.s) }
    fn gt(&self, other: &Self) -> bool { self.f > other.f || (self.f == other.f && self.s > other.s) }
    fn ge(&self, other: &Self) -> bool { self.f > other.f || (self.f == other.f && self.s >= other.s) }
}

#[derive(PartialEq, Debug)]
pub enum ValueSortedSet {
    // FIXME: Vec<u8> is repeated in memory
    Data(OrderedSkipList<SortedSetMember>, HashMap<Vec<u8>, f64>),
}

impl ValueSortedSet {
    pub fn new() -> Self {
        let skiplist = OrderedSkipList::new();
        let hmap = HashMap::new();
        ValueSortedSet::Data(skiplist, hmap)
    }

    pub fn zadd(&mut self, s: f64, el: Vec<u8>, nx: bool, xx: bool, ch: bool, incr: bool) -> bool {
        match *self {
            ValueSortedSet::Data(ref mut skiplist, ref mut hmap) => {
                let mut score = s.clone();
                let contains = hmap.contains_key(&el);
                if contains && nx {
                    return false;
                }
                if !contains && xx {
                    return false;
                }
                if contains {
                    let val = hmap.get(&el).unwrap();
                    if ch && !incr && val == &s {
                        return false;
                    }
                    skiplist.remove(&SortedSetMember::new(val.clone(), el.clone()));
                    if incr {
                        score += val.clone();
                    }
                }
                skiplist.insert(SortedSetMember::new(score.clone(), el.clone()));
                hmap.insert(el, score);
                if ch {
                    true
                } else {
                    !contains
                }
            },
        }
    }

    pub fn zcard(&self) -> usize {
        match *self {
            ValueSortedSet::Data(_, ref hmap) => hmap.len()
        }
    }

    pub fn zscore(&self, element: &Vec<u8>) -> Option<f64> {
        match *self {
            ValueSortedSet::Data(_, ref hmap) => hmap.get(element).map(|x| x.clone()),
        }
    }

    pub fn zincrby(&mut self, increment: f64, member: Vec<u8>) -> f64 {
        match *self {
            ValueSortedSet::Data(ref mut skiplist, ref mut hmap) => {
                let mut val = match hmap.get(&member) {
                    Some(val) => {
                        skiplist.remove(&SortedSetMember::new(val.clone(), member.clone()));
                        val.clone()
                    },
                    None => 0.0,
                };
                val += increment;
                skiplist.insert(SortedSetMember::new(val.clone(), member.clone()));
                hmap.insert(member, val.clone());
                val
            },
        }
    }

    fn rangebyscore(&self, min: Bound<f64>, max: Bound<f64>) -> Vec<&SortedSetMember> {
        let skiplist = match *self {
            ValueSortedSet::Data(ref skiplist, _) => skiplist,
        };
        let mut f1 = SortedSetMember::new(0.0, vec![]);
        let mut f2 = SortedSetMember::new(0.0, vec![]);
        let m1 = match min {
            Bound::Included(f) => { f1.set_f64(f); Bound::Included(&f1) },
            Bound::Excluded(f) => { f1.set_f64(f); f1.set_upper_boundary(true); Bound::Excluded(&f1) },
            Bound::Unbounded => Bound::Unbounded,
        };

        let m2 = match max {
            Bound::Included(f) => { f2.set_f64(f); f2.set_upper_boundary(true); Bound::Included(&f2) },
            Bound::Excluded(f) => { f2.set_f64(f); Bound::Excluded(&f2) },
            Bound::Unbounded => Bound::Unbounded,
        };

        skiplist.range(m1, m2).collect::<Vec<_>>()
    }

    pub fn zcount(&self, min: Bound<f64>, max: Bound<f64>) -> usize {
        self.rangebyscore(min, max).len()
    }

    pub fn zrem(&mut self, member: Vec<u8>) -> bool {
        let (skiplist, hmap) = match *self {
            ValueSortedSet::Data(ref mut skiplist, ref mut hmap) => (skiplist, hmap),
        };
        let score = match hmap.remove(&member) {
            Some(val) => val,
            None => return false,
        };
        skiplist.remove(&SortedSetMember::new(score, member));
        true
    }

    pub fn zremrangebyscore(&mut self, min: Bound<f64>, max: Bound<f64>) -> usize {
        let pos = match min {
            Bound::Included(ref s) => self.zcount(Bound::Unbounded, Bound::Excluded(s.clone())),
            Bound::Excluded(ref s) => self.zcount(Bound::Unbounded, Bound::Included(s.clone())),
            Bound::Unbounded => 0,
        };
        let count = self.zcount(min, max);
        let (skiplist, hmap) = match *self {
            ValueSortedSet::Data(ref mut skiplist, ref mut hmap) => (skiplist, hmap),
        };

        for _ in 0..count {
            let el = skiplist.remove_index(&pos);
            hmap.remove(&el.s);
        };
        count
    }

    fn normalize_range(&self, start: i64, stop: i64, rev: bool) -> (usize, usize) {
        let skiplist = match *self {
            ValueSortedSet::Data(ref skiplist, _) => skiplist,
        };

        let len = skiplist.len();
        if rev {
            (match normalize_position(- stop - 1, len) {
                Ok(i) => i,
                Err(g) => if !g { 0 } else { return (1, 0); },
            },
            match normalize_position(- start - 1, len) {
                Ok(i) => i,
                Err(g) => if !g { return (1, 0); } else { len },
            })
        } else {
            (match normalize_position(start, len) {
                Ok(i) => i,
                Err(g) => if !g { 0 } else { return (1, 0); },
            },
            match normalize_position(stop, len) {
                Ok(i) => i,
                Err(g) => if !g { return (1, 0); } else { len },
            })
        }
    }

    pub fn zremrangebyrank(&mut self, _start: i64, _stop: i64) -> usize {
        let (start, stop) = self.normalize_range(_start, _stop, false);
        if stop < start {
            return 0;
        }

        let (skiplist, hmap) = match *self {
            ValueSortedSet::Data(ref mut skiplist, ref mut hmap) => (skiplist, hmap),
        };

        for _ in 0..(stop - start + 1) {
            let el = skiplist.remove_index(&start);
            hmap.remove(&el.s);
        };
        stop - start + 1
    }

    pub fn zrange(&self, _start: i64, _stop: i64, withscores: bool, rev: bool) -> Vec<Vec<u8>> {
        let skiplist = match *self {
            ValueSortedSet::Data(ref skiplist, _) => skiplist,
        };

        let (start, stop) = self.normalize_range(_start, _stop, rev);
        if stop < start {
            return vec![];
        }

        let first = skiplist.get(&start).unwrap();
        let mut r = vec![];
        if rev {
            for member in skiplist.range(Bound::Included(first), Bound::Unbounded).take(stop - start + 1) {
                if withscores {
                    r.push(format!("{}", member.get_f64()).into_bytes());
                }
                r.push(member.get_vec().clone());
            }
            r = r.iter().rev().cloned().collect::<Vec<_>>();
        } else {
            for member in skiplist.range(Bound::Included(first), Bound::Unbounded).take(stop - start + 1) {
                r.push(member.get_vec().clone());
                if withscores {
                    r.push(format!("{}", member.get_f64()).into_bytes());
                }
            }
        }
        r
    }

    pub fn zrangebyscore(&self, _min: Bound<f64>, _max: Bound<f64>, withscores: bool, offset: usize, count: usize, rev: bool) -> Vec<Vec<u8>> {
        let skiplist = match *self {
            ValueSortedSet::Data(ref skiplist, _) => skiplist,
        };

        // FIXME: duplicated code from ZCOUNT. Trying to create a factory
        // function for this, but I failed because allocation was going
        // out of scope.
        // Probably more function will copy this until I can figure out
        // a better way.
        let mut f1 = SortedSetMember::new(0.0, vec![]);
        let mut f2 = SortedSetMember::new(0.0, vec![]);

        let (min, max) = if rev {
            (_max, _min)
        } else {
            (_min, _max)
        };

        let m1 = match min {
            Bound::Included(f) => { f1.set_f64(f); Bound::Included(&f1) },
            Bound::Excluded(f) => { f1.set_f64(f); f1.set_upper_boundary(true); Bound::Excluded(&f1) },
            Bound::Unbounded => Bound::Unbounded,
        };

        let m2 = match max {
            Bound::Included(f) => { f2.set_f64(f); f2.set_upper_boundary(true); Bound::Included(&f2) },
            Bound::Excluded(f) => { f2.set_f64(f); Bound::Excluded(&f2) },
            Bound::Unbounded => Bound::Unbounded,
        };

        let mut r = vec![];
        if rev {
            let len = skiplist.len();
            let mut c = count;
            if c + offset > len {
                c = len - offset;
            }
            for member in skiplist.range(m1, m2).rev().skip(offset).take(c) {
                r.push(member.get_vec().clone());
                if withscores {
                    r.push(format!("{}", member.get_f64()).into_bytes());
                }
            }
            r.iter().cloned().collect::<Vec<_>>()
        } else {
            for member in skiplist.range(m1, m2).skip(offset).take(count) {
                r.push(member.get_vec().clone());
                if withscores {
                    r.push(format!("{}", member.get_f64()).into_bytes());
                }
            }
            r
        }
    }

    pub fn zrank(&self, el: Vec<u8>) -> Option<usize> {
        let (skiplist, hashmap) = match *self {
            ValueSortedSet::Data(ref skiplist, ref hashmap) => (skiplist, hashmap),
        };

        let score = match hashmap.get(&el) {
            Some(s) => s,
            None => return None,
        };

        let member = SortedSetMember::new(score.clone(), el);
        return Some(skiplist.range(Bound::Unbounded, Bound::Included(&member)).collect::<Vec<_>>().len() - 1);
    }

    pub fn dump<T: Write>(&self, writer: &mut T) -> io::Result<usize> {
        let mut v = vec![];
        let settype;
        match *self {
            ValueSortedSet::Data(_, ref hash) => {
                settype = TYPE_ZSET;
                encode_len(hash.len(), &mut v).unwrap();
                for (value, score) in hash {
                    try!(encode_slice_u8(&*value, &mut v, true));
                    if score == &NAN {
                        try!(v.write(&[253]));
                    } else if score == &INFINITY {
                        try!(v.write(&[254]));
                    } else if score == &NEG_INFINITY {
                        try!(v.write(&[255]));
                    } else {
                        let scorestr = format!("{}", score.abs()).to_owned();
                        try!(encode_slice_u8(scorestr.as_bytes(), &mut v, false));
                    }
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
}

#[test]
fn dump_zset() {
    let mut v = vec![];
    let mut zset = ValueSortedSet::new();
    zset.zadd(1.0, b"a".to_vec(), false, false, false, false);
    zset.zadd(2.0, b"b".to_vec(), false, false, false, false);
    zset.dump(&mut v).unwrap();
    assert!(v == b"\x03\x02\x01b\x012\x01a\x011\x07\x00".to_vec() ||
            v == b"\x03\x02\x01a\x011\x01b\x012\x07\x00".to_vec());
}

#[test]
fn zremrangebyscore() {
    let mut zset = ValueSortedSet::new();
    zset.zadd(1.0, b"a".to_vec(), false, false, false, false);
    zset.zadd(2.0, b"b".to_vec(), false, false, false, false);
    zset.zadd(3.0, b"c".to_vec(), false, false, false, false);
    zset.zadd(4.0, b"d".to_vec(), false, false, false, false);
    assert_eq!(zset.zremrangebyscore(Bound::Included(2.0), Bound::Excluded(4.0)), 2);
    assert_eq!(zset.zrank(b"a".to_vec()).unwrap(), 0);
    assert_eq!(zset.zrank(b"c".to_vec()), None);
    assert_eq!(zset.zremrangebyscore(Bound::Unbounded, Bound::Unbounded), 2);
    assert_eq!(zset.zrank(b"a".to_vec()), None);
    assert_eq!(zset.zremrangebyscore(Bound::Unbounded, Bound::Unbounded), 0);
}

#[test]
fn zremrangebyrank() {
    let mut zset = ValueSortedSet::new();
    zset.zadd(1.0, b"a".to_vec(), false, false, false, false);
    zset.zadd(2.0, b"b".to_vec(), false, false, false, false);
    zset.zadd(3.0, b"c".to_vec(), false, false, false, false);
    zset.zadd(4.0, b"d".to_vec(), false, false, false, false);
    assert_eq!(zset.zremrangebyrank(1, -2), 2);
    assert_eq!(zset.zrank(b"a".to_vec()).unwrap(), 0);
    assert_eq!(zset.zrank(b"c".to_vec()), None);
    assert_eq!(zset.zremrangebyrank(0, -1), 2);
    assert_eq!(zset.zrank(b"a".to_vec()), None);
    assert_eq!(zset.zremrangebyrank(0, -1), 0);
}
