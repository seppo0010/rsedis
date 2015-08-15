use std::io;
use std::io::Write;
use std::str::from_utf8;

use basichll::HLL;
use dbutil::normalize_position;
use error::OperationError;
use rdbutil::constants::*;
use rdbutil::{EncodeError, encode_i64, encode_slice_u8};

const HLL_ERROR:f64 = 0.0019;

#[derive(PartialEq, Debug, Clone)]
pub enum ValueString {
    Integer(i64),
    Data(Vec<u8>),
}

fn to_i64(newvalue: &Vec<u8>) -> Option<i64> {
    if newvalue.len() > 0 && newvalue.len() < 32 { // ought to be enough!
        if newvalue[0] as char != '0' && newvalue[0] as char != ' ' {
            if let Ok(utf8) = from_utf8(&*newvalue) {
                if let Ok(i) = utf8.parse::<i64>() {
                    return Some(i);
                }
            }
        }
    }
    return None;
}

fn to_f64(newvalue: &Vec<u8>) -> Option<f64> {
    if newvalue.len() > 0 && newvalue.len() < 32 { // ought to be enough!
        if newvalue[0] as char != '0' && newvalue[0] as char != ' ' {
            if let Ok(utf8) = from_utf8(&*newvalue) {
                if let Ok(f) = utf8.parse::<f64>() {
                    return Some(f);
                }
            }
        }
    }
    return None;
}

impl ValueString {
    pub fn new(newvalue: Vec<u8>) -> Self {
        match to_i64(&newvalue) {
            Some(i) => ValueString::Integer(i),
            None => ValueString::Data(newvalue),
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        match *self {
            ValueString::Data(ref data) => data.clone(),
            ValueString::Integer(ref int) => format!("{}", int).into_bytes(),
        }
    }

    pub fn strlen(&self) -> usize {
        match *self {
            ValueString::Data(ref data) => data.len(),
            ValueString::Integer(ref int) => format!("{}", int).len(),
        }
    }

    pub fn append(&mut self, newvalue: Vec<u8>) {
        match *self {
            ValueString::Data(ref mut data) => data.extend(newvalue),
            ValueString::Integer(i) => {
                let oldstr = format!("{}", i);
                *self = ValueString::new([oldstr.into_bytes(), newvalue].concat());
            },
        };
    }

    pub fn incr(&mut self, incr: i64) -> Result<i64, OperationError> {
        let val = match *self {
            ValueString::Integer(i) => i,
            ValueString::Data(ref data) => {
                match to_i64(data) {
                    Some(i) => i,
                    None => return Err(OperationError::ValueError("ERR value is not a valid integer".to_owned())),
                }
            },
        };
        let newval = try!(val.checked_add(incr).ok_or(OperationError::OverflowError));
        *self = ValueString::Integer(newval.clone());
        Ok(newval)
    }

    pub fn incrbyfloat(&mut self, incr: f64) -> Result<f64, OperationError> {
        let val = match *self {
            ValueString::Integer(i) => i as f64,
            ValueString::Data(ref data) => {
                match to_f64(data) {
                    Some(f) => f,
                    None => return Err(OperationError::ValueError("ERR value is not a valid float".to_owned())),
                }
            },
        };
        let newval = val + incr;
        *self = ValueString::Data(format!("{}", newval).into_bytes());
        Ok(newval)
    }

    pub fn getrange(&self, _start: i64, _stop: i64) -> Vec<u8> {
        let s = match *self {
            ValueString::Integer(ref i) => format!("{}", i).into_bytes(),
            ValueString::Data(ref s) => s.clone(),
        };

        let len = s.len();
        let start = match normalize_position(_start, len) {
            Ok(i) => i,
            Err(g) => if !g { 0 } else { return Vec::new(); }
        } as usize;
        let stop = match normalize_position(_stop, len) {
            Ok(i) => i,
            Err(g) => if !g { return Vec::new(); } else { len - 1 }
        } as usize;

        if stop < start {
            return Vec::new();
        }

        let mut v = Vec::with_capacity(stop - start + 1);
        v.extend(s[start..stop + 1].iter());
        v
    }

    pub fn setbit(&mut self, bitoffset: usize, on: bool) -> bool {
        match *self {
            ValueString::Integer(i) => *self = ValueString::Data(format!("{}", i).into_bytes()),
            ValueString::Data(_) => (),
        };
        let mut d = match *self {
            ValueString::Data(ref mut d) => d,
            _ => panic!("Value should be data"),
        };

        let byte = bitoffset >> 3;

        while byte + 1 > d.len() {
            d.push(0);
        }

        let mut byteval = d[byte];
        let bit = 7 - (bitoffset & 0x7);
        let bitval = byteval & (1 << bit);

        byteval &= !(1 << bit);
        byteval |= (if on { 1 } else { 0 } & 0x1) << bit;
        d[byte] = byteval;

        bitval != 0
    }

    pub fn getbit(&self, bitoffset: usize) -> bool {
        let tmp;
        let d = match *self {
            ValueString::Integer(i) => { tmp = format!("{}", i).into_bytes(); &tmp },
            ValueString::Data(ref d) => d,
        };

        let byte = bitoffset >> 3;
        if byte >= d.len() {
            return false;
        }

        let bit = 7 - (bitoffset & 0x7);;
        let bitval = d[byte] & (1 << bit);

        bitval != 0
    }

    pub fn setrange(&mut self, _index: usize, data: Vec<u8>) -> usize {
        if data.len() == 0 {
            return self.strlen();
        }

        match *self {
            ValueString::Integer(i) => *self = ValueString::Data(format!("{}", i).into_bytes()),
            ValueString::Data(_) => (),
        }

        let mut d = match self {
            &mut ValueString::Data(ref mut s) => s,
            _ => panic!("String must be data"),
        };

        let mut index = _index;
        for _ in d.len()..index {
            d.push(0);
        }
        for c in data {
            d.push(c);
            if index < d.len() - 1 {
                d.swap_remove(index);
            }
            index += 1;
        }
        d.len()
    }

    pub fn pfadd(&mut self, data: Vec<Vec<u8>>) -> Result<bool, OperationError> {
        let mut changed = false;
        let mut hll = if self.strlen() == 0 {
            HLL::new(HLL_ERROR)
        } else {
            try!(HLL::from_vec(self.to_vec()))
        };
        for el in data {
            changed = hll.insert(&el) || changed;
        }
        if changed {
            *self = ValueString::new(try!(hll.into_vec()));
        }
        Ok(changed)
    }

    pub fn pfcount(&self) -> Result<usize, OperationError> {
        let hll = if self.strlen() == 0 {
            return Ok(0);
        } else {
            try!(HLL::from_vec(self.to_vec()))
        };
        Ok(hll.count().round() as usize)
    }

    pub fn pfmerge(&mut self, data: Vec<&ValueString>) -> Result<(), OperationError> {
        let mut hll = if self.strlen() == 0 {
            HLL::new(HLL_ERROR)
        } else {
            try!(HLL::from_vec(self.to_vec()))
        };

        for s in data {
            hll = &hll + &try!(HLL::from_vec(s.to_vec()));
        }

        *self = ValueString::new(try!(hll.into_vec()));
        Ok(())
    }

    pub fn dump<T: Write>(&self, writer: &mut T) -> io::Result<usize> {
        let mut v = vec![];
        match *self {
            ValueString::Integer(ref i) => match encode_i64(i.clone(), &mut v) {
                Ok(s) => s,
                Err(e) => match e {
                    EncodeError::IOError(e) => return Err(e),
                    EncodeError::OverflowError => try!(encode_slice_u8(&*self.to_vec(), &mut v, false))
                }
            },
            ValueString::Data(ref d) => try!(encode_slice_u8(&*d, &mut v, true)),
        };
        let data = [
            vec![TYPE_STRING],
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
            ValueString::Integer(_) => "int",
            ValueString::Data(_) => "raw",
        };
        format!("Value at:0x0000000000 refcount:1 encoding:{} serializedlength:{} lru:0 lru_seconds_idle:0", encoding, serialized).to_owned()
    }
}

#[cfg(test)]
mod test_rdb {
    use std::i64;

    use super::ValueString;

    #[test]
    fn dump_integer() {
        let mut v = vec![];
        ValueString::Integer(1).dump(&mut v).unwrap();
        assert_eq!(&*v, b"\x00\xc0\x01\x07\x00");
    }

    #[test]
    fn dump_integer_overflow() {
        let mut v = vec![];
        ValueString::Integer(i64::MAX).dump(&mut v).unwrap();
        assert_eq!(&*v, b"\x00\x139223372036854775807\x07\x00");
    }

    #[test]
    fn dump_string() {
        let mut v = vec![];
        ValueString::Data(b"hello world".to_vec()).dump(&mut v).unwrap();
        assert_eq!(&*v, b"\x00\x0bhello world\x07\x00");
    }
}
