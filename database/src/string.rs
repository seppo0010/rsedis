use std::io;
use std::io::Write;
use std::str::from_utf8;

use dbutil::normalize_position;
use error::OperationError;
use rdbutil::constants::*;
use rdbutil::{EncodeError, encode_i64, encode_slice_u8};

#[derive(PartialEq, Debug, Clone)]
pub enum ValueString {
    Integer(i64),
    Data(Vec<u8>),
}

impl ValueString {
    pub fn new(newvalue: Vec<u8>) -> Self {
        if newvalue.len() < 32 { // ought to be enough!
            if let Ok(utf8) = from_utf8(&*newvalue) {
                if let Ok(i) = utf8.parse::<i64>() {
                    return ValueString::Integer(i);
                }
            }
        }
        return ValueString::Data(newvalue);
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
                if data.len() > 32 {
                    return Err(OperationError::OverflowError);
                }
                let res = try!(from_utf8(&data));
                try!(res.parse::<i64>())
            },
        };
        let newval = try!(val.checked_add(incr).ok_or(OperationError::OverflowError));
        *self = ValueString::Integer(newval.clone());
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
            Err(g) => if !g { return Vec::new(); } else { len }
        } as usize;
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

    pub fn setrange(&mut self, _index: i64, data: Vec<u8>) -> usize {
        match *self {
            ValueString::Integer(i) => *self = ValueString::Data(format!("{}", i).into_bytes()),
            ValueString::Data(_) => (),
        }

        let mut d = match self {
            &mut ValueString::Data(ref mut s) => s,
            _ => panic!("String must be data"),
        };

        let mut index = match normalize_position(_index, d.len()) {
            Ok(i) => i,
            Err(g) => if !g { 0 } else { _index as usize },
        };
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
