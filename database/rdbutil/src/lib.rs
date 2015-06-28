extern crate util;
pub mod constants;

use std::u32;
use std::i64;
use std::io;
use std::str::from_utf8;
#[cfg(test)] use std::usize;

use util::htonl;

use constants::*;

#[derive(Debug)]
pub enum EncodeError {
    IOError(io::Error),
    OverflowError,
}

impl From<io::Error> for EncodeError {
    fn from(err: io::Error) -> Self {
        EncodeError::IOError(err)
    }
}

// I wish I didn't have to specify i64 and it can be a generic type instead
// But... I don't know how can it be specified that a generic can be compared
// with hardcoded numbers (e.g.: -(1 << 7)) or what would happen with unsigned
// numbers.
// For now, the caller will have to explicitely cast or implement a wrapper function
pub fn encode_i64<W: io::Write>(value: i64, enc: &mut W) -> Result<usize, EncodeError> {
    Ok(try!(if value >= -(1 << 7) && value <= (1 << 7) - 1 {
        enc.write(
                &[
                    (ENCVAL << 6) | ENC_INT8,
                    (value & 0xFF) as u8
                ])
    } else if value >= -(1 << 15) && value <= (1 << 15) - 1 {
        enc.write(&[
                (ENCVAL << 6) | ENC_INT16,
                (value & 0xFF) as u8,
                ((value >> 8) & 0xFF) as u8,
        ])
    } else if value >= -(1 << 31) && value <= (1 << 31) - 1 {
        enc.write(&[
                (ENCVAL<<6) | ENC_INT32,
                (value & 0xFF) as u8,
                ((value >> 8) & 0xFF) as u8,
                ((value >> 16) & 0xFF) as u8,
                ((value >> 24) & 0xFF) as u8,
        ])
    } else {
        return Err(EncodeError::OverflowError);
    }))
}

pub fn encode_u8<W: io::Write>(value: u8, enc: &mut W) -> Result<usize, EncodeError> {
    encode_i64(value as i64, enc)
}

pub fn encode_u16<W: io::Write>(value: u16, enc: &mut W) -> Result<usize, EncodeError> {
    encode_i64(value as i64, enc)
}

pub fn encode_u32<W: io::Write>(value: u32, enc: &mut W) -> Result<usize, EncodeError> {
    encode_i64(value as i64, enc)
}

pub fn encode_i8<W: io::Write>(value: i8, enc: &mut W) -> Result<usize, EncodeError> {
    encode_i64(value as i64, enc)
}

pub fn encode_i16<W: io::Write>(value: i16, enc: &mut W) -> Result<usize, EncodeError> {
    encode_i64(value as i64, enc)
}

pub fn encode_i32<W: io::Write>(value: i32, enc: &mut W) -> Result<usize, EncodeError> {
    encode_i64(value as i64, enc)
}

pub fn encode_usize<W: io::Write>(value: usize, enc: &mut W) -> Result<usize, EncodeError> {
    if value > i64::MAX as usize {
        return Err(EncodeError::OverflowError)
    }
    Ok(try!(encode_i64(value as i64, enc)))
}

pub fn u64_to_slice_u8(value: u64) -> [u8; 8] {
    [
            (value & 0xFF) as u8,
            ((value >> 8) & 0xFF) as u8,
            ((value >> 16) & 0xFF) as u8,
            ((value >> 24) & 0xFF) as u8,
            ((value >> 32) & 0xFF) as u8,
            ((value >> 40) & 0xFF) as u8,
            ((value >> 48) & 0xFF) as u8,
            ((value >> 56) & 0xFF) as u8,
    ]
}

pub fn encode_len<W: io::Write>(len: usize, enc: &mut W) -> Result<usize, EncodeError> {
    if len > u32::MAX as usize {
        panic!("Length does not fit in four bytes");
    }

    if len < (1 << 6) {
        Ok(try!(enc.write(&[((len & 0xFF) as u8) | (BITLEN6 << 6)])))
    } else if len < (1 << 14) {
        Ok(try!(enc.write(&[
                (((len >> 8) as u8) & 0xFF) | (BITLEN14 << 6),
                (len & 0xFF) as u8,
        ])))
    } else {
        let mut s = try!(enc.write(&[BITLEN32 << 6]));
        s += try!(enc.write(&htonl(len as u32)));
        Ok(s)
    }
}

pub fn encode_slice_u8<W: io::Write>(data: &[u8], enc: &mut W) -> Result<usize, io::Error> {
    if data.len() <= 11 {
        if let Ok(s) = from_utf8(data) {
            if let Ok(i) = s.parse::<i64>() {
                if let Ok(w) = encode_i64(i, enc) {
                    return Ok(w);
                }
            }
        }
    }

    // TODO: lzf compression

    let mut len = encode_len(data.len(), enc).unwrap();
    len += try!(enc.write(data));
    Ok(len)
}

#[test]
fn test_encode_i64() {
    let mut v = vec![];
    assert_eq!(encode_i64(1, &mut v).unwrap(), 2);
    assert_eq!(v, vec![192, 1]);
}

#[test]
fn test_encode_i64_2bytes() {
    let mut v = vec![];
    assert_eq!(encode_i64(260, &mut v).unwrap(), 3);
    assert_eq!(v, b"\xc1\x04\x01");
}

#[test]
fn test_encode_i64_4bytes() {
    let mut v = vec![];
    assert_eq!(encode_i64(70000, &mut v).unwrap(), 5);
    assert_eq!(v, b"\xc2p\x11\x01\x00");
}

#[test]
fn test_encode_i64_overflow() {
    let mut v = vec![];
    match encode_usize(usize::MAX, &mut v).unwrap_err() {
        EncodeError::OverflowError => (),
        _ => panic!("Unexpected error"),
    }
}

#[test]
fn test_encode_usize() {
    let mut v = vec![];
    assert_eq!(encode_usize(123, &mut v).unwrap(), 2);
    assert_eq!(v, vec![192, 123]);
}

#[test]
fn test_encode_usize_overflow() {
    let mut v = vec![];
    match encode_usize(usize::MAX, &mut v).unwrap_err() {
        EncodeError::OverflowError => (),
        _ => panic!("Unexpected error"),
    }
}

#[test]
fn test_encode_slice_u8_integer() {
    let mut v = vec![];
    assert_eq!(encode_slice_u8(b"1", &mut v).unwrap(), 2);
    assert_eq!(v, vec![192, 1]);
}

#[test]
fn test_encode_slice_u8_data() {
    let mut v = vec![];
    assert_eq!(encode_slice_u8(b"hello world", &mut v).unwrap(), 12);
    assert_eq!(v, b"\x0bhello world");
}
