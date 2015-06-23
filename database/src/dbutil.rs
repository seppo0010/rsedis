use std::str::from_utf8;

use error::OperationError;

pub fn normalize_position(position: i64, _len: usize) -> Result<usize, usize> {
    let len = _len as i64;
    let mut pos = position;
    if pos < 0 {
        pos += len;
    }
    if pos < 0 {
        return Err(0);
    }
    if pos > len {
        return Err(len as usize);
    }
    return Ok(pos as usize);
}

pub fn usize_to_vec(i: usize) -> Vec<u8> {
    format!("{}", i).into_bytes()
}

pub fn vec_to_usize(data: &Vec<u8>) -> Result<usize, OperationError> {
    let res = try!(from_utf8(&data));
    Ok(try!(res.parse::<usize>()))
}
