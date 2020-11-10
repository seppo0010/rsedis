use std::str::from_utf8;

use error::OperationError;

/// Gets the position in a list from a signed integer
/// Redis uses this logic to start counting from the end using negative numbers.
///
/// When the absolute number is bigger than the len, an error is produced
/// returning true if the value was greater than the last index and false
/// if the value is lower than the first index.
///
/// # Examples
/// ```
/// use database::dbutil::normalize_position;
///
/// assert_eq!(normalize_position(0, 10), Ok(0));
/// assert_eq!(normalize_position(-1, 10), Ok(9));
/// assert_eq!(normalize_position(10, 10), Err(true));
/// assert_eq!(normalize_position(-11, 10), Err(false));
/// assert_eq!(normalize_position(-100, 0), Err(false));
/// assert_eq!(normalize_position(100, 0), Err(true));
/// ```
pub fn normalize_position(position: i64, _len: usize) -> Result<usize, bool> {
    let len = _len as i64;
    let mut pos = position;
    if pos < 0 {
        pos += len;
    }

    if pos < 0 {
        Err(false)
    } else if pos >= len {
        Err(true)
    } else {
        Ok(pos as usize)
    }
}

/// Creates an ASCII representation of a number
///
/// # Examples
/// ```
/// use database::dbutil::usize_to_vec;
///
/// assert_eq!(usize_to_vec(200), vec!['2' as u8, '0' as u8, '0' as u8]);
/// ```
pub fn usize_to_vec(i: usize) -> Vec<u8> {
    format!("{}", i).into_bytes()
}

/// Parses an ASCII representation of a number
///
/// # Examples
/// ```
/// use database::dbutil::vec_to_usize;
///
/// assert_eq!(vec_to_usize(&vec!['2' as u8, '0' as u8, '0' as u8]).unwrap(), 200);
/// assert!(vec_to_usize(&vec!['a' as u8]).is_err());
/// assert!(vec_to_usize(&b"01".to_vec()).is_err());
/// assert!(vec_to_usize(&b"0".to_vec()).is_ok());
/// assert!(vec_to_usize(&b"".to_vec()).is_err());
/// ```
pub fn vec_to_usize(data: &[u8]) -> Result<usize, OperationError> {
    // "01" should not be transformed into 1
    if data.is_empty() || (data.len() > 1 && data[0] as char == '0') {
        return Err(OperationError::ValueError(
            "ERR value is not an integer".to_owned(),
        ));
    }

    let res = from_utf8(&data)?;
    Ok(res.parse::<usize>()?)
}
