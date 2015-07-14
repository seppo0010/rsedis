use std::error::Error;
use std::fmt;
use std::io;
use std::num::{ParseIntError, ParseFloatError};
use std::str::Utf8Error;

#[derive(Debug)]
pub enum OperationError {
    OverflowError,
    ValueError,
    UnknownKeyError,
    WrongTypeError,
    OutOfBoundsError,
    IOError(io::Error),
}

impl fmt::Display for OperationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.description().fmt(f)
    }
}

impl Error for OperationError {
    fn description(&self) -> &str {
        return "oops";
    }
}

impl From<Utf8Error> for OperationError {
    fn from(_: Utf8Error) -> OperationError { OperationError::ValueError }
}

impl From<ParseIntError> for OperationError {
    fn from(_: ParseIntError) -> OperationError { OperationError::ValueError }
}

impl From<ParseFloatError> for OperationError {
    fn from(_: ParseFloatError) -> OperationError { OperationError::ValueError }
}

impl From<io::Error> for OperationError {
    fn from(e: io::Error) -> OperationError { OperationError::IOError(e) }
}
