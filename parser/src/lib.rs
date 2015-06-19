#![feature(collections_bound)]

use std::collections::Bound;
use std::str::from_utf8;
use std::str::Utf8Error;
use std::num::ParseIntError;
use std::num::ParseFloatError;
use std::error::Error;
use std::fmt;

pub struct Argument {
    pub pos: usize,
    pub len: usize,
}

pub struct Parser<'a> {
    data: &'a[u8],
    pub argv: Vec<Argument>
}

pub enum ParseError {
    Incomplete,
    BadProtocol,
    InvalidArgument,
}

impl fmt::Debug for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        return write!(f, "{}", self.description());
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        return self.description().fmt(f);
    }
}

impl Error for ParseError {
    fn description(&self) -> &str {
        match *self {
            ParseError::Incomplete => "Incomplete data",
            ParseError::BadProtocol => "Invalid data",
            ParseError::InvalidArgument => "Invalid argument",
        }
    }

    fn cause(&self) -> Option<&Error> { None }
}

impl From<Utf8Error> for ParseError {
    fn from(_: Utf8Error) -> ParseError { ParseError::InvalidArgument }
}

impl From<ParseIntError> for ParseError {
    fn from(_: ParseIntError) -> ParseError { ParseError::InvalidArgument }
}

impl From<ParseFloatError> for ParseError {
    fn from(_: ParseFloatError) -> ParseError { ParseError::InvalidArgument }
}

impl<'a> Parser<'a> {
    pub fn new(data: &[u8], argv: Vec<Argument>) -> Parser {
        return Parser {
            data: data,
            argv: argv,
        };
    }

    pub fn get_f64_bound(&self, pos: usize) -> Result<Bound<f64>, ParseError> {
        let s = try!(self.get_str(pos));
        if s == "inf" || s == "+inf" || s == "-inf" {
            return Ok(Bound::Unbounded);
        }
        if s.starts_with("(") {
            return Ok(Bound::Excluded(try!(s[1..].parse::<f64>())));
        }
        return Ok(Bound::Included(try!(s.parse::<f64>())));
    }

    // TODO: get<T>
    pub fn get_f64(&self, pos: usize) -> Result<f64, ParseError> {
        let s = try!(self.get_str(pos));
        return Ok(try!(s.parse::<f64>()));
    }

    pub fn get_i64(&self, pos: usize) -> Result<i64, ParseError> {
        let s = try!(self.get_str(pos));
        return Ok(try!(s.parse::<i64>()));
    }

    pub fn get_str(&self, pos: usize) -> Result<&str, ParseError> {
        let data = try!(self.get_slice(pos));
        Ok(try!(from_utf8(&data)))
    }

    pub fn get_vec(&self, pos: usize) -> Result<Vec<u8>, ParseError> {
        let data = try!(self.get_slice(pos));
        return Ok(data.to_vec());
    }

    pub fn get_slice(&self, pos: usize) -> Result<&[u8], ParseError> {
        if pos >= self.argv.len() {
            return Err(ParseError::InvalidArgument);
        }
        let arg = &self.argv[pos];
        return Ok(&self.data[arg.pos..arg.pos+arg.len]);
    }
}

impl<'a> fmt::Debug for Parser<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        return write!(f, "{}", self.description());
    }
}

impl<'a> fmt::Display for Parser<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        return self.description().fmt(f);
    }
}

impl<'a> Error for Parser<'a> {
    fn description(&self) -> &str {
        return "parser";
    }
    fn cause(&self) -> Option<&Error> { None }
}

fn parse_int(input: &[u8], len: usize) -> Result<(usize, usize), ParseError> {
    let mut i = 0;
    let mut argc = 0;
    while input[i] as char != '\r' {
        let c = input[i] as char;
        if c < '0' || c > '9' {
            return Err(ParseError::BadProtocol);
        }
        argc *= 10;
        argc += input[i] as usize - '0' as usize;
        i += 1;
        if i == len {
            return Err(ParseError::Incomplete);
        }
    }
    i += 1;
    if input[i] as char != '\n' {
        return Err(ParseError::BadProtocol);
    }
    return Ok((argc, i + 1));
}

pub fn parse(input: &[u8], len: usize) -> Result<Parser, ParseError> {
    if input[0] as char != '*' {
        return Err(ParseError::BadProtocol);
    } else {
        let mut pos = 1;
        let (argc, intlen) = try!(parse_int(&input[pos..len], len - pos));
        pos += intlen;
        let mut argv = Vec::new();
        for i in 0..argc {
            if input[pos] as char != '$' {
                return Err(ParseError::BadProtocol);
            }
            pos += 1;
            let (arglen, arglenlen) = try!(parse_int(&input[pos..len], len - pos));
            pos += arglenlen;
            let arg = Argument {
                pos: pos,
                len: arglen,
            };
            argv.push(arg);
            pos += arglen + 2;
            if pos > len || (pos == len && i != argc - 1) {
                return Err(ParseError::Incomplete);
            }
        }
        Ok(Parser::new(input, argv))
    }
}

#[test]
fn parse_valid() {
    let message = b"*2\r\n$3\r\nfoo\r\n$4\r\nbarz\r\n";
    let r = parse(message, message.len());
    assert!(r.is_ok());
    let parser = r.unwrap();
    assert_eq!(parser.argv.len(), 2);
    assert_eq!(parser.get_str(0).unwrap(), "foo");
    assert_eq!(parser.get_str(1).unwrap(), "barz");
}

#[test]
fn parse_incomplete() {
    let message = b"*2\r\n$3\r\nfoo";
    let r = parse(message, message.len());
    assert!(r.is_err());
    match r.unwrap_err() {
        ParseError::Incomplete => {},
        _ => assert!(false)
    }
}

#[test]
fn parse_invalid() {
    let message = b"-2\r\n$3\r\nfoo";
    let r = parse(message, message.len());
    assert!(r.is_err());
    match r.unwrap_err() {
        ParseError::BadProtocol => {},
        _ => assert!(false)
    }
}
