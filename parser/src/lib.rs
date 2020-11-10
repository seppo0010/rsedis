extern crate util;

use std::collections::Bound;
use std::error::Error;
use std::f64::{INFINITY, NEG_INFINITY};
use std::fmt;
use std::iter;
use std::num::{ParseFloatError, ParseIntError};
use std::str::{from_utf8, Utf8Error};

use util::format_repr;

/// A command argument
#[derive(Debug, Clone)]
pub struct Argument {
    /// The position in the array
    pub pos: usize,
    /// The length in the array
    pub len: usize,
}

/// A protocol parser
pub struct ParsedCommand<'a> {
    /// The data itself
    data: &'a [u8],
    /// The arguments location and length
    pub argv: Vec<Argument>,
}

#[derive(Debug)]
pub struct OwnedParsedCommand {
    data: Vec<u8>,
    pub argv: Vec<Argument>,
}

/// Error parsing
#[derive(Debug, PartialEq)]
pub enum ParseError {
    /// The received buffer is valid but needs more data
    Incomplete,
    /// The received buffer is invalid
    BadProtocol(String),
    /// Expected one type of argument and received another
    InvalidArgument,
}

impl ParseError {
    pub fn is_incomplete(&self) -> bool {
        match *self {
            ParseError::Incomplete => true,
            _ => false,
        }
    }

    fn response_string(&self) -> String {
        match *self {
            ParseError::Incomplete => "Incomplete data".to_owned(),
            ParseError::BadProtocol(ref s) => format!("Protocol error: {}", s),
            ParseError::InvalidArgument => "Invalid argument".to_owned(),
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.response_string().fmt(f)
    }
}

impl Error for ParseError {
    fn description(&self) -> &str {
        match *self {
            ParseError::Incomplete => "Incomplete data",
            ParseError::BadProtocol(_) => "Protocol error",
            ParseError::InvalidArgument => "Invalid argument",
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}

impl From<Utf8Error> for ParseError {
    fn from(_: Utf8Error) -> ParseError {
        ParseError::InvalidArgument
    }
}

impl From<ParseIntError> for ParseError {
    fn from(_: ParseIntError) -> ParseError {
        ParseError::InvalidArgument
    }
}

impl From<ParseFloatError> for ParseError {
    fn from(_: ParseFloatError) -> ParseError {
        ParseError::InvalidArgument
    }
}

impl OwnedParsedCommand {
    pub fn new(data: Vec<u8>, argv: Vec<Argument>) -> Self {
        OwnedParsedCommand {
            data,
            argv,
        }
    }

    pub fn get_command(&self) -> ParsedCommand {
        ParsedCommand::new(&*self.data, self.argv.clone())
    }
}

impl<'a> ParsedCommand<'a> {
    /// Creates a new parser with the data and arguments provided
    pub fn new(data: &[u8], argv: Vec<Argument>) -> ParsedCommand {
        ParsedCommand { data, argv }
    }

    /// Gets a `Bound` from a parameter.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::collections::Bound;
    /// # use parser::{ParsedCommand, Argument};
    /// let parser = ParsedCommand::new(b"+inf", vec![Argument { pos: 0, len: 4 }]);
    /// assert_eq!(parser.get_f64_bound(0).unwrap(), Bound::Unbounded);
    /// ```
    ///
    /// ```
    /// # use std::collections::Bound;
    /// # use parser::{ParsedCommand, Argument};
    /// let parser = ParsedCommand::new(b"1.23", vec![Argument { pos: 0, len: 4 }]);
    /// assert_eq!(parser.get_f64_bound(0).unwrap(), Bound::Included(1.23));
    /// ```
    ///
    /// ```
    /// # use std::collections::Bound;
    /// # use parser::{ParsedCommand, Argument};
    /// let parser = ParsedCommand::new(b"(1.23", vec![Argument { pos: 0, len: 5 }]);
    /// assert_eq!(parser.get_f64_bound(0).unwrap(), Bound::Excluded(1.23));
    /// ```
    pub fn get_f64_bound(&self, pos: usize) -> Result<Bound<f64>, ParseError> {
        let s = self.get_str(pos)?;
        if s == "inf" || s == "+inf" || s == "-inf" {
            return Ok(Bound::Unbounded);
        }

        if s.starts_with('(') {
            let f = s[1..].parse::<f64>()?;
            if f.is_nan() {
                return Err(ParseError::InvalidArgument);
            }
            return Ok(Bound::Excluded(f));
        }
        let f = s.parse::<f64>()?;

        if f.is_nan() {
            Err(ParseError::InvalidArgument)
        } else {
            Ok(Bound::Included(f))
        }
    }

    // TODO: get<T>

    /// Gets an f64 from a parameter
    ///
    /// # Examples
    ///
    /// ```
    /// # use parser::{ParsedCommand, Argument};
    /// let parser = ParsedCommand::new(b"1.23", vec![Argument { pos: 0, len: 4 }]);
    /// assert_eq!(parser.get_f64(0).unwrap(), 1.23);
    /// ```
    pub fn get_f64(&self, pos: usize) -> Result<f64, ParseError> {
        let s = self.get_str(pos)?;
        if s == "+inf" || s == "inf" {
            return Ok(INFINITY);
        }
        if s == "-inf" {
            return Ok(NEG_INFINITY);
        }
        let f = s.parse::<f64>()?;
        if f.is_nan() {
            Err(ParseError::InvalidArgument)
        } else {
            Ok(f)
        }
    }

    /// Gets an i64 from a parameter
    ///
    /// # Examples
    ///
    /// ```
    /// # use parser::{ParsedCommand, Argument};
    /// let parser = ParsedCommand::new(b"-123", vec![Argument { pos: 0, len: 4 }]);
    /// assert_eq!(parser.get_i64(0).unwrap(), -123);
    /// ```
    pub fn get_i64(&self, pos: usize) -> Result<i64, ParseError> {
        let s = self.get_str(pos)?;

        Ok(s.parse::<i64>()?)
    }

    /// Gets an str from a parameter
    ///
    /// # Examples
    ///
    /// ```
    /// # use parser::{ParsedCommand, Argument};
    /// let parser = ParsedCommand::new(b"foo", vec![Argument { pos: 0, len: 3 }]);
    /// assert_eq!(parser.get_str(0).unwrap(), "foo");
    /// ```
    pub fn get_str(&self, pos: usize) -> Result<&str, ParseError> {
        let data = self.get_slice(pos)?;
        Ok(from_utf8(&data)?)
    }

    /// Gets a Vec<u8> from a parameter
    ///
    /// # Examples
    ///
    /// ```
    /// # use parser::{ParsedCommand, Argument};
    /// let parser = ParsedCommand::new(b"foo", vec![Argument { pos: 0, len: 3 }]);
    /// assert_eq!(parser.get_vec(0).unwrap(), b"foo".to_vec());
    /// ```
    pub fn get_vec(&self, pos: usize) -> Result<Vec<u8>, ParseError> {
        let data = self.get_slice(pos)?;
        Ok(data.to_vec())
    }

    /// Gets a &[u8] from a parameter
    ///
    /// # Examples
    ///
    /// ```
    /// # use parser::{ParsedCommand, Argument};
    /// let parser = ParsedCommand::new(b"foo", vec![Argument { pos: 0, len: 3 }]);
    /// assert_eq!(parser.get_slice(0).unwrap(), b"foo");
    /// ```
    pub fn get_slice(&self, pos: usize) -> Result<&[u8], ParseError> {
        if pos >= self.argv.len() {
            return Err(ParseError::InvalidArgument);
        }
        let arg = &self.argv[pos];
        Ok(&self.data[arg.pos..arg.pos + arg.len])
    }

    pub fn get_data(&self) -> &'a [u8] {
        self.data
    }

    pub fn into_owned(self) -> OwnedParsedCommand {
        OwnedParsedCommand::new(Vec::from(self.data), self.argv)
    }

    pub fn to_owned(&self) -> OwnedParsedCommand {
        OwnedParsedCommand::new(Vec::from(self.data), self.argv.clone())
    }
}

impl<'a> fmt::Debug for ParsedCommand<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        for a in self.argv.iter() {
            format_repr(f, &self.data[a.pos..(a.pos + a.len)])?;
            f.write_str(" ")?;
        }
        Ok(())
    }
}

/// Parses the length of the paramenter in the slice
/// Upon success, it returns a tuple with the length of the argument and the
/// length of the parsed length.
fn parse_int(input: &[u8], len: usize, name: &str) -> Result<(Option<usize>, usize), ParseError> {
    if input.is_empty() {
        return Err(ParseError::Incomplete);
    }
    let mut i = 0;
    let mut argc = 0;
    let mut argco = None;
    while input[i] as char != '\r' {
        let c = input[i] as char;
        if argc == 0 && c == '-' {
            while input[i] as char != '\r' {
                i += 1;
            }
            argco = None;
            break;
        } else if c < '0' || c > '9' {
            return Err(ParseError::BadProtocol(format!("invalid {} length", name)));
        }
        argc *= 10;
        argc += input[i] as usize - '0' as usize;
        i += 1;
        if i == len {
            return Err(ParseError::Incomplete);
        }
        argco = Some(argc);
    }
    i += 1;
    if i == len {
        return Err(ParseError::Incomplete);
    }
    if input[i] as char != '\n' {
        return Err(ParseError::BadProtocol(format!(
            "expected \\r\\n separator, got \\r{}",
            input[i] as char
        )));
    }

    Ok((argco, i + 1))
}

/// Creates a parser from a buffer.
///
/// # Examples
///
/// ```
/// # use parser::parse;
/// let s = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$2\r\n10\r\n";
/// let (parser, len) = parse(s).unwrap();
/// assert_eq!(len, 32);
/// assert_eq!(parser.get_str(0).unwrap(), "SET");
/// assert_eq!(parser.get_str(1).unwrap(), "mykey");
/// assert_eq!(parser.get_i64(2).unwrap(), 10);
/// ```
pub fn parse(input: &[u8]) -> Result<(ParsedCommand, usize), ParseError> {
    let mut pos = 0;
    while input.len() > pos && input[pos] as char == '\r' {
        if pos + 1 < input.len() {
            if input[pos + 1] as char != '\n' {
                return Err(ParseError::BadProtocol(format!(
                    "expected \\r\\n separator, got \
                     \\r{}",
                    input[pos + 1] as char
                )));
            }
            pos += 2;
        } else {
            return Err(ParseError::Incomplete);
        }
    }
    if pos >= input.len() {
        return Err(ParseError::Incomplete);
    }
    if input[pos] as char != '*' {
        return Err(ParseError::BadProtocol(format!(
            "expected '*', got '{}'",
            input[pos] as char
        )));
    }
    pos += 1;
    let len = input.len();
    let (argco, intlen) = parse_int(&input[pos..len], len - pos, "multibulk")?;
    let argc = match argco {
        Some(i) => i,
        None => 0,
    };
    pos += intlen;
    if argc > 1024 * 1024 {
        return Err(ParseError::BadProtocol(
            "invalid multibulk length".to_owned(),
        ));
    }
    let mut argv = Vec::new();
    for i in 0..argc {
        if input.len() == pos {
            return Err(ParseError::Incomplete);
        }
        if input[pos] as char != '$' {
            return Err(ParseError::BadProtocol(format!(
                "expected '$', got '{}'",
                input[pos] as char
            )));
        }
        pos += 1;
        let (argleno, arglenlen) = parse_int(&input[pos..len], len - pos, "bulk")?;
        let arglen = match argleno {
            Some(i) => i,
            None => return Err(ParseError::BadProtocol("invalid bulk length".to_owned())),
        };
        if arglen > 512 * 1024 * 1024 {
            return Err(ParseError::BadProtocol("invalid bulk length".to_owned()));
        }
        pos += arglenlen;
        let arg = Argument { pos, len: arglen };
        argv.push(arg);
        pos += arglen + 2;
        if pos > len || (pos == len && i != argc - 1) {
            return Err(ParseError::Incomplete);
        }
    }
    Ok((ParsedCommand::new(input, argv), pos))
}

/// A stream parser
pub struct Parser {
    data: Vec<u8>,
    pub position: usize,
    pub written: usize,
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Parser {
    pub fn new() -> Parser {
        Parser {
            data: vec![],
            position: 0,
            written: 0,
        }
    }

    pub fn allocate(&mut self) {
        if self.position > 0 && self.written == self.position {
            self.written = 0;
            self.position = 0;
        }

        let len = self.data.len();
        let add = if len == 0 {
            16
        } else if self.written * 2 > len {
            len
        } else {
            0
        };

        if add > 0 {
            self.data.extend(iter::repeat(0).take(add));
        }
    }

    pub fn get_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }

    pub fn is_incomplete(&self) -> bool {
        let data = &(&*self.data)[self.position..self.written];
        match parse(data) {
            Ok(_) => false,
            Err(e) => e.is_incomplete(),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<ParsedCommand, ParseError> {
        let data = &(&*self.data)[self.position..self.written];
        let (r, len) = parse(data)?;
        self.position += len;
        Ok(r)
    }
}

impl fmt::Debug for Parser {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.write_str("Parser: ")?;
        format_repr(f, &(&*self.data)[self.position..self.written])
    }
}

#[cfg(test)]
mod test_parser {
    use super::{parse, ParseError, Parser};

    #[test]
    fn parse_valid() {
        let message = b"*2\r\n$3\r\nfoo\r\n$4\r\nbarz\r\n";
        let r = parse(message);
        assert!(r.is_ok());
        let (command, len) = r.unwrap();
        assert_eq!(len, message.len());
        assert_eq!(command.argv.len(), 2);
        assert_eq!(command.get_str(0).unwrap(), "foo");
        assert_eq!(command.get_str(1).unwrap(), "barz");
    }

    #[test]
    fn parse_incomplete() {
        let message = b"*2\r\n$3\r\nfoo";
        let r = parse(message);
        assert!(r.is_err());
        match r.unwrap_err() {
            ParseError::Incomplete => {}
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_invalid() {
        let message = b"-2\r\n$3\r\nfoo";
        let r = parse(message);
        assert!(r.is_err());
        match r.unwrap_err() {
            ParseError::BadProtocol(_) => {}
            _ => assert!(false),
        }
    }

    #[test]
    fn parser_basic() {
        let mut parser = Parser::new();
        {
            let message = b"*2\r\n$3\r\nfoo\r\n$4\r\nbarz\r\n";
            parser.written += message.len();
            let mut v = parser.get_mut();
            v.extend(&*message.to_vec());
        }
        {
            let command = parser.next().unwrap();
            assert_eq!(command.argv.len(), 2);
            assert_eq!(command.get_str(0).unwrap(), "foo");
            assert_eq!(command.get_str(1).unwrap(), "barz");
        }
        assert_eq!(parser.next().unwrap_err(), ParseError::Incomplete);
    }

    #[test]
    fn parser_incomplete() {
        let mut parser = Parser::new();
        assert_eq!(parser.next().unwrap_err(), ParseError::Incomplete);
        parser.written += 2;
        parser.get_mut().extend(&*b"*2".to_vec());
        assert_eq!(parser.next().unwrap_err(), ParseError::Incomplete);
        let message = b"\r\n$3\r\nfoo\r\n$4\r\nbarz\r\n";
        parser.written += message.len();
        parser.get_mut().extend(&*message.to_vec());
        parser.next().unwrap();
    }

    #[test]
    fn parser_skip_blank_line() {
        let mut parser = Parser::new();
        {
            let message = b"\r\n\r\n*1\r\n$3\r\nfoo\r\n";
            parser.written += message.len();
            let mut v = parser.get_mut();
            v.extend(&*message.to_vec());
        }

        {
            let command = parser.next().unwrap();
            assert_eq!(command.argv.len(), 1);
            assert_eq!(command.get_str(0).unwrap(), "foo");
        }
    }

    #[test]
    fn parser_multiple() {
        let mut parser = Parser::new();
        {
            let message = b"*2\r\n$3\r\nfoo\r\n$4\r\nbarz\r\n";
            parser.written += message.len();
            parser.written += message.len();
            let mut v = parser.get_mut();
            v.extend(&*message.to_vec());
            v.extend(&*message.to_vec());
        }
        parser.next().unwrap();
        parser.next().unwrap();
        assert_eq!(parser.next().unwrap_err(), ParseError::Incomplete);
    }

    #[test]
    fn parser_multiple2() {
        let mut parser = Parser::new();
        let message = b"*2\r\n$3\r\nfoo\r\n$4\r\nbarz\r\n";
        {
            parser.written += message.len();
            let mut v = parser.get_mut();
            v.extend(&*message.to_vec());
        }
        parser.next().unwrap();
        {
            parser.written += message.len();
            let mut v = parser.get_mut();
            v.extend(&*message.to_vec());
        }
        parser.next().unwrap();
        assert_eq!(parser.next().unwrap_err(), ParseError::Incomplete);
    }

    #[test]
    fn parser_debug_formatter() {
        let mut parser = Parser::new();
        let message = b"*2\r\n$3\r\n\x01\x00\x08\r\n$4\r\n\xffarz\r\n";
        {
            parser.written += message.len();
            let mut v = parser.get_mut();
            v.extend(&*message.to_vec());
        }
        assert_eq!(
            format!("{:?}", parser),
            "Parser: \"*2\\r\\n$3\\r\\n\\x01\\x00\\b\\r\\n$4\\r\\n\\xffarz\\r\\n\""
        );
    }
}
