extern crate parser;

use std::fmt::{Debug, Error, Formatter};
use std::sync::mpsc::Receiver;

use parser::OwnedParsedCommand;

/// A command response to send to a client
#[derive(PartialEq, Debug)]
pub enum Response {
    /// No data
    Nil,
    /// A number
    Integer(i64),
    /// Binary data
    Data(Vec<u8>),
    /// A simple error string
    Error(String),
    /// A simple status string
    Status(String),
    /// An array of responses that may mix different types
    Array(Vec<Response>),
}

/// No response was issued
pub enum ResponseError {
    /// The command generated no response
    NoReply,
    /// The command generated no response yet. At a later time, a new command
    /// should be executed, or give up if a None is received.
    /// Only one message will be sent.
    Wait(Receiver<Option<OwnedParsedCommand>>),
}

impl Debug for ResponseError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        match self {
            ResponseError::NoReply => write!(f, "NoReply"),
            ResponseError::Wait(_) => write!(f, "Wait"),
        }
    }
}

impl Response {
    /// Serializes the response into an array of bytes using Redis protocol.
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Response::Nil => b"$-1\r\n".to_vec(),
            Response::Data(d) => [
                &b"$"[..],
                &d.len().to_string().into_bytes()[..],
                b"\r\n",
                &d[..],
                b"\r\n",
            ]
            .concat(),
            Response::Integer(i) => [&b":"[..], &i.to_string().into_bytes()[..], b"\r\n"].concat(),
            Response::Error(d) => [&b"-"[..], (*d).as_bytes(), b"\r\n"].concat(),
            Response::Status(d) => [
                &b"+"[..],
                (*d).as_bytes(),
                &"\r\n".to_owned().into_bytes()[..],
            ]
            .concat(),
            Response::Array(a) => [
                &b"*"[..],
                &a.len().to_string().into_bytes()[..],
                b"\r\n",
                &(a.iter().map(|el| el.as_bytes()).collect::<Vec<_>>()[..].concat())[..],
            ]
            .concat(),
        }
    }

    /// Returns true if and only if the response is an error.
    pub fn is_error(&self) -> bool {
        if let Response::Error(_) = *self {
            true
        } else {
            false
        }
    }

    /// Is the response a status
    pub fn is_status(&self) -> bool {
        if let Response::Status(_) = *self {
            true
        } else {
            false
        }
    }
}
