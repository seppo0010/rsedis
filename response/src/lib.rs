use std::sync::mpsc::Receiver;
use std::fmt::{Debug, Formatter, Error};

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
    /// The command generated no response yet, but if the receiver reads a `true`,
    /// the command can be retried.
    Wait(Receiver<bool>),
}

impl Debug for ResponseError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        match self {
            &ResponseError::NoReply => write!(f, "NoReply"),
            &ResponseError::Wait(_) => write!(f, "Wait"),
        }
    }
}

impl Response {
    /// Serializes the response into an array of bytes using Redis protocol.
    pub fn as_bytes(&self) -> Vec<u8> {
        return match *self {
            Response::Nil => b"$-1\r\n".to_vec(),
            Response::Data(ref d) => [&b"$"[..], &format!("{}\r\n", d.len()).into_bytes()[..], &d[..], &"\r\n".to_owned().into_bytes()[..]].concat(),
            Response::Integer(ref i) => [&b":"[..], &format!("{}\r\n", i).into_bytes()[..]].concat(),
            Response::Error(ref d) => [&b"-"[..], (*d).as_bytes(), &"\r\n".to_owned().into_bytes()[..]].concat(),
            Response::Status(ref d) => [&b"+"[..], (*d).as_bytes(), &"\r\n".to_owned().into_bytes()[..]].concat(),
            Response::Array(ref a) => [&b"*"[..],  &format!("{}\r\n", a.len()).into_bytes()[..],
                &(a.iter().map(|el| el.as_bytes()).collect::<Vec<_>>()[..].concat())[..]
                ].concat()
        }
    }

    /// Returns true if and only if the response is an error.
    pub fn is_error(&self) -> bool {
        match *self {
            Response::Error(_) => true,
            _ => false,
        }
    }
}
