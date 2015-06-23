#![feature(tcp)]
#[cfg(unix)]extern crate libc;

extern crate config;
extern crate util;
extern crate parser;
extern crate response;
extern crate database;
extern crate command;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{ToSocketAddrs, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Sender, channel};
use std::thread;

#[cfg(unix)] use std::path::Path;
#[cfg(unix)] use std::fs::File;

#[cfg(unix)] use libc::funcs::posix88::unistd::fork;
#[cfg(unix)] use libc::funcs::c95::stdlib::exit;
#[cfg(unix)] use libc::funcs::posix88::unistd::getpid;

use config::Config;
use database::{Database, PubsubEvent};
use response::{Response, ResponseError};
use command::command;
use parser::parse;
use parser::ParseError;

struct Client {
    stream: TcpStream,
    db: Arc<Mutex<Database>>
}

pub struct Server {
    config: Config,
    db: Arc<Mutex<Database>>,
    listener_channels: Vec<Sender<u8>>,
    listener_threads: Vec<thread::JoinHandle<()>>,
}

impl Client {
    pub fn new(stream: TcpStream, db: Arc<Mutex<Database>>) -> Client {
        return Client {
            stream: stream,
            db: db,
        }
    }

    pub fn run(&mut self) {
        #![allow(unused_must_use)]
        let (stream_tx, rx) = channel::<Response>();
        {
            let mut stream = self.stream.try_clone().unwrap();
            thread::spawn(move || {
                loop {
                    match rx.recv() {
                        Ok(msg) => stream.write(&*msg.as_bytes()),
                        Err(_) => break,
                    };
                }
            });
        }
        let (pubsub_tx, pubsub_rx) = channel::<PubsubEvent>();
        {
            let tx = stream_tx.clone();
            thread::spawn(move || {
                loop {
                    match pubsub_rx.recv() {
                        Ok(msg) => tx.send(msg.as_response()),
                        Err(_) => break,
                    };
                }
            });
        }
        let mut buffer = [0u8; 512];
        let mut dbindex = 0;
        let mut subscriptions = HashMap::new();
        let mut psubscriptions = HashMap::new();
        loop {
            let len = match self.stream.read(&mut buffer) {
                Ok(r) => r,
                Err(_) => break,
            };
            if len == 0 {
                break;
            }
            let parser = match parse(&buffer, len) {
                Ok(p) => p,
                Err(err) => match err {
                    ParseError::Incomplete => { continue; }
                    _ => { break; }
                },
            };

            let mut error = false;
            loop {
                let mut db = match self.db.lock() {
                    Ok(db) => db,
                    Err(_) => break,
                };
                match command(&parser, &mut *db, &mut dbindex, Some(&mut subscriptions), Some(&mut psubscriptions), Some(&pubsub_tx)) {
                    Ok(response) => {
                        match stream_tx.send(response) {
                            Ok(_) => (),
                            // TODO: send a kill signal to the writer thread?
                            Err(_) => error = true,
                        };
                        break;
                    },
                    Err(err) => match err {
                        ResponseError::NoReply => (),
                        // Repeating the same command is actually wrong because of the timeout
                        ResponseError::Wait(ref receiver) => {
                            drop(db);
                            if !receiver.recv().unwrap() {
                                match stream_tx.send(Response::Nil) {
                                    Ok(_) => (),
                                    // TODO: send a kill signal to the writer thread?
                                    Err(_) => error = true,
                                };
                            }
                        }
                    },
                }
            }
            if error {
                break;
            }
        };
    }
}

impl Server {
    pub fn new(config: Config) -> Server {
        let db = Database::new(&config);
        return Server {
            config: config,
            db: Arc::new(Mutex::new(db)),
            listener_channels: Vec::new(),
            listener_threads: Vec::new(),
        }
    }

    #[cfg(unix)]
    pub fn run(&mut self) {
        if self.config.daemonize {
            unsafe {
                match fork() {
                    -1 => panic!("Fork failed"),
                    0 => {
                        if let Ok(mut fp) = File::create(Path::new(&*self.config.pidfile)) {
                            match write!(fp, "{}", getpid()) {
                                // TODO warn on error?
                                _ => (),
                            }
                        }
                        self.start();
                        self.join();
                    },
                    _ => exit(0),
                };
            }
        } else {
            self.start();
            self.join();
        }
    }

    #[cfg(not(unix))]
    pub fn run(&mut self) {
        if self.config.daemonize {
            panic!("Cannot daemonize in non-unix");
        } else {
            self.start();
            self.join();
        }
    }

    pub fn join(&mut self) {
        #![allow(unused_must_use)]
        while self.listener_threads.len() > 0 {
            self.listener_threads.pop().unwrap().join();
        }
    }

    pub fn start(&mut self) {
        let tcp_keepalive = self.config.tcp_keepalive;
        for addr in self.config.addresses() {
            let (tx, rx) = channel();
            self.listener_channels.push(tx);
            let listener = TcpListener::bind(addr).unwrap();
            let db = self.db.clone();
            let th =  thread::spawn(move || {
                for stream in listener.incoming() {
                    if rx.try_recv().is_ok() {
                        // any new message should break
                        break;
                    }
                    match stream {
                        Ok(stream) => {
                            let db1 = db.clone();
                            thread::spawn(move || {
                                stream.set_keepalive(if tcp_keepalive > 0 { Some(tcp_keepalive) } else { None }).unwrap();
                                let mut client = Client::new(stream, db1);
                                client.run();
                            });
                        }
                        Err(e) => { println!("error {}", e); }
                    }
                }
            });
            self.listener_threads.push(th);
        }
    }

    pub fn stop(&mut self) {
        #![allow(unused_must_use)]
        for sender in self.listener_channels.iter() {
            sender.send(0);
            for addr in self.config.addresses() {
                for addrs in addr.to_socket_addrs().unwrap() {
                    TcpStream::connect(addrs);
                }
            }
        }
        self.join();
    }
}

#[cfg(test)]
mod test_networking {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::str::from_utf8;

    use config::Config;

    use super::Server;

    #[test]
    fn parse_ping() {
        let port = 6379;

        let mut server = Server::new(Config::mock(port));
        server.start();

        let addr = format!("127.0.0.1:{}", port);
        let streamres = TcpStream::connect(&*addr);
        assert!(streamres.is_ok());
        let mut stream = streamres.unwrap();
        let message = b"*2\r\n$4\r\nping\r\n$4\r\npong\r\n";
        assert!(stream.write(message).is_ok());
        let mut h = [0u8; 4];
        assert!(stream.read(&mut h).is_ok());
        assert_eq!(from_utf8(&h).unwrap(), "$4\r\n");
        let mut c = [0u8; 6];
        assert!(stream.read(&mut c).is_ok());
        assert_eq!(from_utf8(&c).unwrap(), "pong\r\n");
        server.stop();
    }

    #[test]
    fn allow_multiwrite() {
        let port = 6380;
        let mut server = Server::new(Config::mock(port));
        server.start();

        let addr = format!("127.0.0.1:{}", port);
        let streamres = TcpStream::connect(&*addr);
        assert!(streamres.is_ok());
        let mut stream = streamres.unwrap();
        let message = b"*2\r\n$4\r\nping\r\n";
        assert!(stream.write(message).is_ok());
        let message = b"$4\r\npong\r\n";
        assert!(stream.write(message).is_ok());
        let mut h = [0u8; 4];
        assert!(stream.read(&mut h).is_ok());
        assert_eq!(from_utf8(&h).unwrap(), "$4\r\n");
        let mut c = [0u8; 6];
        assert!(stream.read(&mut c).is_ok());
        assert_eq!(from_utf8(&c).unwrap(), "pong\r\n");
        server.stop();
    }

    #[test]
    fn allow_stop() {
        let port = 6381;
        let mut server = Server::new(Config::mock(port));
        server.start();
        {
            let addr = format!("127.0.0.1:{}", port);
            let streamres = TcpStream::connect(&*addr);
            assert!(streamres.is_ok());
        }
        server.stop();

        {
            let addr = format!("127.0.0.1:{}", port);
            let streamres = TcpStream::connect(&*addr);
            assert!(streamres.is_err());
        }

        server.start();
        {
            let addr = format!("127.0.0.1:{}", port);
            let streamres = TcpStream::connect(&*addr);
            assert!(streamres.is_ok());
        }
        server.stop();
    }
}
