use std::net::{Ipv4Addr, TcpListener, TcpStream};
use std::io::{Read, Write};
use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Sender, channel};

use super::command::command;
use super::database::Database;
use super::parser::parse;
use super::parser::ParseError;

pub struct Client {
    pub stream: TcpStream,
    pub db: Arc<Mutex<Database>>
}

pub struct Server {
    pub ip: Ipv4Addr,
    pub port: u16,
    pub db: Arc<Mutex<Database>>,
    pub listener_channel: Option<Sender<u8>>,
    pub listener_thread: Option<thread::JoinHandle>,
}

impl Client {
    pub fn new(stream: TcpStream, db: Arc<Mutex<Database>>) -> Client {
        return Client {
            stream: stream,
            db: db,
        }
    }

    pub fn run(&mut self) {
        let mut buffer = [0u8; 512];
        loop {
            let result = self.stream.read(&mut buffer);
            if result.is_err() {
                break;
            }
            let len = result.unwrap();
            if len == 0 {
                break;
            }
            let try_parser = parse(&buffer, len);
            if try_parser.is_err() {
                let err = try_parser.unwrap_err();
                match err {
                    ParseError::BadProtocol => { break; }
                    ParseError::Incomplete => { continue; }
                };
            }
            let parser = try_parser.unwrap();
            let mut db = self.db.lock().unwrap();
            let response = command(&parser, &mut *db);
            let writeres = self.stream.write(&*response.as_bytes());
            if writeres.is_err() {
                break;
            }
        };
    }
}

impl Server {
    pub fn new(ip: Ipv4Addr, port: u16) -> Server {
        return Server {
            ip: ip,
            port: port,
            db: Arc::new(Mutex::new(Database::new())),
            listener_channel: None,
            listener_thread: None,
        }
    }

    fn get_listener(&self) -> TcpListener {
        return TcpListener::bind((self.ip, self.port)).unwrap();
    }

    pub fn run(&mut self) {
        self.start();
        self.join();
    }

    pub fn join(&mut self) {
        #![allow(unused_must_use)]
        match self.listener_thread.take() {
            Some(th) => { th.join(); },
            _ => {},
        }
    }

    pub fn start(&mut self) {
        let listener = self.get_listener();
        let db = self.db.clone();
        let (tx, rx) = channel();
        self.listener_channel = Some(tx);
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
                            let mut client = Client::new(stream, db1);
                            client.run();
                        });
                    }
                    Err(e) => { println!("error {}", e); }
                }
            }
        });
        self.listener_thread = Some(th);
    }

    pub fn stop(&mut self) {
        #![allow(unused_must_use)]
        match self.listener_channel {
            Some(ref sender) => {
                sender.send(0);
                TcpStream::connect(&*format!("127.0.0.1:{}", self.port));
            },
            _ => {},
        }
        self.join();
    }
}

pub fn new_server(ip: Ipv4Addr, port: u16) -> Server {
    return Server::new(ip, port);
}
