use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use std::thread;
use std::sync::{Arc, Mutex};

use super::command::command;
use super::database::Database;
use super::parser::parse;
use super::parser::ParseError;

pub struct Client {
    pub stream: TcpStream,
    pub db: Arc<Mutex<Database>>
}

pub struct Server {
    pub ip: String,
    pub port: i32,
    pub db: Arc<Mutex<Database>>,
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
    pub fn new(ip: &str, port: &i32) -> Server {
        return Server {
            ip: ip.to_string(),
            port: *port,
            db: Arc::new(Mutex::new(Database::new())),
        }
    }

    fn get_listener(&self) -> TcpListener {
        let addr: String = format!("{}:{}", self.ip, self.port);
        return TcpListener::bind(&*addr).unwrap();
    }

    pub fn run(&mut self) {
        #![allow(unused_must_use)]
        self.start().join();
    }

    pub fn start(&mut self) -> thread::JoinHandle {
        let listener = self.get_listener();
        let db = self.db.clone();
        return thread::spawn(move || {
            for stream in listener.incoming() {
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
    }

    pub fn stop(&self) {
        // TODO
    }
}

pub fn new_server(ip: &str, port: &i32) -> Server {
    return Server::new(ip, port);
}
