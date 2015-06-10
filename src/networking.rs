use std::collections::HashMap;
use std::net::{ToSocketAddrs, TcpListener, TcpStream};
use std::io::{Read, Write};
use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Sender, channel};

use super::command::command;
use super::command::{Response, ResponseError};
use super::database::{Database, PubsubEvent};
use super::parser::parse;
use super::parser::ParseError;

struct Client {
    stream: TcpStream,
    db: Arc<Mutex<Database>>
}

pub struct Server<A: ToSocketAddrs + Clone> {
    addr: A,
    db: Arc<Mutex<Database>>,
    listener_channel: Option<Sender<u8>>,
    listener_thread: Option<thread::JoinHandle<()>>,
}

impl PubsubEvent {
    pub fn as_response(&self) -> Response {
        match self {
            &PubsubEvent::Message(ref channel, ref pattern, ref message) => match pattern {
                &Some(ref pattern) => Response::Array(vec![
                        Response::Data(b"message".to_vec()),
                        Response::Data(channel.clone()),
                        Response::Data(pattern.clone()),
                        Response::Data(message.clone()),
                        ]),
                &None => Response::Array(vec![
                        Response::Data(b"message".to_vec()),
                        Response::Data(channel.clone()),
                        Response::Data(message.clone()),
                        ]),
            },
            &PubsubEvent::Subscription(ref channel, ref subscriptions) => Response::Array(vec![
                    Response::Data(b"subscribe".to_vec()),
                    Response::Data(channel.clone()),
                    Response::Integer(subscriptions.clone() as i64),
                    ]),
            &PubsubEvent::Unsubscription(ref channel, ref subscriptions) => Response::Array(vec![
                    Response::Data(b"unsubscribe".to_vec()),
                    Response::Data(channel.clone()),
                    Response::Integer(subscriptions.clone() as i64),
                    ]),
            &PubsubEvent::PatternSubscription(ref pattern, ref subscriptions) => Response::Array(vec![
                    Response::Data(b"psubscribe".to_vec()),
                    Response::Data(pattern.clone()),
                    Response::Integer(subscriptions.clone() as i64),
                    ]),
            &PubsubEvent::PatternUnsubscription(ref pattern, ref subscriptions) => Response::Array(vec![
                    Response::Data(b"punsubscribe".to_vec()),
                    Response::Data(pattern.clone()),
                    Response::Integer(subscriptions.clone() as i64),
                    ]),
        }
    }
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
        let (stream_tx, rx) = channel();
        {
            let mut stream = self.stream.try_clone().unwrap();
            thread::spawn(move || {
                loop {
                    let try_recv = rx.recv();
                    if try_recv.is_err() {
                        break;
                    }
                    let msg:Response = try_recv.unwrap();
                    let resp = msg.as_bytes();
                    stream.write(&*resp);
                }
            });
        }
        let (pubsub_tx, pubsub_rx) = channel();
        {
            let tx = stream_tx.clone();
            thread::spawn(move || {
                loop {
                    let try_recv = pubsub_rx.recv();
                    if try_recv.is_err() {
                        break;
                    }
                    let msg:PubsubEvent = try_recv.unwrap();
                    tx.send(msg.as_response());
                }
            });
        }
        let mut buffer = [0u8; 512];
        let mut dbindex = 0;
        let mut subscriptions = HashMap::new();
        let mut psubscriptions = HashMap::new();
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
            let mut error = false;
            loop {
                let mut db = self.db.lock().unwrap();
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

impl<A: ToSocketAddrs + Clone> Server<A> {
    pub fn new(addr: A) -> Server<A> {
        return Server {
            addr: addr,
            db: Arc::new(Mutex::new(Database::new())),
            listener_channel: None,
            listener_thread: None,
        }
    }

    fn get_listener(&self) -> TcpListener {
        return TcpListener::bind(self.addr.clone()).unwrap();
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
                for addrs in self.addr.to_socket_addrs().unwrap() {
                    TcpStream::connect(addrs);
                }
            },
            _ => {},
        }
        self.join();
    }
}
