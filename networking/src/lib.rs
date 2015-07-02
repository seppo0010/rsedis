#![feature(duration)]
#![feature(socket_timeout)]
#![feature(tcp)]
#[cfg(unix)]extern crate libc;
#[cfg(unix)]extern crate unix_socket;
#[macro_use(log, sendlog)] extern crate logger;
extern crate config;
extern crate util;
extern crate parser;
extern crate response;
extern crate database;
extern crate command;
extern crate net2;

use std::collections::HashMap;
use std::time::Duration;
use std::io;
use std::io::{Read, Write};
use std::net::{SocketAddr, ToSocketAddrs, TcpStream};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Sender, channel};
use std::thread;

#[cfg(unix)] use std::path::Path;
#[cfg(unix)] use std::fs::File;

#[cfg(unix)] use libc::funcs::posix88::unistd::fork;
#[cfg(unix)] use libc::funcs::c95::stdlib::exit;
#[cfg(unix)] use libc::funcs::posix88::unistd::getpid;
use net2::TcpBuilder;
#[cfg(unix)] use unix_socket::{UnixStream, UnixListener};

use command::command;
use config::Config;
use database::{Database, PubsubEvent};
use logger::Level;
use parser::parse;
use parser::ParseError;
use response::{Response, ResponseError};

enum Stream {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl Stream {
    fn try_clone(&self) -> io::Result<Stream> {
        match *self {
            Stream::Tcp(ref s) => Ok(Stream::Tcp(try!(s.try_clone()))),
            Stream::Unix(ref s) => Ok(Stream::Unix(try!(s.try_clone()))),
        }
    }

    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            Stream::Tcp(ref mut s) => s.write(buf),
            Stream::Unix(ref mut s) => s.write(buf),
        }
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            Stream::Tcp(ref mut s) => s.read(buf),
            Stream::Unix(ref mut s) => s.read(buf),
        }
    }

    fn set_keepalive(&self, seconds: Option<u32>) -> io::Result<()> {
        match *self {
            Stream::Tcp(ref s) => s.set_keepalive(seconds),
            Stream::Unix(_) => Ok(()),
        }
    }

    fn set_write_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match *self {
            Stream::Tcp(ref s) => s.set_write_timeout(dur),
            // TODO: couldn't figure out how to enable this in unix_socket
            Stream::Unix(_) => Ok(()),
        }
    }

    fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match *self {
            Stream::Tcp(ref s) => s.set_read_timeout(dur),
            // TODO: couldn't figure out how to enable this in unix_socket
            Stream::Unix(_) => Ok(()),
        }
    }
}

struct Client {
    stream: Stream,
    db: Arc<Mutex<Database>>
}

pub struct Server {
    db: Arc<Mutex<Database>>,
    listener_channels: Vec<Sender<u8>>,
    listener_threads: Vec<thread::JoinHandle<()>>,
}

impl Client {
    pub fn tcp(stream: TcpStream, db: Arc<Mutex<Database>>) -> Client {
        return Client {
            stream: Stream::Tcp(stream),
            db: db,
        }
    }

    pub fn unix(stream: UnixStream, db: Arc<Mutex<Database>>) -> Client {
        return Client {
            stream: Stream::Unix(stream),
            db: db,
        }
    }

    pub fn run(&mut self, sender: Sender<(Level, String)>) {
        #![allow(unused_must_use)]
        let (stream_tx, rx) = channel::<Option<Response>>();
        {
            let mut stream = self.stream.try_clone().unwrap();
            let sender = sender.clone();
            thread::spawn(move || {
                loop {
                    match rx.recv() {
                        Ok(m) => match m {
                            Some(msg) => match stream.write(&*msg.as_bytes()) {
                                Ok(_) => (),
                                Err(e) => sendlog!(sender, Warning, "Error writing to client: {:?}", e).unwrap(),
                            },
                            None => break,
                        },
                        Err(_) => break,
                    };
                }
            });
        }
        let (pubsub_tx, pubsub_rx) = channel::<Option<PubsubEvent>>();
        {
            let tx = stream_tx.clone();
            thread::spawn(move || {
                loop {
                    match pubsub_rx.recv() {
                        Ok(m) => match m {
                            Some(msg) => tx.send(Some(msg.as_response())),
                            None => break,
                        },
                        Err(_) => break,
                    };
                }
                tx.send(None);
            });
        }
        let mut auth = false;
        let mut buffer = [0u8; 512];
        let mut dbindex = 0;
        let mut subscriptions = HashMap::new();
        let mut psubscriptions = HashMap::new();
        loop {
            let len = match self.stream.read(&mut buffer) {
                Ok(r) => r,
                Err(err) => {
                    sendlog!(sender, Verbose, "Reading from client: {:?}", err);
                    break;
                },
            };
            if len == 0 {
                sendlog!(sender, Verbose, "Client closed connection");
                break;
            }
            let parser = match parse(&buffer, len) {
                Ok(p) => p,
                Err(err) => match err {
                    ParseError::Incomplete => { continue; }
                    _ =>  {
                        sendlog!(sender, Verbose, "Protocol error from client: {:?}", err);
                        break;
                    }
                },
            };

            let mut error = false;
            loop {
                let mut db = match self.db.lock() {
                    Ok(db) => db,
                    Err(_) => break,
                };
                match command(&parser, &mut *db, &mut dbindex, &mut auth, Some(&mut subscriptions), Some(&mut psubscriptions), Some(&pubsub_tx)) {
                    Ok(response) => {
                        match stream_tx.send(Some(response)) {
                            Ok(_) => (),
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
                                match stream_tx.send(Some(Response::Nil)) {
                                    Ok(_) => (),
                                    Err(_) => error = true,
                                };
                            }
                        }
                    },
                }
            }
            if error {
                stream_tx.send(None);
                pubsub_tx.send(None);
                break;
            }
        };
    }
}

macro_rules! handle_listener {
    ($logger: expr, $listener: expr, $server: expr, $rx: expr, $tcp_keepalive: expr, $timeout: expr, $t: ident) => ({
        let db = $server.db.clone();
        let sender = $logger.sender();
        thread::spawn(move || {
            for stream in $listener.incoming() {
                if $rx.try_recv().is_ok() {
                    // any new message should break
                    break;
                }
                match stream {
                    Ok(stream) => {
                        sendlog!(sender, Verbose, "Accepted connection to {:?}", stream).unwrap();
                        let db1 = db.clone();
                        let mysender = sender.clone();
                        thread::spawn(move || {
                            let mut client = Client::$t(stream, db1);
                            client.stream.set_keepalive(if $tcp_keepalive > 0 { Some($tcp_keepalive) } else { None }).unwrap();
                            client.stream.set_read_timeout(if $timeout > 0 { Some(Duration::new($timeout, 0)) } else { None }).unwrap();
                            client.stream.set_write_timeout(if $timeout > 0 { Some(Duration::new($timeout, 0)) } else { None }).unwrap();
                            client.run(mysender);
                        });
                    }
                    Err(e) => sendlog!(sender, Warning, "Accepting client connection: {:?}", e).unwrap(),
                }
            }
        })
    })
}

impl Server {
    pub fn new(config: Config) -> Server {
        let db = Database::new(config);
        return Server {
            db: Arc::new(Mutex::new(db)),
            listener_channels: Vec::new(),
            listener_threads: Vec::new(),
        }
    }

    #[cfg(unix)]
    pub fn run(&mut self) {
        let (daemonize, pidfile) = {
            let db = self.db.lock().unwrap();
            (db.config.daemonize.clone(), db.config.pidfile.clone())
        };
        if daemonize {
            unsafe {
                match fork() {
                    -1 => panic!("Fork failed"),
                    0 => {
                        if let Ok(mut fp) = File::create(Path::new(&*pidfile)) {
                            match write!(fp, "{}", getpid()) {
                                Ok(_) => (),
                                Err(e) => {
                                    let db = self.db.lock().unwrap();
                                    log!(db.config.logger, Warning, "Error writing pid: {}", e);
                                },
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
        let db = self.db.lock().unwrap();
        if db.config.daemonize {
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

    fn listen<T: ToSocketAddrs>(&mut self, t: T, tcp_keepalive: u32, timeout: u64, tcp_backlog: i32) -> io::Result<()> {
        for addr in try!(t.to_socket_addrs()) {
            let (tx, rx) = channel();
            let builder = try!(match addr {
                SocketAddr::V4(_) => TcpBuilder::new_v4(),
                SocketAddr::V6(_) => TcpBuilder::new_v6(),
            });
            let listener = try!(try!(
                        builder.bind(addr))
                    .listen(tcp_backlog));
            self.listener_channels.push(tx);
            {
                let db = self.db.lock().unwrap();
                let th = handle_listener!(db.config.logger, listener, self, rx, tcp_keepalive, timeout, tcp);
                self.listener_threads.push(th);
            }
        }
        Ok(())
    }

    pub fn start(&mut self) {
        let (tcp_keepalive, timeout, addresses, tcp_backlog) = {
            let db = self.db.lock().unwrap();
            (db.config.tcp_keepalive.clone(),
             db.config.timeout.clone(),
             db.config.addresses().clone(),
             db.config.tcp_backlog.clone(),
             )
        };
        for (host, port) in addresses {
            match self.listen((&host[..], port), tcp_keepalive, timeout, tcp_backlog) {
                Ok(ok) => ok,
                Err(err) => {
                    let db = self.db.lock().unwrap();
                    log!(db.config.logger, Warning, "Creating Server TCP listening socket {}:{}: {:?}", host, port, err);
                    continue;
                }
            }

        }
        self.handle_unixsocket();
    }

    #[cfg(unix)]
    fn handle_unixsocket(&mut self) {
        let db = self.db.lock().unwrap();
        if let Some(ref unixsocket) = db.config.unixsocket {
            let tcp_keepalive = db.config.tcp_keepalive;
            let timeout = db.config.timeout;

            let (tx, rx) = channel();
            self.listener_channels.push(tx);
            let listener = match UnixListener::bind(unixsocket) {
                Ok(l) => l,
                Err(err) => {
                    log!(db.config.logger, Warning, "Creating Server Unix socket {}: {:?}", unixsocket, err);
                    return;
                }
            };
            let th = handle_listener!(db.config.logger, listener, self, rx, tcp_keepalive, timeout, unix);
            self.listener_threads.push(th);
        }
    }

    #[cfg(not(unix))]
    fn handle_unixsocket() {
        let db = self.db.lock().unwrap();
        if db.config.unixsocket.is_some() {
            writeln!(&mut std::io::stderr(), "Ignoring unixsocket in non unix environment\n");
        }
    }

    pub fn stop(&mut self) {
        #![allow(unused_must_use)]
        for sender in self.listener_channels.iter() {
            sender.send(0);
            let db = self.db.lock().unwrap();
            for (host, port) in db.config.addresses() {
                for addrs in (&host[..], port).to_socket_addrs().unwrap() {
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
    use logger::Logger;

    use super::Server;

    #[test]
    fn parse_ping() {
        let port = 16379;

        let mut server = Server::new(Config::mock(port, Logger::null()));
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
        let port = 16380;
        let mut server = Server::new(Config::mock(port, Logger::null()));
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
        let port = 16381;
        let mut server = Server::new(Config::mock(port, Logger::null()));
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
