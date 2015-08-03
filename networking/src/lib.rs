#![feature(duration)]
#![feature(socket_timeout)]
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

use std::time::Duration;
use std::io;
use std::io::{Read, Write};
use std::net::{SocketAddr, ToSocketAddrs, TcpStream};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;

#[cfg(unix)] use std::path::Path;
#[cfg(unix)] use std::fs::File;

#[cfg(unix)] use libc::funcs::posix88::unistd::fork;
#[cfg(unix)] use libc::funcs::c95::stdlib::exit;
#[cfg(unix)] use libc::funcs::posix88::unistd::getpid;
use net2::{TcpBuilder, TcpStreamExt};
#[cfg(unix)] use unix_socket::{UnixStream, UnixListener};

use config::Config;
use database::{Database, PubsubEvent};
use logger::Level;
use parser::{OwnedParsedCommand, Parser, ParseError};
use response::{Response, ResponseError};

/// A stream connection.
#[cfg(unix)]
enum Stream {
    Tcp(TcpStream),
    Unix(UnixStream),
}

#[cfg(not(unix))]
enum Stream {
    Tcp(TcpStream),
}

#[cfg(unix)]
impl Stream {
    /// Creates a new independently owned handle to the underlying socket.
    fn try_clone(&self) -> io::Result<Stream> {
        match *self {
            Stream::Tcp(ref s) => Ok(Stream::Tcp(try!(s.try_clone()))),
            Stream::Unix(ref s) => Ok(Stream::Unix(try!(s.try_clone()))),
        }
    }

    /// Write a buffer into this object, returning how many bytes were written.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            Stream::Tcp(ref mut s) => s.write(buf),
            Stream::Unix(ref mut s) => s.write(buf),
        }
    }

    /// Sets the keepalive timeout to the timeout specified.
    /// It fails silently for UNIX sockets.
    fn set_keepalive(&self, duration: Option<Duration>) -> io::Result<()> {
        match *self {
            Stream::Tcp(ref s) => TcpStreamExt::set_keepalive(s, duration),
            Stream::Unix(_) => Ok(()),
        }
    }

    /// Sets the write timeout to the timeout specified.
    /// It fails silently for UNIX sockets.
    fn set_write_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match *self {
            Stream::Tcp(ref s) => s.set_write_timeout(dur),
            // TODO: couldn't figure out how to enable this in unix_socket
            Stream::Unix(_) => Ok(()),
        }
    }

    /// Sets the read timeout to the timeout specified.
    /// It fails silently for UNIX sockets.
    fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match *self {
            Stream::Tcp(ref s) => s.set_read_timeout(dur),
            // TODO: couldn't figure out how to enable this in unix_socket
            Stream::Unix(_) => Ok(()),
        }
    }
}

#[cfg(unix)]
impl Read for Stream {
    /// Pull some bytes from this source into the specified buffer,
    /// returning how many bytes were read.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            Stream::Tcp(ref mut s) => s.read(buf),
            Stream::Unix(ref mut s) => s.read(buf),
        }
    }
}

#[cfg(not(unix))]
impl Stream {
    /// Creates a new independently owned handle to the underlying socket.
    fn try_clone(&self) -> io::Result<Stream> {
        match *self {
            Stream::Tcp(ref s) => Ok(Stream::Tcp(try!(s.try_clone()))),
        }
    }

    /// Write a buffer into this object, returning how many bytes were written.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            Stream::Tcp(ref mut s) => s.write(buf),
        }
    }

    /// Sets the keepalive timeout to the timeout specified.
    /// It fails silently for UNIX sockets.
    fn set_keepalive(&self, duration: Option<Duration>) -> io::Result<()> {
        match *self {
            Stream::Tcp(ref s) => TcpStreamExt::set_keepalive(s, duration),
        }
    }

    /// Sets the write timeout to the timeout specified.
    /// It fails silently for UNIX sockets.
    fn set_write_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match *self {
            Stream::Tcp(ref s) => s.set_write_timeout(dur),
        }
    }

    /// Sets the read timeout to the timeout specified.
    /// It fails silently for UNIX sockets.
    fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match *self {
            Stream::Tcp(ref s) => s.set_read_timeout(dur),
        }
    }
}

#[cfg(not(unix))]
impl Read for Stream {
    /// Pull some bytes from this source into the specified buffer,
    /// returning how many bytes were read.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            Stream::Tcp(ref mut s) => s.read(buf),
        }
    }
}

/// A client connection
struct Client {
    /// The socket connection
    stream: Stream,
    /// A reference to the database
    db: Arc<Mutex<Database>>,
    /// The client unique identifier
    id: usize,
}

/// The database server
pub struct Server {
    /// A reference to the database
    db: Arc<Mutex<Database>>,
    /// A list of channels listening for incoming connections
    listener_channels: Vec<Sender<u8>>,
    /// A list of threads listening for incoming connections
    listener_threads: Vec<thread::JoinHandle<()>>,
    /// An incremental id for new clients
    pub next_id: Arc<Mutex<usize>>,
}

impl Client {
    /// Creates a new TCP socket client
    pub fn tcp(stream: TcpStream, db: Arc<Mutex<Database>>, id: usize) -> Client {
        return Client {
            stream: Stream::Tcp(stream),
            db: db,
            id: id,
        }
    }

    /// Creates a new UNIX socket client
    #[cfg(unix)]
    pub fn unix(stream: UnixStream, db: Arc<Mutex<Database>>, id: usize) -> Client {
        return Client {
            stream: Stream::Unix(stream),
            db: db,
            id: id,
        }
    }

    /// Creates a thread that writes into the client stream each response received
    fn create_writer_thread(&self, sender: Sender<(Level, String)>, rx: Receiver<Option<Response>>) {
        let mut stream = self.stream.try_clone().unwrap();
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

    /// Creates a thread that sends responses for every pubsub event received
    fn create_pubsub_thread(&self, tx: Sender<Option<Response>>, pubsub_rx: Receiver<Option<PubsubEvent>>) {
        #![allow(unused_must_use)]
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

    /// Runs all clients commands. The function loops until the client
    /// disconnects.
    pub fn run(&mut self, sender: Sender<(Level, String)>) {
        #![allow(unused_must_use)]
        let (stream_tx, rx) = channel::<Option<Response>>();
        self.create_writer_thread(sender.clone(), rx);

        let (pubsub_tx, pubsub_rx) = channel::<Option<PubsubEvent>>();
        self.create_pubsub_thread(stream_tx.clone(), pubsub_rx);

        let mut client = command::Client::new(pubsub_tx, self.id);
        let mut parser = Parser::new();

        let mut this_command:Option<OwnedParsedCommand>;
        let mut next_command:Option<OwnedParsedCommand> = None;
        loop {
            if next_command.is_none() {
                parser.allocate();
                let len = {
                    let pos = parser.written;
                    let mut buffer = parser.get_mut();

                    // read socket
                    match self.stream.read(&mut buffer[pos..]) {
                        Ok(r) => r,
                        Err(err) => {
                            sendlog!(sender, Verbose, "Reading from client: {:?}", err);
                            break;
                        },
                    }
                };
                parser.written += len;

                // client closed connection
                if len == 0 {
                    sendlog!(sender, Verbose, "Client closed connection");
                    break;
                }
            }

            // was there an error during the execution?
            let mut error = false;

            this_command = next_command;
            next_command = None;

            // try to parse received command
            let parsed_command = match this_command {
                Some(ref c) => c.get_command(),
                None => match parser.next() {
                    Ok(p) => p,
                    Err(err) => match err {
                        // if it's incomplete, keep adding to the buffer
                        ParseError::Incomplete => { continue; }
                        ParseError::BadProtocol(s) => {
                            let _ = stream_tx.send(Some(Response::Error(s)));
                            break;
                        },
                        _ =>  {
                            sendlog!(sender, Verbose, "Protocol error from client: {:?}", err);
                            break;
                        }
                    },
                }
            };

            let mut db = match self.db.lock() {
                Ok(db) => db,
                Err(_) => break,
            };

            // execute the command
            let r = command::command(parsed_command, &mut *db, &mut client);
            // unlock the db
            drop(db);

            // check out the response
            match r {
                // received a response, send it to the client
                Ok(response) => {
                    match stream_tx.send(Some(response)) {
                        Ok(_) => (),
                        Err(_) => error = true,
                    };
                },
                // no response
                Err(err) => match err {
                    // There is no reply to send, that's ok
                    ResponseError::NoReply => (),
                    // We have to wait until a sender signals us back and then retry
                    // (Repeating the same command is actually wrong because of the timeout)
                    ResponseError::Wait(ref receiver) => {
                        // if we receive a None, send a nil, otherwise execute the command
                        match receiver.recv().unwrap() {
                            Some(cmd) => next_command = Some(cmd),
                            None => match stream_tx.send(Some(Response::Nil)) {
                                Ok(_) => (),
                                Err(_) => error = true,
                            },
                        }
                    }
                },
            }

            // if something failed, let's shut down the client
            if error {
                // kill threads
                stream_tx.send(None);
                client.pubsub_sender.send(None);
                break;
            }
        }
    }
}

macro_rules! handle_listener {
    ($logger: expr, $listener: expr, $server: expr, $rx: expr, $tcp_keepalive: expr, $timeout: expr, $t: ident) => ({
        let db = $server.db.clone();
        let sender = $logger.sender();
        let next_id = $server.next_id.clone();
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
                        let id = {
                            let mut nid = next_id.lock().unwrap();
                            *nid += 1;
                            *nid - 1
                        };
                        thread::spawn(move || {
                            let mut client = Client::$t(stream, db1, id);
                            client.stream.set_keepalive(if $tcp_keepalive > 0 { Some(Duration::from_secs($tcp_keepalive as u64)) } else { None }).unwrap();
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
    /// Creates a new server
    pub fn new(config: Config) -> Server {
        let db = Database::new(config);
        return Server {
            db: Arc::new(Mutex::new(db)),
            listener_channels: Vec::new(),
            listener_threads: Vec::new(),
            next_id: Arc::new(Mutex::new(0)),
        }
    }

    /// Runs the server. If `config.daemonize` is true, it forks and exits.
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
        let daemonize = {
            let db = self.db.lock().unwrap();
            db.config.daemonize
        };
        if daemonize {
            panic!("Cannot daemonize in non-unix");
        } else {
            self.start();
            self.join();
        }
    }

    #[cfg(windows)]
    fn reuse_address(&self, _: &TcpBuilder) -> io::Result<()> {
        Ok(())
    }

    #[cfg(not(windows))]
    fn reuse_address(&self, builder: &TcpBuilder) -> io::Result<()> {
        try!(builder.reuse_address(true));
        Ok(())
    }

    /// Join the listener threads.
    pub fn join(&mut self) {
        #![allow(unused_must_use)]
        while self.listener_threads.len() > 0 {
            self.listener_threads.pop().unwrap().join();
        }
    }

    /// Listens to a socket address.
    fn listen<T: ToSocketAddrs>(&mut self, t: T, tcp_keepalive: u32, timeout: u64, tcp_backlog: i32) -> io::Result<()> {
        for addr in try!(t.to_socket_addrs()) {
            let (tx, rx) = channel();
            let builder = try!(match addr {
                SocketAddr::V4(_) => TcpBuilder::new_v4(),
                SocketAddr::V6(_) => TcpBuilder::new_v6(),
            });

            try!(self.reuse_address(&builder));
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

    /// Starts threads listening to new connections.
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
                Ok(_) => {
                    let db = self.db.lock().unwrap();
                    log!(db.config.logger, Notice, "The server is now ready to accept connections on port {}", port);
                },
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
    fn handle_unixsocket(&mut self) {
        let db = self.db.lock().unwrap();
        if db.config.unixsocket.is_some() {
            let _ = writeln!(&mut std::io::stderr(), "Ignoring unixsocket in non unix environment\n");
        }
    }

    /// Sends a kill signal to the listeners and connects to the incoming
    /// connections to break the listening loop.
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
    use std::thread;

    use config::Config;
    use logger::{Logger, Level};

    use super::Server;

    #[test]
    fn parse_ping() {
        let port = 16379;

        let mut server = Server::new(Config::mock(port, Logger::new(Level::Warning)));
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
        let mut server = Server::new(Config::mock(port, Logger::new(Level::Warning)));
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
        let mut server = Server::new(Config::mock(port, Logger::new(Level::Warning)));
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

    #[test]
    fn allow_multiple_clients() {
        let port = 16382;
        let mut server = Server::new(Config::mock(port, Logger::new(Level::Warning)));
        server.start();

        let addr = format!("127.0.0.1:{}", port);
        let _ = TcpStream::connect(&*addr);
        thread::sleep_ms(100);
        assert_eq!(*server.next_id.lock().unwrap(), 1);
        let _ = TcpStream::connect(&*addr);
        thread::sleep_ms(100);
        assert_eq!(*server.next_id.lock().unwrap(), 2);
        server.stop();
    }
}
