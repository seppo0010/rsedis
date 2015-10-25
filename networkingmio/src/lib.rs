extern crate config;
extern crate database;
extern crate mio;
extern crate parser;

use mio::*;
use mio::tcp::*;
use mio::util::Slab;
use std::io;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;

use std::net::ToSocketAddrs;
use std::sync::{Arc, Mutex, MutexGuard};

use config::Config;
use database::Database;
use parser::{Parser};

const SERVER: Token = Token(0);
const CONN_TOKEN_START : Token = Token(1);
const CONNS_MAX : usize = 256;

struct Connection {
    sock: TcpStream,
    write_buf: Vec<u8>,
    parser: Parser,
    token: Option<Token>,
    peer_hup: bool,
    interest: EventSet,
}

impl Connection {
    fn new(sock: TcpStream) -> Connection {
        Connection {
            sock: sock,
            write_buf: Vec::new(),
            parser: Parser::new(),
            token: None,
            peer_hup: false,
            interest: EventSet::hup() | EventSet::readable(),
        }
    }

    fn is_finished(&self) -> bool {
        self.interest == EventSet::none()
    }

    fn reregister(&mut self,
                  event_loop: &mut EventLoop<Server>,
                 ) -> io::Result<()> {
        // have somewhere to write
        if self.parser.is_incomplete() {
            self.interest.insert(EventSet::writable());
        } else {
            self.interest.remove(EventSet::writable());
        }

        // have somewhere to read to and someone to receive from
        if !self.peer_hup && self.write_buf.len() > 0 {
            self.interest.insert(EventSet::readable());
        } else {
            self.interest.remove(EventSet::readable());
        }

        event_loop.reregister(
                &self.sock, self.token.unwrap(),
                self.interest, PollOpt::edge() | PollOpt::oneshot()
                )
    }

    fn writable(&mut self,
                event_loop: &mut EventLoop<Server>,
                ) -> io::Result<()> {
        /*
        loop {
            let (len, res) = {
                let buf = &self.write_buf.bytes();
                let len = buf.len();
                let res = self.sock.try_write(buf);
                (len, res)
            };
            match res {
                Ok(None) => {
                    break;
                },
                Ok(Some(r)) => {
                    Buf::advance(&mut self.write_buf, r);
                    if r != len || Buf::remaining(&self.write_buf) == 0 {
                        break;
                    }
                },
                Err(_) => {
                    Buf::advance(&mut self.write_buf, len);
                    self.peer_hup = true;
                    break;
                },
            }
        }
        */

        self.reregister(event_loop)
    }

    fn readable(&mut self,
                event_loop: &mut EventLoop<Server>,
                ) -> io::Result<()> {
        loop {
            self.parser.allocate();
            let len = {
                let mut buf = self.parser.get_mut();
                match self.sock.try_read(buf) {
                    Ok(Some(len)) => len,
                    _ => break,
                }
            };
            self.parser.written += len;
            println!("{:?}", self.parser);
        }

        self.reregister(event_loop)
    }

    fn hup(&mut self,
           event_loop: &mut EventLoop<Server>,
          ) -> io::Result<()> {
        if self.interest == EventSet::hup() {
            self.interest = EventSet::none();
            try!(event_loop.deregister(&self.sock));
            Ok(())
        } else {
            self.peer_hup = true;
            self.reregister(event_loop)
        }
    }


}

impl fmt::Display for Connection {
    fn fmt(&self, fmt : &mut fmt::Formatter) -> Result<(), fmt::Error> {
        try!(self.token.fmt(fmt));
        try!(<Display>::fmt(&", ", fmt));
        try!((&self.sock as *const TcpStream).fmt(fmt));
        try!(<Display>::fmt(&", ", fmt));
        self.sock.peer_addr().fmt(fmt)
    }
}

pub struct Server {
    db: Arc<Mutex<Database>>,
    sock: Option<TcpListener>,
    conns: Slab<Connection>,
}

impl Server {
    pub fn new(config: Config) -> Server {
        let db = Database::new(config);
        Server {
            db: Arc::new(Mutex::new(db)),
            sock: None,
            conns: Slab::new_starting_at(CONN_TOKEN_START, CONNS_MAX),
        }
    }

    pub fn get_mut_db<'a>(&'a self) -> MutexGuard<'a, database::Database> {
        self.db.lock().unwrap()
    }

    fn accept(&mut self, event_loop: &mut EventLoop<Server>) -> io::Result<()> {
        loop {
            let sock = match try!(self.sock.as_ref().unwrap().accept()) {
                None => break,
                Some(sock) => sock,
            };

            // Don't buffer output in TCP - kills latency sensitive benchmarks
            let _ = sock.set_nodelay(true);
            let conn = Connection::new(sock);
            let tok = self.conns.insert(conn);
            let tok = match tok {
                Ok(tok) => tok,
                Err(_) => return Ok(()),
            };
            self.conns[tok].token = Some(tok);
            try!(event_loop.register_opt(
                    &self.conns[tok].sock, tok, EventSet::readable() , PollOpt::edge() | PollOpt::oneshot())
                );
        }
        Ok(())
    }

    fn conn_handle_finished(&mut self, tok : Token, finished : bool) {
        if finished {
            self.conns.remove(tok);
        }
    }

    fn conn_readable(&mut self, event_loop: &mut EventLoop<Server>, tok: Token) -> io::Result<()> {
        let (res, finished) = {
            let conn = self.conn(tok);
            let res = conn.readable(event_loop);
            (res, conn.is_finished())
        };
        self.conn_handle_finished(tok, finished);
        res
    }

    fn conn_writable(&mut self, event_loop: &mut EventLoop<Server>, tok: Token) -> io::Result<()> {
        let (res, finished) = {
            let conn = self.conn(tok);
            let res = conn.writable(event_loop);
            (res, conn.is_finished())
        };
        self.conn_handle_finished(tok, finished);
        res
    }

    fn conn_hup(&mut self, event_loop: &mut EventLoop<Server>, tok: Token) -> io::Result<()> {
        let (res, finished) = {
            let conn = self.conn(tok);
            let res = conn.hup(event_loop);
            (res, conn.is_finished())
        };
        self.conn_handle_finished(tok, finished);
        res
    }

    fn conn<'a>(&'a mut self, tok: Token) -> &'a mut Connection {
        &mut self.conns[tok]
    }

    pub fn run(&mut self) {
        let addresses = {
            let db = self.db.lock().unwrap();
            db.config.addresses().clone()
        };
        let (host, port) = addresses.into_iter().next().unwrap();
        let addr = (&host[..], port).to_socket_addrs().unwrap().next().unwrap();
        let ssock = TcpSocket::v4().unwrap();

        ssock.set_reuseaddr(true).unwrap();
        ssock.bind(&addr).unwrap();

        let sock = ssock.listen(1024).unwrap();
        let config = EventLoopConfig {
            io_poll_timeout_ms: 1,
            notify_capacity: 4_096,
            messages_per_tick: 256,
            timer_tick_ms: 1,
            timer_wheel_size: 1_024,
            timer_capacity: 65_536,
        };
        let mut ev_loop : EventLoop<Server> = EventLoop::configured(config).unwrap();

        ev_loop.register_opt(&sock, SERVER, EventSet::readable(), PollOpt::edge()).unwrap();
        self.sock = Some(sock);

        ev_loop.run(self).unwrap();
    }
}

impl Handler for Server {
    type Timeout = usize;
    type Message = ();

    fn ready(&mut self, event_loop: &mut EventLoop<Server>, token: Token, events: EventSet) {

        let res = match token {
            SERVER => self.accept(event_loop),
            i => {
                if events.is_hup() {
                    let _ = self.conn_hup(event_loop, i);
                }
                if events.is_readable() {
                    let _ = self.conn_readable(event_loop, i);
                }
                if events.is_writable() {
                    let _ = self.conn_writable(event_loop, i);
                }

                Ok(())
            }
        };
        res.unwrap();
    }
}
