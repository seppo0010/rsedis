use std::net::{TcpListener, TcpStream};
use std::io::Read;
use std::io::Write;
use std::thread;

use super::parser::parse;
use super::parser::ParseError;

pub struct Client {
    pub stream: TcpStream
}

pub struct Server {
    pub ip: String,
    pub port: i32,
}

impl Client {
    pub fn new(stream: TcpStream) -> Client {
        return Client {
            stream: stream
        }
    }

    pub fn read(&mut self) {
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
            if parser.argc == 1 && parser.get_str(0).unwrap() == "exit" {
                break;
            }
            if parser.argc == 2 && parser.get_str(0).unwrap() == "ping" {
                let response = parser.get_str(1).unwrap();
                let writeres = self.stream.write(&*format!("${}\r\n", response.len()).as_bytes());
                if writeres.is_err() {
                    break;
                }
                let writeres = self.stream.write(response.as_bytes());
                if writeres.is_err() {
                    break;
                }
                let writeres = self.stream.write(b"\r\n");
                if writeres.is_err() {
                    break;
                }
            }
            println!("{}", parser.argc);
            for i in 0..parser.argc {
                println!("{}", parser.get_str(i).unwrap())
            }
        };
    }
}

fn handle_client(stream: TcpStream) {
    thread::spawn(move || {
        let mut client = Client::new(stream);
        client.read();
    });
}

pub fn run(listener: TcpListener) {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_client(stream)
            }
            Err(e) => { println!("error {}", e); }
        }
    }
    drop(listener);
}

impl Server {
    pub fn new(ip: &str, port: &i32) -> Server {
        return Server {
            ip: ip.to_string(),
            port: *port,
        }
    }

    fn get_listener(&self) -> TcpListener {
        let addr: String = format!("{}:{}", self.ip, self.port);
        return TcpListener::bind(&*addr).unwrap();
    }
    pub fn run(&self) {
        run(self.get_listener());
    }

    pub fn start(&self) {
        let listener = self.get_listener();
        thread::spawn(move || {
            run(listener);
        });
    }

}

pub fn new_server(ip: &str, port: &i32) -> Server {
    return Server::new(ip, port);
}
