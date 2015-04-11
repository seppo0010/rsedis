use std::net::{TcpListener, TcpStream};
use std::io::Read;
use std::thread;

use super::parser::parse;

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
            println!("gonna print {}", len);
            if len == 0 {
                break;
            }
            let try_parser = parse(&buffer, len);
            if try_parser.is_err() {
                break;
            }
            let parser = try_parser.unwrap();
            println!("{}", parser.argc);
            for i in 0..parser.argc {
                println!("{}", parser.get_str(i).unwrap())
            }
        };
    }
}

impl Server {
    pub fn new(ip: &str, port: &i32) -> Server {
        return Server {
            ip: ip.to_string(),
            port: *port,
        }
    }

    pub fn handle_client(&mut self, stream: TcpStream) {
        println!("Client connected");
        thread::spawn(move || {
            let mut client = Client::new(stream);
            client.read();
        });
    }

    pub fn start(&mut self) {
        let addr: String = format!("{}:{}", self.ip, self.port);
        let listener = TcpListener::bind(&*addr).unwrap();
        println!("Listening to new connections on {}", addr);
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    // connection succeeded
                    self.handle_client(stream)
                }
                Err(e) => { println!("error {}", e); }
            }
        }
        // close the socket server
        drop(listener);
    }

}

pub fn new_server(ip: &str, port: &i32) -> Server {
    return Server::new(ip, port);
}
