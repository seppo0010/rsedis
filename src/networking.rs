use std::net::{TcpListener, TcpStream};
use std::io::Read;
use std::str::from_utf8;
use std::thread;

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
            let usize = self.stream.read(&mut buffer).unwrap();
            if usize == 0 {
                break;
            }
            println!("{} {}", from_utf8(&buffer).unwrap(), usize);
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
                Err(e) => { println!("{}", e); }
            }
        }
        // close the socket server
        drop(listener);
    }

}

pub fn new_server(ip: &str, port: &i32) -> Server {
    return Server::new(ip, port);
}
