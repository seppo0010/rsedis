extern crate rsedis;

use std::net::TcpStream;
use std::io::Write;

use rsedis::networking::Server;

#[test]
fn parse_ping() {
    let port: i32 = 6379;
    let server = Server::new("127.0.0.1", &port);
    server.start();

    let addr = format!("127.0.0.1:{}", port);
    let streamres = TcpStream::connect(&*addr);
    assert!(streamres.is_ok());
    let mut stream = streamres.unwrap();
    let message = b"*2\r\n$4\r\nping\r\n$4\r\npong\r\n";
    assert!(stream.write(message).is_ok());
}
