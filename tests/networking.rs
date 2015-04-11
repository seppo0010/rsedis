extern crate rsedis;

use std::net::TcpStream;
use std::io::Write;
use std::thread;

use rsedis::networking::Server;

#[test]
fn parse_ping() {
    let port: i32 = 6379;
    thread::spawn(move || {
        let mut server = Server::new("127.0.0.1", &port);
        server.start();
    });

    // FIXME
    // needing a sleep is bad and you should feel bad about it
    thread::sleep_ms(20);
    let addr = format!("127.0.0.1:{}", port);
    let streamres = TcpStream::connect(&*addr);
    assert!(streamres.is_ok());
    let mut stream = streamres.unwrap();
    let message = b"*2\r\n$4\r\nping\r\n$4\r\npong\r\n";
    assert!(stream.write(message).is_ok());
}
