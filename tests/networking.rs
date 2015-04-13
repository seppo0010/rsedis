extern crate rsedis;

use std::str::FromStr;
use std::str::from_utf8;
use std::net::Ipv4Addr;
use std::net::TcpStream;
use std::io::Write;
use std::io::Read;

use rsedis::networking::Server;

#[test]
fn parse_ping() {
    let port = 6379;

    let mut server = Server::new(Ipv4Addr::from_str("127.0.0.1").unwrap(), port);
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
    let port = 6380;
    let mut server = Server::new(Ipv4Addr::from_str("127.0.0.1").unwrap(), port);
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
    let port = 6381;
    let mut server = Server::new(Ipv4Addr::from_str("127.0.0.1").unwrap(), port);
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
