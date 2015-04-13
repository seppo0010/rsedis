extern crate rsedis;

use std::net::Ipv4Addr;
use std::str::FromStr;

use rsedis::networking::Server;

fn main() {
    let mut server = Server::new(Ipv4Addr::from_str("127.0.0.1").unwrap(), 6379);
    server.run();
}
