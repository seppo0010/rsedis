extern crate rsedis;

use rsedis::networking::Server;
use rsedis::config::Config;
use std::env::args;

fn main() {
    #![allow(dead_code)]
    let config = Config::new(args().nth(1)).unwrap();
    let mut server = Server::new(config);
    server.run();
}
