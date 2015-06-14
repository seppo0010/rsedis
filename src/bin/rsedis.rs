extern crate rsedis;

use rsedis::networking::Server;
use rsedis::config::Config;
use std::env::args;

fn main() {
    #![allow(dead_code)]
    let mut config = Config::new();
    match args().nth(1) {
        Some(f) => config.parsefile(f).unwrap(),
        None => (),
    }
    let mut server = Server::new(config);
    server.run();
}
