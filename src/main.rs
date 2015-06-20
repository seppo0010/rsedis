extern crate rsedis;
extern crate config;
extern crate networking;

use networking::Server;
use config::Config;
use std::env::args;

fn main() {
    let mut config = Config::new();
    match args().nth(1) {
        Some(f) => config.parsefile(f).unwrap(),
        None => (),
    }
    let mut server = Server::new(config);
    server.run();
}
