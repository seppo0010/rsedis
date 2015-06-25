extern crate rsedis;
extern crate config;
extern crate logger;
extern crate networking;

use std::env::args;
use std::process::exit;

use config::Config;
use networking::Server;
use logger::{Logger, Level};

fn main() {
    let mut logger = Logger::new(Level::Notice);
    let mut config = Config::new(&mut logger);
    match args().nth(1) {
        Some(f) => match config.parsefile(f) {
            Ok(_) => (),
            Err(_) => {
                thread::sleep_ms(100);
                // I'm not proud, but fatal errors are logged in a background thread
                // I need to ensure they were printed
                exit(1);
            },
        },
        None => (),
    }
    let mut server = Server::new(config);
    server.run();
}
