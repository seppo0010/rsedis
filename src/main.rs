extern crate rsedis;
extern crate config;
extern crate logger;
extern crate networking;
extern crate compat;

mod release;

use std::env::args;
use std::process::exit;
use std::thread;

use compat::getpid;
use config::Config;
use networking::Server;
use logger::{Logger, Level};
use release::{GIT_SHA1, GIT_DIRTY};

fn main() {
    let mut config = Config::new(Logger::new(Level::Notice));
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
    let (port, daemonize) = (config.port, config.daemonize);
    let mut server = Server::new(config);
    {
        let mut db = server.get_mut_db();
        db.git_sha1 = GIT_SHA1;
        db.git_dirty = GIT_DIRTY;
        db.version = env!("CARGO_PKG_VERSION");
    }

    if !daemonize {
        println!("Port: {}", port);
        println!("PID: {}", getpid());
    }
    server.run();
}
