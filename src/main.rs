extern crate rsedis;
extern crate config;
extern crate logger;
#[cfg(not(feature = "networking-mio"))]
extern crate networking;
#[cfg(feature = "networking-mio")]
extern crate networkingmio;
extern crate compat;

mod release;

use std::env::args;
use std::process::exit;

use compat::getpid;
use config::Config;
#[cfg(not(feature = "networking-mio"))]
use networking::Server;
#[cfg(feature = "networking-mio")]
use networkingmio::Server;
use logger::{Logger, Level};
use release::*;

fn main() {
    let mut config = Config::new(Logger::new(Level::Notice));
    match args().nth(1) {
        Some(f) => match config.parsefile(f) {
            Ok(_) => (),
            Err(_) => {
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
        db.rustc_version = RUSTC_VERSION;
    }

    if !daemonize {
        println!("Port: {}", port);
        println!("PID: {}", getpid());
    }
    server.run();
}
