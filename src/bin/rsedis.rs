extern crate rsedis;

use rsedis::networking::Server;

fn main() {
    #![allow(dead_code)]
    let mut server = Server::new(("127.0.0.1", 6379));
    server.run();
}
