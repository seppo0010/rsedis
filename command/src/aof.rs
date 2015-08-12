use std::io::Read;
use std::sync::mpsc::channel;

use database::Database;
use logger::Level;
use parser::{Parser, ParseError};

use command;

pub fn load(db: &mut Database) {
    let mut aof = db.aof.take().unwrap();
    let mut client = command::Client::new(channel().0, 0);
    let mut parser = Parser::new();
    loop {
        if parser.is_incomplete() {
            parser.allocate();
            let len = {
                let pos = parser.written;
                let mut buffer = parser.get_mut();

                match aof.read(&mut buffer[pos..]) {
                    Ok(r) => r,
                    Err(err) => panic!("Error reading aof: {:?}", err),
                }
            };
            parser.written += len;

            if len == 0 {
                // TODO: if there's something in the buffer
                // it might be an incomplete AOF
                if parser.written > parser.position {
                    aof.truncate(parser.position);
                }
                break;
            }
        }

        let parsed_command = match parser.next() {
            Ok(p) => p,
            Err(err) => match err {
                ParseError::Incomplete => { continue; }
                // TODO: break, continue, or panic?
                ParseError::BadProtocol(s) => { log!(db.config.logger, Warning, "Bad file format reading the append only file {:?}", s); break; },
                _ => panic!("Broken aof {:?}"),
            }
        };

        command::command(parsed_command, db, &mut client).unwrap();
    }
    if client.multi {
        log!(db.config.logger, Warning, "Unexpected end of file reading the append only file");
    }
    db.aof = Some(aof);
}
