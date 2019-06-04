use std::io::Read;
use std::sync::mpsc::channel;

use database::Database;
use logger::Level;
use parser::{ParseError, Parser};

use command;

const UNEXPECTED_END: &'static str =
    "Unexpected end of file reading the append only file. You can: 1) Make a backup of your AOF \
     file, then use ./redis-check-aof --fix <filename>. 2) Alternatively you can set the \
     'aof-load-truncated' configuration option to yes and restart the server.";

pub fn load(db: &mut Database) {
    let mut aof = db.aof.take().unwrap();
    db.loading = true;
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
                if parser.written > parser.position {
                    if !db.config.aof_load_truncated {
                        log_and_exit!(db.config.logger, Warning, 1, "{}", UNEXPECTED_END);
                    }
                    aof.truncate(parser.position);
                }
                break;
            }
        }

        let parsed_command = match parser.next() {
            Ok(p) => p,
            Err(err) => {
                match err {
                    ParseError::Incomplete => {
                        continue;
                    }
                    // TODO: break, continue, or panic?
                    ParseError::BadProtocol(s) => {
                        log!(
                            db.config.logger,
                            Warning,
                            "Bad file format reading the append only file {:?}",
                            s
                        );
                        break;
                    }
                    _ => panic!("Broken aof {:?}"),
                }
            }
        };

        command::command(parsed_command, db, &mut client).unwrap();
    }
    if client.multi && !db.config.aof_load_truncated {
        log_and_exit!(db.config.logger, Warning, 1, "{}", UNEXPECTED_END);
    }
    db.aof = Some(aof);
    db.loading = false;
}
