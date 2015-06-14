use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Error as IOError;
use std::num::ParseIntError;
use std::path::Path;

pub struct Config {
    pub daemonize: bool,
    pub databases: u8,
    pub pidfile: String,
    pub bind: Vec<String>,
    pub port: u16,
    pub tcp_keepalive: u32,
}

#[derive(Debug)]
pub enum ConfigError {
    IOError(IOError),
    ParseIntError(ParseIntError),
}

impl Config {
    pub fn mock(port: u16) -> Config {
        Config {
            daemonize: false,
            databases: 16,
            pidfile: "/var/run/sredis.pid".to_owned(),
            bind: vec!["127.0.0.1".to_owned()],
            port: port,
            tcp_keepalive: 0,
        }
    }

    pub fn new() -> Config {
        Config {
            daemonize: false,
            databases: 16,
            pidfile: "/var/run/sredis.pid".to_owned(),
            bind: vec![],
            port: 6379,
            tcp_keepalive: 0,
        }
    }

    pub fn parsefile(&mut self, fname: String) -> Result<(), ConfigError> {
        let path = Path::new(&*fname);
        let file = BufReader::new(try!(File::open(&path)));
        for line_iter in file.lines() {
            let lline = try!(line_iter);
            let line = lline.trim();
            if line.starts_with("#") {
                continue;
            }

            if line.starts_with("bind ") {
                self.bind.extend(line[5..].split(' ').filter(|x| x.trim().len() > 0).map(|x| x.trim().to_owned()));
            }
            else if line.starts_with("port ") {
                self.port = try!(line[5..].trim().parse());
            }
            else if line.starts_with("daemonize ") {
                self.daemonize = line[9..].trim() == "yes";
            }
            else if line.starts_with("databases ") {
                self.databases = try!(line[9..].trim().parse());
            }
            else if line.starts_with("pidfile ") {
                self.pidfile = line[8..].trim().to_owned();
            }
            else if line.starts_with("include ") {
                try!(self.parsefile(line[8..].trim().to_owned()));
            }
            else if line.starts_with("tcp-keepalive ") {
                self.tcp_keepalive = try!(line[13..].trim().parse());
            }
        }

        Ok(())
    }

    pub fn addresses(&self) -> Vec<(&str, u16)> {
        if self.bind.len() == 0 {
            vec![("127.0.0.1", self.port)]
        } else {
            self.bind.iter().map(|s| (&s[..], self.port)).collect::<Vec<_>>()
        }
    }
}

impl From<IOError> for ConfigError {
    fn from(e: IOError) -> ConfigError { ConfigError::IOError(e) }
}

impl From<ParseIntError> for ConfigError {
    fn from(e: ParseIntError) -> ConfigError { ConfigError::ParseIntError(e) }
}
