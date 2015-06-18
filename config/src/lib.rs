extern crate rand;
extern crate time;

use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Error as IOError;
use std::num::ParseIntError;
use std::path::Path;

use time::get_time;

pub struct Config {
    pub daemonize: bool,
    pub databases: u8,
    pub pidfile: String,
    pub bind: Vec<String>,
    pub port: u16,
    pub tcp_keepalive: u32,
}

pub fn ustime() -> i64 {
    let tv = get_time();
    tv.sec * 1000000 + tv.nsec as i64
}

pub fn mstime() -> i64 {
    ustime() / 1000
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


#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::fs::create_dir;
    use std::io::Write;
    use rand::random;

    macro_rules! config {
        ($str: expr) => ({
            let dirpath = format!("tmp/{}", mstime());
            let filepath = format!("{}/{}.conf", dirpath, random::<u64>());
            match create_dir("tmp") { _ => () }
            match create_dir(dirpath) { _ => () }
            match File::create(filepath.clone()).unwrap().write_all($str) { _ => () }
            let mut config = Config::new();
            config.parsefile(filepath).unwrap();
            config
        })
    }

    #[test]
    fn parse_bind() {
        let config = config!(b"bind 1.2.3.4\nbind 5.6.7.8");
        assert_eq!(config.bind, vec!["1.2.3.4", "5.6.7.8"]);
        assert_eq!(config.port, 6379);
    }

    #[test]
    fn parse_port() {
        let config = config!(b"port 12345");
        assert_eq!(config.port, 12345);
        assert_eq!(config.addresses(), vec![("127.0.0.1", 12345)]);
    }

    #[test]
    fn parse_bind_port() {
        let config = config!(b"bind 127.0.0.1\nport 12345");
        assert_eq!(config.bind, vec!["127.0.0.1"]);
        assert_eq!(config.port, 12345);
    }

    #[test]
    fn parse_daemonize_yes() {
        let config = config!(b"daemonize yes");
        assert!(config.daemonize);
    }

    #[test]
    fn parse_daemonize_no() {
        let config = config!(b"daemonize no");
        assert!(!config.daemonize);
    }

    #[test]
    fn parse_databases() {
        let config = config!(b"databases 20");
        assert_eq!(config.databases, 20);
    }

    #[test]
    fn parse_keepalive() {
        let config = config!(b"tcp-keepalive 123");
        assert_eq!(config.tcp_keepalive, 123);
    }
}
