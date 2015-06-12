use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Error as IOError;
use std::num::ParseIntError;
use std::path::Path;

pub struct Config {
    pub daemonize: bool,
    pub bind: Vec<String>,
    pub port: u16,
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
            bind: vec!["127.0.0.1".to_owned()],
            port: port,
        }
    }

    pub fn new(confpath: Option<String>) -> Result<Config, ConfigError> {
        let mut bind = Vec::new();
        let mut port = 6379;
        let mut daemonize = false;
        match confpath {
            Some(fname) => {
                let path = Path::new(&*fname);
                let file = BufReader::new(try!(File::open(&path)));
                for line_iter in file.lines() {
                    let lline = try!(line_iter);
                    let line = lline.trim();
                    if line.starts_with("#") {
                        continue;
                    }

                    if line.starts_with("bind ") {
                        bind.extend(line[5..].split(' ').filter(|x| x.trim().len() > 0).map(|x| x.trim().to_owned()));
                    }
                    else if line.starts_with("port ") {
                        port = try!(line[5..].trim().parse::<u16>());
                    }
                    else if line.starts_with("daemonize ") {
                        daemonize = line[9..].trim() == "yes"
                    }
                }

                if bind.len() == 0 {
                    bind.push("127.0.0.1".to_owned());
                }
            },
            None => bind.push("127.0.0.1".to_owned()),
        };
        Ok(Config {
            daemonize: daemonize,
            bind: bind,
            port: port,
        })
    }

    pub fn addresses(&self) -> Vec<(&str, u16)> {
        self.bind.iter().map(|s| (&s[..], self.port)).collect::<Vec<_>>()
    }
}

impl From<IOError> for ConfigError {
    fn from(e: IOError) -> ConfigError { ConfigError::IOError(e) }
}

impl From<ParseIntError> for ConfigError {
    fn from(e: ParseIntError) -> ConfigError { ConfigError::ParseIntError(e) }
}
