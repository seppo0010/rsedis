use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Error as IOError;
use std::num::ParseIntError;
use std::path::Path;

pub struct Config {
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
            bind: vec!["127.0.0.1".to_owned()],
            port: port,
        }
    }

    pub fn new(confpath: Option<String>) -> Result<Config, ConfigError> {
        let mut bind = Vec::new();
        let mut port = 6379;
        match confpath {
            Some(fname) => {
                let path = Path::new(&*fname);
                let file = BufReader::new(try!(File::open(&path)));
                for line_iter in file.lines() {
                    let lline = try!(line_iter);
                    let line = lline.trim();
                    if line.len() > 0 && &line[0..0] == "#" {
                        continue;
                    }
                    if &line[0..4] == "bind" {
                        bind.extend(line[4..].split(' ').filter(|x| x.trim().len() > 0).map(|x| x.trim().to_owned()));
                    }
                    else if &line[0..4] == "port" {
                        port = try!(line[4..].trim().parse::<u16>());
                    }
                }
                if bind.len() == 0 {
                    bind.push("0.0.0.0".to_owned());
                }
            },
            None => bind.push("127.0.0.1".to_owned()),
        };
        Ok(Config {
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
