#![feature(owned_ascii_ext)]

#[macro_use(log)]
extern crate logger;
extern crate rand;
extern crate time;
extern crate util;

use std::ascii::OwnedAsciiExt;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Error as IOError;
use std::io::Write;
use std::num::ParseIntError;
use std::path::Path;
use std::str::from_utf8;
use std::str::FromStr;
use std::str::Utf8Error;

use util::splitargs;
use logger::{Logger, Level};

pub struct Config {
    pub logger: Logger,
    pub daemonize: bool,
    pub databases: u8,
    pub pidfile: String,
    pub bind: Vec<String>,
    pub port: u16,
    pub tcp_keepalive: u32,
    pub active_rehashing: bool,
    pub set_max_intset_entries: usize,
    pub timeout: u64,
    pub unixsocket: Option<String>,
    pub unixsocketperm: u32,
    pub rename_commands: HashMap<String, Option<String>>,
    pub requirepass: Option<String>,
    pub tcp_backlog: i32,
}

#[derive(Debug)]
pub enum ConfigError {
    InvalidFormat,
    InvalidParameter,
    IOError(IOError),
    FileNotFound,
}

fn read_string(args: Vec<Vec<u8>>) -> Result<String, ConfigError> {
    if args.len() != 2 {
        Err(ConfigError::InvalidFormat)
    } else {
        Ok(try!(from_utf8(&*args[1])).to_owned())
    }
}

fn read_parse<T>(args: Vec<Vec<u8>>) -> Result<T, ConfigError>
        where T: FromStr {
    let s = try!(read_string(args));
    match s.parse() {
        Ok(f) => Ok(f),
        Err(_) => Err(ConfigError::InvalidParameter),
    }
}

fn read_bool(args: Vec<Vec<u8>>) -> Result<bool, ConfigError> {
    Ok(match &*try!(read_string(args)) {
        "yes" => true,
        "no" => false,
        _ => return Err(ConfigError::InvalidFormat),
    })
}

impl Config {
    pub fn mock(port: u16, logger: Logger) -> Config {
        Config {
            logger: logger,
            active_rehashing: true,
            daemonize: false,
            databases: 16,
            pidfile: "/var/run/sredis.pid".to_owned(),
            bind: vec!["127.0.0.1".to_owned()],
            port: port,
            tcp_keepalive: 0,
            set_max_intset_entries: 512,
            timeout: 0,
            unixsocket: None,
            unixsocketperm: 0700,
            rename_commands: HashMap::new(),
            requirepass: None,
            tcp_backlog: 511,
        }
    }

    pub fn new(logger: Logger) -> Config {
        Config {
            logger: logger,
            active_rehashing: true,
            daemonize: false,
            databases: 16,
            pidfile: "/var/run/sredis.pid".to_owned(),
            bind: vec![],
            port: 6379,
            tcp_keepalive: 0,
            set_max_intset_entries: 512,
            timeout: 0,
            unixsocket: None,
            unixsocketperm: 0700,
            rename_commands: HashMap::new(),
            requirepass: None,
            tcp_backlog: 511,
        }
    }

    pub fn parsefile(&mut self, fname: String) -> Result<(), ConfigError> {
        let path = Path::new(&*fname);
        let file = BufReader::new(match File::open(&path) {
            Ok(f) => f,
            Err(_) => {
                log!(self.logger, Warning, "Fatal error, can't open config file '{}'", fname);
                return Err(ConfigError::FileNotFound);
            }
        });
        for line_iter in file.lines() {
            let lline = try!(line_iter);
            let line = lline.trim();
            if line.starts_with("#") {
                continue;
            }

            let args = match splitargs(line.as_bytes()) {
                Ok(args) => args,
                Err(_) => return Err(ConfigError::InvalidFormat),
            };

            if args.len() == 0 {
                continue;
            }

            match &*args[0] {
                b"bind" => self.bind.extend(args[1..].iter().filter(|x| x.len() > 0).map(|x| match from_utf8(x) {
                            Ok(s) => s.to_owned(),
                            Err(_) => "".to_owned(), // TODO: return ConfigError
                            })),
                b"port" => self.port = try!(read_parse(args)),
                b"activerehashing" => self.active_rehashing = try!(read_bool(args)),
                b"daemonize" => self.daemonize = try!(read_bool(args)),
                b"databases" => self.databases = try!(read_parse(args)),
                b"tcp-keepalive" => self.tcp_keepalive = try!(read_parse(args)),
                b"set-max-intset-entries" => self.set_max_intset_entries = try!(read_parse(args)),
                b"timeout" => self.timeout = try!(read_parse(args)),
                b"unixsocket" => self.unixsocket = Some(try!(read_string(args)).to_owned()),
                b"unixsocketperm" => self.unixsocketperm = try!(u32::from_str_radix(&*try!(read_string(args)), 8)),
                b"pidfile" => self.pidfile = try!(read_string(args)).to_owned(),
                b"logfile" => {
                    let logfile = try!(read_string(args));
                    if logfile.len() > 0 {
                        try!(self.logger.set_logfile(&*logfile))
                    }
                },
                b"loglevel" => self.logger.set_loglevel(match &*try!(read_string(args)) {
                    "debug" => Level::Debug,
                    "verbose" => Level::Verbose,
                    "notice" => Level::Notice,
                    "warning" => Level::Warning,
                    _ => return Err(ConfigError::InvalidParameter),
                }),
                b"rename-command" => {
                    if args.len() != 3 {
                        return Err(ConfigError::InvalidFormat)
                    } else {
                        let command = try!(from_utf8(&*args[1])).to_owned();
                        let newname = try!(from_utf8(&*args[2])).to_owned();
                        if newname.len() > 0 {
                            self.rename_commands.insert(newname.into_ascii_lowercase(), Some(command.clone().into_ascii_lowercase()));
                        }
                        self.rename_commands.insert(command.into_ascii_lowercase(), None);
                    }
                },
                b"requirepass" => self.requirepass = Some(try!(read_string(args)).to_owned()),
                b"tcp-backlog" => self.tcp_backlog = try!(read_parse(args)),
                b"include" => if args.len() != 2 {
                    return Err(ConfigError::InvalidFormat)
                } else {
                    try!(self.parsefile(try!(from_utf8(&*args[1])).to_owned()));
                },
                _ => writeln!(&mut std::io::stderr(), "Unknown configuration {:?}", line).unwrap(),
            };
        }

        Ok(())
    }

    pub fn addresses(&self) -> Vec<(String, u16)> {
        if self.bind.len() == 0 {
            vec![("127.0.0.1".to_owned(), self.port)]
        } else {
            self.bind.iter().map(|s| (s.clone(), self.port)).collect::<Vec<_>>()
        }
    }
}

impl From<IOError> for ConfigError {
    fn from(e: IOError) -> ConfigError { ConfigError::IOError(e) }
}

impl From<ParseIntError> for ConfigError {
    fn from(_: ParseIntError) -> ConfigError { ConfigError::InvalidParameter }
}

impl From<Utf8Error> for ConfigError {
    fn from(_: Utf8Error) -> ConfigError { ConfigError::InvalidParameter }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs::File;
    use std::fs::create_dir;
    use std::io::Write;

    use rand::random;

    use logger::{Logger, Level};
    use util::mstime;

    macro_rules! config {
        ($str: expr, $logger: expr) => ({
            let dirpath = format!("tmp/{}", mstime());
            let filepath = format!("{}/{}.conf", dirpath, random::<u64>());
            match create_dir("tmp") { _ => () }
            match create_dir(dirpath) { _ => () }
            match File::create(filepath.clone()).unwrap().write_all($str) { _ => () }
            let mut config = Config::new($logger);
            config.parsefile(filepath).unwrap();
            config
        })
    }

    #[test]
    fn parse_bind() {
        let config = config!(b"bind 1.2.3.4\nbind 5.6.7.8", Logger::new(Level::Warning));
        assert_eq!(config.bind, vec!["1.2.3.4", "5.6.7.8"]);
        assert_eq!(config.port, 6379);
    }

    #[test]
    fn parse_port() {
        let config = config!(b"port 12345", Logger::new(Level::Warning));
        assert_eq!(config.port, 12345);
        assert_eq!(config.addresses(), vec![("127.0.0.1".to_owned(), 12345)]);
    }

    #[test]
    fn parse_bind_port() {
        let config = config!(b"bind 127.0.0.1\nport 12345", Logger::new(Level::Warning));
        assert_eq!(config.bind, vec!["127.0.0.1"]);
        assert_eq!(config.port, 12345);
    }

    #[test]
    fn parse_daemonize_yes() {
        let config = config!(b"daemonize yes", Logger::new(Level::Warning));
        assert!(config.daemonize);
    }

    #[test]
    fn parse_daemonize_no() {
        let config = config!(b"daemonize no", Logger::new(Level::Warning));
        assert!(!config.daemonize);
    }

    #[test]
    fn parse_active_rehashing_yes() {
        let config = config!(b"activerehashing yes", Logger::new(Level::Warning));
        assert!(config.active_rehashing);
    }

    #[test]
    fn parse_active_rehashing_no() {
        let config = config!(b"activerehashing no", Logger::new(Level::Warning));
        assert!(!config.active_rehashing);
    }

    #[test]
    fn parse_databases() {
        let config = config!(b"databases 20", Logger::new(Level::Warning));
        assert_eq!(config.databases, 20);
    }

    #[test]
    fn parse_keepalive() {
        let config = config!(b"tcp-keepalive 123", Logger::new(Level::Warning));
        assert_eq!(config.tcp_keepalive, 123);
    }

    #[test]
    fn parse_keepalive_quotes() {
        let config = config!(b"tcp-keepalive \"123\"", Logger::new(Level::Warning));
        assert_eq!(config.tcp_keepalive, 123);
    }

    #[test]
    fn parse_set_max_intset_entries() {
        let config = config!(b"set-max-intset-entries 123456", Logger::new(Level::Warning));
        assert_eq!(config.set_max_intset_entries, 123456);
    }

    #[test]
    fn parse_timeout() {
        let config = config!(b"timeout 23456", Logger::new(Level::Warning));
        assert_eq!(config.timeout, 23456);
    }

    #[test]
    fn parse_unixsocket() {
        let config = config!(b"unixsocket /dev/null\nunixsocketperm 777", Logger::new(Level::Warning));
        assert_eq!(config.unixsocket, Some("/dev/null".to_owned()));
        assert_eq!(config.unixsocketperm, 511);
    }

    #[test]
    fn parse_rename_commands() {
        let config = config!(b"rename-command C1 C2\nrename-command HELLO world", Logger::new(Level::Warning));
        let mut h = HashMap::new();
        h.insert("c2".to_owned(), Some("c1".to_owned()));
        h.insert("c1".to_owned(), None);
        h.insert("world".to_owned(), Some("hello".to_owned()));
        h.insert("hello".to_owned(), None);
        assert_eq!(config.rename_commands, h);
    }

    #[test]
    fn parse_requirepass() {
        let config = config!(b"requirepass THISISASTRONGPASSWORD", Logger::new(Level::Warning));
        assert_eq!(config.requirepass, Some("THISISASTRONGPASSWORD".to_owned()));
    }
}
