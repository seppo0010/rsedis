#[macro_use(log_and_exit)]
extern crate logger;
extern crate rand;
extern crate time;
extern crate util;

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

use logger::{Level, Logger};
use util::splitargs;

pub struct Config {
    pub logger: Logger,
    pub daemonize: bool,
    pub databases: u8,
    pub pidfile: String,
    pub dir: String,
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
    pub syslog_enabled: bool,
    pub syslog_ident: String,
    pub syslog_facility: String,
    pub hz: u32,
    pub appendonly: bool,
    pub appendfilename: String,
    pub aof_load_truncated: bool,
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
        Ok(from_utf8(&*args[1])?.to_owned())
    }
}

fn read_parse<T>(args: Vec<Vec<u8>>) -> Result<T, ConfigError>
where
    T: FromStr,
{
    let s = read_string(args)?;
    match s.parse() {
        Ok(f) => Ok(f),
        Err(_) => Err(ConfigError::InvalidParameter),
    }
}

fn read_bool(args: Vec<Vec<u8>>) -> Result<bool, ConfigError> {
    Ok(match &*read_string(args)? {
        "yes" => true,
        "no" => false,
        _ => return Err(ConfigError::InvalidFormat),
    })
}

impl Config {
    pub fn default(port: u16, logger: Logger) -> Config {
        Config {
            logger,
            active_rehashing: true,
            daemonize: false,
            databases: 16,
            pidfile: "/var/run/rsedis.pid".to_owned(),
            dir: "./".to_owned(),
            bind: vec![],
            port,
            tcp_keepalive: 0,
            set_max_intset_entries: 512,
            timeout: 0,
            unixsocket: None,
            unixsocketperm: 0o700,
            rename_commands: HashMap::new(),
            requirepass: None,
            tcp_backlog: 511,
            syslog_enabled: false,
            syslog_ident: "rsedis".to_owned(),
            syslog_facility: "local0".to_owned(),
            hz: 10,
            appendonly: false,
            appendfilename: "appendonly.aof".to_owned(),
            aof_load_truncated: false,
        }
    }

    pub fn new(logger: Logger) -> Config {
        Self::default(6379, logger)
    }

    pub fn parsefile(&mut self, fname: String) -> Result<(), ConfigError> {
        let path = Path::new(&*fname);
        let file = BufReader::new(match File::open(&path) {
            Ok(f) => f,
            Err(_) => {
                log_and_exit!(
                    self.logger,
                    Warning,
                    1,
                    "Fatal error, can't open config file '{}'",
                    fname
                );
                return Err(ConfigError::FileNotFound);
            }
        });
        for line_iter in file.lines() {
            let lline = line_iter?;
            let line = lline.trim();
            if line.starts_with('#') {
                continue;
            }

            let args = match splitargs(line.as_bytes()) {
                Ok(args) => args,
                Err(_) => return Err(ConfigError::InvalidFormat),
            };

            if args.is_empty() {
                continue;
            }

            match &*args[0] {
                b"bind" => {
                    self.bind
                        .extend(args[1..].iter().filter(|x| !x.is_empty()).map(|x| {
                            match from_utf8(x) {
                                Ok(s) => s.to_owned(),
                                Err(_) => "".to_owned(), // TODO: return ConfigError
                            }
                        }))
                }
                b"port" => self.port = read_parse(args)?,
                b"activerehashing" => self.active_rehashing = read_bool(args)?,
                b"daemonize" => self.daemonize = read_bool(args)?,
                b"databases" => self.databases = read_parse(args)?,
                b"tcp-keepalive" => self.tcp_keepalive = read_parse(args)?,
                b"set-max-intset-entries" => self.set_max_intset_entries = read_parse(args)?,
                b"timeout" => self.timeout = read_parse(args)?,
                b"unixsocket" => self.unixsocket = Some(read_string(args)?.to_owned()),
                b"unixsocketperm" => {
                    self.unixsocketperm = u32::from_str_radix(&*read_string(args)?, 8)?
                }
                b"pidfile" => self.pidfile = read_string(args)?.to_owned(),
                b"dir" => self.dir = read_string(args)?.to_owned(),
                b"logfile" => {
                    let logfile = read_string(args)?;
                    if !logfile.is_empty() {
                        self.logger.set_logfile(&*logfile)?
                    }
                }
                b"loglevel" => self.logger.set_loglevel(match &*read_string(args)? {
                    "debug" => Level::Debug,
                    "verbose" => Level::Verbose,
                    "notice" => Level::Notice,
                    "warning" => Level::Warning,
                    _ => return Err(ConfigError::InvalidParameter),
                }),
                b"rename-command" => {
                    if args.len() != 3 {
                        return Err(ConfigError::InvalidFormat);
                    } else {
                        let command = from_utf8(&*args[1])?.to_owned();
                        let newname = from_utf8(&*args[2])?.to_owned();
                        if !newname.is_empty() {
                            self.rename_commands.insert(
                                newname.to_lowercase(),
                                Some(command.clone().to_lowercase()),
                            );
                        }
                        self.rename_commands.insert(command.to_lowercase(), None);
                    }
                }
                b"requirepass" => self.requirepass = Some(read_string(args)?.to_owned()),
                b"tcp-backlog" => self.tcp_backlog = read_parse(args)?,
                b"syslog-enabled" => self.syslog_enabled = read_bool(args)?,
                b"syslog-ident" => self.syslog_ident = read_string(args)?.to_owned(),
                b"syslog-facility" => self.syslog_facility = read_string(args)?.to_owned(),
                b"hz" => self.hz = read_parse(args)?,
                b"appendonly" => self.appendonly = read_bool(args)?,
                b"appendfilename" => self.appendfilename = read_string(args)?.to_owned(),
                b"aof-load-truncated" => self.aof_load_truncated = read_bool(args)?,
                b"include" => {
                    if args.len() != 2 {
                        return Err(ConfigError::InvalidFormat);
                    } else {
                        self.parsefile(from_utf8(&*args[1])?.to_owned())?;
                    }
                }
                _ => writeln!(&mut std::io::stderr(), "Unknown configuration {:?}", line).unwrap(),
            };
        }
        if self.syslog_enabled {
            self.logger
                .set_syslog(&self.syslog_ident, &self.syslog_facility);
        }

        Ok(())
    }

    pub fn addresses(&self) -> Vec<(String, u16)> {
        if self.bind.is_empty() {
            vec![("127.0.0.1".to_owned(), self.port)]
        } else {
            self.bind
                .iter()
                .map(|s| (s.clone(), self.port))
                .collect::<Vec<_>>()
        }
    }
}

impl From<IOError> for ConfigError {
    fn from(e: IOError) -> ConfigError {
        ConfigError::IOError(e)
    }
}

impl From<ParseIntError> for ConfigError {
    fn from(_: ParseIntError) -> ConfigError {
        ConfigError::InvalidParameter
    }
}

impl From<Utf8Error> for ConfigError {
    fn from(_: Utf8Error) -> ConfigError {
        ConfigError::InvalidParameter
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs::create_dir;
    use std::fs::File;
    use std::io::Write;

    use rand::random;

    use logger::{Level, Logger};
    use util::mstime;

    macro_rules! config {
        ($str: expr, $logger: expr) => {{
            let dirpath = format!("tmp/{}", mstime());
            let filepath = format!("{}/{}.conf", dirpath, random::<u64>());
            match create_dir("tmp") {
                _ => (),
            }
            match create_dir(dirpath) {
                _ => (),
            }
            match File::create(filepath.clone()).unwrap().write_all($str) {
                _ => (),
            }
            let mut config = Config::new($logger);
            config.parsefile(filepath).unwrap();
            config
        }};
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
        let config = config!(
            b"set-max-intset-entries 123456",
            Logger::new(Level::Warning)
        );
        assert_eq!(config.set_max_intset_entries, 123456);
    }

    #[test]
    fn parse_timeout() {
        let config = config!(b"timeout 23456", Logger::new(Level::Warning));
        assert_eq!(config.timeout, 23456);
    }

    #[test]
    fn parse_unixsocket() {
        let config = config!(
            b"unixsocket /dev/null\nunixsocketperm 777",
            Logger::new(Level::Warning)
        );
        assert_eq!(config.unixsocket, Some("/dev/null".to_owned()));
        assert_eq!(config.unixsocketperm, 511);
    }

    #[test]
    fn parse_rename_commands() {
        let config = config!(
            b"rename-command C1 C2\nrename-command HELLO world",
            Logger::new(Level::Warning)
        );
        let mut h = HashMap::new();
        h.insert("c2".to_owned(), Some("c1".to_owned()));
        h.insert("c1".to_owned(), None);
        h.insert("world".to_owned(), Some("hello".to_owned()));
        h.insert("hello".to_owned(), None);
        assert_eq!(config.rename_commands, h);
    }

    #[test]
    fn parse_requirepass() {
        let config = config!(
            b"requirepass THISISASTRONGPASSWORD",
            Logger::new(Level::Warning)
        );
        assert_eq!(config.requirepass, Some("THISISASTRONGPASSWORD".to_owned()));
    }
}
