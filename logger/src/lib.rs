#[cfg(unix)]
extern crate syslog;

use std::fmt::{Debug, Error, Formatter};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{stderr, stdout, Write};
use std::iter::FromIterator;
use std::path::Path;
use std::process;
use std::sync::mpsc::{channel, Sender};
use std::thread;

/// Macro to log a message. Uses the `format!` syntax.
/// See `std::fmt` for more information.
///
/// # Examples
///
/// ```
/// # #[macro_use(log)]
/// # extern crate logger;
/// # use logger::{Logger, Level};
/// #
/// # fn main() {
/// # let logger = Logger::new(Level::Warning);
/// log!(logger, Debug, "hello {}", "world");
/// # }
/// ```
#[macro_export]
macro_rules! log {
    ($logger: expr, $level: ident, $($arg:tt)*) => ({
        $logger.log(Level::$level, format!($($arg)*), None)
    })
}

#[macro_export]
macro_rules! log_and_exit {
    ($logger: expr, $level: ident, $code: expr, $($arg:tt)*) => ({
        $logger.log(Level::$level, format!($($arg)*), Some($code))
    })
}

/// Macro to send a message to a `Sender<(Level, String)>`.
/// Uses the `format!` syntax.
/// See `std::fmt` for more information.
///
/// # Examples
///
/// ```
/// # #[macro_use(sendlog)]
/// # extern crate logger;
/// # use logger::{Logger, Level};
/// # use std::sync::mpsc::channel;
/// #
/// # fn main() {
/// # let (tx, rx) = channel();
/// # let logger = Logger::channel(Level::Debug, tx);
/// # let sender = logger.sender();
/// sendlog!(sender, Debug, "hello {}", "world");
/// # assert_eq!(rx.recv().unwrap(), b"hello world\n");
/// # }
/// ```
#[macro_export]
macro_rules! sendlog {
    ($sender: expr, $level: ident, $($arg:tt)*) => ({
        $sender.send((Level::$level, format!($($arg)*)))
    })
}

enum Output {
    /// Sends logs to a channel
    Channel(Sender<Vec<u8>>),
    /// Writes to the standard output
    Stdout,
    /// Writes to the standard error
    Stderr,
    /// Writes to a `File` in `String` path
    File(File, String),
}

impl Debug for Output {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        match *self {
            Output::Channel(_) => fmt.write_str("Channel"),
            Output::Stderr => fmt.write_str("Stderr"),
            Output::Stdout => fmt.write_str("Stdout"),
            Output::File(_, ref filename) => fmt.write_fmt(format_args!("File: {}", filename)),
        }
    }
}

impl Write for Output {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        match *self {
            Output::Channel(ref v) => {
                v.send(Vec::from_iter(data.iter().cloned())).unwrap();
                Ok(data.len())
            }
            Output::Stderr => stderr().write(data),
            Output::Stdout => stdout().write(data),
            Output::File(ref mut v, _) => v.write(data),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            Output::Channel(_) => Ok(()),
            Output::Stderr => stderr().flush(),
            Output::Stdout => stdout().flush(),
            Output::File(ref mut v, _) => v.flush(),
        }
    }
}

impl Clone for Output {
    fn clone(&self) -> Self {
        match *self {
            Output::Channel(ref v) => Output::Channel(v.clone()),
            Output::Stderr => Output::Stderr,
            Output::Stdout => Output::Stdout,
            Output::File(_, ref path) => Output::File(
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(path)
                    .unwrap(),
                path.clone(),
            ),
        }
    }
}

/// A level that identifies a log message.
/// A lower level includes all higher levels.
#[derive(PartialEq, Clone, Debug)]
pub enum Level {
    Debug,
    Verbose,
    Notice,
    Warning,
}

impl Level {
    /// Whether the level is equal or lower than another level.
    /// For example, `Debug` includes all other levels, while `Warning` only
    /// includes itself.
    ///
    /// # Examples
    ///
    /// ```
    /// # use logger::Level;
    /// #
    /// assert!(Level::Debug.contains(&Level::Debug));
    /// assert!(!Level::Warning.contains(&Level::Debug));
    /// assert!(Level::Debug.contains(&Level::Warning));
    /// ```
    pub fn contains(&self, other: &Level) -> bool {
        match *self {
            Level::Debug => true,
            Level::Verbose => *other != Level::Debug,
            Level::Notice => *other == Level::Notice || *other == Level::Warning,
            Level::Warning => *other == Level::Warning,
        }
    }
}

#[derive(Clone)]
#[cfg(unix)]
pub struct Logger {
    // this might be ugly, but here it goes...
    /// To change the Output target, send `(Some(Output), None, None, None)`
    /// To change the Level target, send `(None, Some(Level), None, None)`
    /// To log a message send `(None, Some(Level), Some(String), None)` where the
    /// level is the message level
    /// If the last parameter is not none, it will exit the process with that exit code
    tx: Sender<(
        Option<Output>,
        Option<Level>,
        Option<String>,
        Option<Option<Box<syslog::Logger>>>,
        Option<i32>,
    )>,
}

#[derive(Clone)]
#[cfg(not(unix))]
pub struct Logger {
    // this might be ugly, but here it goes...
    /// To change the Output target, send `(Some(Output), None, None, None)`
    /// To change the Level target, send `(None, Some(Level), None, None)`
    /// To log a message send `(None, Some(Level), Some(String), None)` where the
    /// level is the message level
    tx: Sender<(
        Option<Output>,
        Option<Level>,
        Option<String>,
        Option<()>,
        Option<i32>,
    )>,
}

impl Logger {
    /// Creates a new `Logger` for a given `Output` and severity `Level`.
    #[cfg(unix)]
    fn create(level: Level, output: Output) -> Logger {
        let (tx, rx) = channel::<(
            Option<Output>,
            Option<Level>,
            Option<String>,
            Option<Option<Box<syslog::Logger>>>,
            Option<i32>,
        )>();
        {
            let mut level = level;
            let mut output = output;
            let mut syslog_writer: Option<Box<syslog::Logger>> = None;
            thread::spawn(move || {
                while let Ok((out, lvl, msg, syslog, code)) = rx.recv() {
                    match (out, lvl, msg, syslog) {
                        (_, Some(lvl), Some(msg), _) => {
                            if level.contains(&lvl) {
                                match write!(output, "{}", format!("{}\n", msg)) {
                                    Ok(_) => (),
                                    Err(e) => {
                                        // failing to log a message... will write straight to stderr
                                        // if we cannot do that, we'll panic
                                        eprint!("Failed to log {:?} {}", e, msg);
                                    }
                                };
                                if let Some(ref mut w) = syslog_writer {
                                    match w.send_3164(
                                        match lvl {
                                            Level::Debug => syslog::Severity::LOG_DEBUG,
                                            Level::Verbose => syslog::Severity::LOG_INFO,
                                            Level::Notice => syslog::Severity::LOG_NOTICE,
                                            Level::Warning => syslog::Severity::LOG_WARNING,
                                        },
                                        msg.clone(),
                                    ) {
                                        Ok(_) => (),
                                        Err(e) => {
                                            // failing to log a message... will write straight to stderr
                                            // if we cannot do that, we'll panic
                                            eprint!("Failed to log {:?} {}", e, msg);
                                        }
                                    }
                                }
                            }
                        }
                        (_, Some(lvl), _, _) => {
                            level = lvl;
                        }
                        (Some(out), _, _, _) => {
                            output = out;
                        }
                        (_, _, _, Some(syslog)) => {
                            syslog_writer = syslog;
                        }
                        (out, lvl, msg, syslog) => {
                            panic!("Unknown message {:?}", (out, lvl, msg, syslog.is_some()));
                        }
                    }

                    if let Some(code) = code {
                        process::exit(code);
                    }
                }
            });
        }

        Logger { tx }
    }

    #[cfg(not(unix))]
    fn create(level: Level, output: Output) -> Logger {
        let (tx, rx) = channel::<(
            Option<Output>,
            Option<Level>,
            Option<String>,
            Option<()>,
            Option<i32>,
        )>();
        {
            let mut level = level;
            let mut output = output;
            thread::spawn(move || {
                loop {
                    let (_output, _level, _msg, _, _code) = match rx.recv() {
                        Ok(m) => m,
                        Err(_) => break,
                    };
                    if _msg.is_some() {
                        let lvl = _level.unwrap();
                        if level.contains(&lvl) {
                            let msg = _msg.unwrap();
                            match write!(output, "{}", format!("{}\n", msg)) {
                                Ok(_) => (),
                                Err(e) => {
                                    // failing to log a message... will write straight to stderr
                                    // if we cannot do that, we'll panic
                                    write!(stderr(), "Failed to log {:?} {}", e, msg).unwrap();
                                }
                            };
                        }
                    } else if _level.is_some() {
                        level = _level.unwrap();
                    } else if _output.is_some() {
                        output = _output.unwrap();
                    } else {
                        panic!("Unknown message {:?}", (_output, _level, _msg));
                    }
                    if let Some(code) = _code {
                        process::exit(code);
                    }
                }
            });
        }

        Logger { tx: tx }
    }

    /// Creates a new logger that writes in the standard output.
    ///
    /// # Examples
    /// ```
    /// # use logger::{Logger, Level};
    /// #
    /// let logger = Logger::new(Level::Warning);
    /// logger.log(Level::Warning, "hello world".to_owned(), None);
    /// ```
    pub fn new(level: Level) -> Self {
        Self::create(level, Output::Stdout)
    }

    /// Creates a new logger that writes in the standard error.
    ///
    /// # Examples
    /// ```
    /// # use logger::{Logger, Level};
    /// #
    /// let logger = Logger::new_err(Level::Warning);
    /// logger.log(Level::Warning, "hello world".to_owned(), None);
    /// ```
    pub fn new_err(level: Level) -> Self {
        Self::create(level, Output::Stderr)
    }

    /// Creates a new logger that sends log messages to `s`.
    ///
    /// # Examples
    /// ```
    /// # use logger::{Logger, Level};
    /// # use std::sync::mpsc::channel;
    /// #
    /// let (tx, rx) = channel();
    /// let logger = Logger::channel(Level::Debug, tx);
    /// logger.log(Level::Debug, "hello world".to_owned(), None);
    /// assert_eq!(rx.recv().unwrap(), b"hello world\n".to_vec());
    /// ```
    pub fn channel(level: Level, s: Sender<Vec<u8>>) -> Self {
        Self::create(level, Output::Channel(s))
    }

    /// Creates a new logger that writes in a file.
    pub fn file(level: Level, path: &str) -> io::Result<Self> {
        Ok(Self::create(
            level,
            Output::File(File::create(Path::new(path))?, path.to_owned()),
        ))
    }

    /// Disables syslog
    #[cfg(unix)]
    pub fn disable_syslog(&mut self) {
        self.tx.send((None, None, None, Some(None), None)).unwrap();
    }

    #[cfg(not(unix))]
    pub fn disable_syslog(&mut self) {}

    /// Enables syslog.
    #[cfg(unix)]
    pub fn set_syslog(&mut self, ident: &str, facility: &str) {
        let mut w = syslog::unix(match &*facility.to_ascii_lowercase() {
            "local0" => syslog::Facility::LOG_LOCAL0,
            "local1" => syslog::Facility::LOG_LOCAL1,
            "local2" => syslog::Facility::LOG_LOCAL2,
            "local3" => syslog::Facility::LOG_LOCAL3,
            "local4" => syslog::Facility::LOG_LOCAL4,
            "local5" => syslog::Facility::LOG_LOCAL5,
            "local6" => syslog::Facility::LOG_LOCAL6,
            "local7" => syslog::Facility::LOG_LOCAL7,
            _ => syslog::Facility::LOG_USER,
        })
        .unwrap();
        w.set_process_name(ident.to_owned());
        self.tx
            .send((None, None, None, Some(Some(w)), None))
            .unwrap();
    }

    #[cfg(not(unix))]
    pub fn set_syslog(&mut self, _: &str, _: &str) {}

    /// Changes the output to be a file in `path`.
    pub fn set_logfile(&mut self, path: &str) -> io::Result<()> {
        let file = Output::File(File::create(Path::new(path))?, path.to_owned());
        self.tx.send((Some(file), None, None, None, None)).unwrap();
        Ok(())
    }

    /// Changes the log level.
    pub fn set_loglevel(&mut self, level: Level) {
        self.tx.send((None, Some(level), None, None, None)).unwrap();
    }

    /// Creates a new sender to log messages.
    pub fn sender(&self) -> Sender<(Level, String)> {
        let (tx, rx) = channel();
        let tx2 = self.tx.clone();
        thread::spawn(move || {
            while let Ok((level, message)) = rx.recv() {
                match tx2.send((None, Some(level), Some(message), None, None)) {
                    Ok(_) => (),
                    Err(_) => break,
                };
            }
        });
        tx
    }

    /// Logs a message with a log level.
    pub fn log(&self, level: Level, msg: String, code: Option<i32>) {
        self.tx
            .send((None, Some(level), Some(msg), None, code))
            .unwrap();
    }
}

unsafe impl Sync for Logger {}

#[cfg(test)]
mod test_log {
    use super::{Level, Logger};
    use std::sync::mpsc::{channel, TryRecvError};

    #[test]
    fn log_levels() {
        assert!(Level::Debug.contains(&Level::Debug));
        assert!(Level::Debug.contains(&Level::Verbose));
        assert!(Level::Debug.contains(&Level::Notice));
        assert!(Level::Debug.contains(&Level::Warning));

        assert!(!Level::Verbose.contains(&Level::Debug));
        assert!(Level::Verbose.contains(&Level::Verbose));
        assert!(Level::Verbose.contains(&Level::Notice));
        assert!(Level::Verbose.contains(&Level::Warning));

        assert!(!Level::Notice.contains(&Level::Debug));
        assert!(!Level::Notice.contains(&Level::Verbose));
        assert!(Level::Notice.contains(&Level::Notice));
        assert!(Level::Notice.contains(&Level::Warning));

        assert!(!Level::Warning.contains(&Level::Debug));
        assert!(!Level::Warning.contains(&Level::Verbose));
        assert!(!Level::Warning.contains(&Level::Notice));
        assert!(Level::Warning.contains(&Level::Warning));
    }

    #[test]
    fn log_something() {
        let (tx, rx) = channel();
        let logger = Logger::channel(Level::Debug, tx);
        logger.log(Level::Debug, "hello world".to_owned(), None);
        assert_eq!(rx.recv().unwrap(), b"hello world\n");
    }

    #[test]
    fn dont_log_something() {
        let (tx, rx) = channel();
        let logger = Logger::channel(Level::Warning, tx);
        logger.log(Level::Debug, "hello world".to_owned(), None);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
    }

    #[test]
    fn test_macro() {
        let (tx, rx) = channel();
        let logger = Logger::channel(Level::Debug, tx);
        log!(logger, Debug, "hello {}", "world");
        assert_eq!(rx.recv().unwrap(), b"hello world\n");
    }

    #[test]
    fn test_sender() {
        let (tx, rx) = channel();
        let logger = Logger::channel(Level::Debug, tx);
        let sender = logger.sender();
        sender
            .send((Level::Debug, "hello world".to_owned()))
            .unwrap();
        assert_eq!(rx.recv().unwrap(), b"hello world\n");
    }
}
