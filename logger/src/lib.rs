use std::io;
use std::io::{Write, stderr};
use std::iter::FromIterator;
use std::fs::{File, OpenOptions};
use std::fmt::{Debug, Error, Formatter};
use std::path::Path;
use std::sync::mpsc::{Sender, channel};
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
        $logger.log(Level::$level, format!($($arg)*))
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
            Output::File(_, ref filename) => fmt.write_fmt(format_args!("File: {}", filename)),
        }
    }
}

impl Write for Output {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        match *self {
            Output::Channel(ref v) => { v.send(Vec::from_iter(data.iter().cloned())).unwrap(); Ok(data.len()) },
            Output::Stderr => stderr().write(data),
            Output::File(ref mut v, _) => v.write(data),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            Output::Channel(_) => Ok(()),
            Output::Stderr => stderr().flush(),
            Output::File(ref mut v, _) => v.flush(),
        }
    }
}

impl Clone for Output {
    fn clone(&self) -> Self {
        match *self {
            Output::Channel(ref v) => Output::Channel(v.clone()),
            Output::Stderr => Output::Stderr,
            Output::File(_, ref path) => Output::File(OpenOptions::new().write(true).create(true).open(path).unwrap(), path.clone()),
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
    /// assert!(Level::Debug.contains(Level::Debug));
    /// assert!(!Level::Warning.contains(Level::Debug));
    /// assert!(Level::Debug.contains(Level::Warning));
    /// ```
    pub fn contains(&self, other: Level) -> bool {
        match *self {
            Level::Debug => true,
            Level::Verbose => other != Level::Debug,
            Level::Notice => other == Level::Notice || other == Level::Warning,
            Level::Warning => other == Level::Warning,
        }
    }
}

#[derive(Clone)]
pub struct Logger {
    // this might be ugly, but here it goes...
    /// To change the Output target, send `(Some(Output), None, None)`
    /// To change the Level target, send `(None, Some(Level), None)`
    /// To log a message send `(None, Some(Level), Some(String))` where the
    /// level is the message level
    tx: Sender<(Option<Output>, Option<Level>, Option<String>)>,
}

impl Logger {
    /// Creates a new `Logger` for a given `Output` and severity `Level`.
    fn create(level: Level, output: Output) -> Logger {
        let (tx, rx) = channel::<(Option<Output>, Option<Level>, Option<String>)>();
        {
            let mut level = level;
            let mut output = output;
            thread::spawn(move || {
                loop {
                    let (_output, _level, _msg) = match rx.recv() {
                        Ok(m) => m,
                        Err(_) => break,
                    };
                    if _msg.is_some() {
                        if level.contains(_level.unwrap()) {
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
                };
            });
        }

        Logger {
            tx: tx,
        }
    }

    /// Creates a new logger that writes in the standard output.
    ///
    /// # Examples
    /// ```
    /// # use logger::{Logger, Level};
    /// #
    /// let logger = Logger::new(Level::Warning);
    /// logger.log(Level::Warning, "hello world".to_owned());
    /// ```
    pub fn new(level: Level) -> Self {
        Self::create(level, Output::Stderr)
    }

    /// Creates a new logger that writes in the standard output.
    ///
    /// # Examples
    /// ```
    /// # use logger::{Logger, Level};
    /// # use std::sync::mpsc::channel;
    /// #
    /// let (tx, rx) = channel();
    /// let logger = Logger::channel(Level::Debug, tx);
    /// logger.log(Level::Debug, "hello world".to_owned());
    /// assert_eq!(rx.recv().unwrap(), b"hello world\n".to_vec());
    /// ```
    pub fn channel(level: Level, s: Sender<Vec<u8>>) -> Self {
        Self::create(level, Output::Channel(s))
    }

    /// Creates a new logger that writes in a file.
    pub fn file(level: Level, path: &str) -> io::Result<Self> {
        Ok(Self::create(level, Output::File(try!(File::create(Path::new(path))), path.to_owned())))
    }

    /// Changes the output to be a file in `path`.
    pub fn set_logfile(&mut self, path: &str) -> io::Result<()> {
        let file = Output::File(try!(File::create(Path::new(path))), path.to_owned());
        self.tx.send((Some(file), None, None)).unwrap();
        Ok(())
    }

    /// Changes the log level.
    pub fn set_loglevel(&mut self, level: Level) {
        self.tx.send((None, Some(level), None)).unwrap();
    }

    /// Creates a new sender to log messages.
    pub fn sender(&self) -> Sender<(Level, String)> {
        let (tx, rx) = channel();
        let tx2 = self.tx.clone();
        thread::spawn(move || {
            loop {
                let (level, message) = match rx.recv() {
                    Ok(msg) => msg,
                    Err(_) => break,
                };
                match tx2.send((None, Some(level), Some(message))) {
                    Ok(_) => (),
                    Err(_) => break,
                };
            }
        });
        tx
    }

    /// Logs a message with a log level.
    pub fn log(&self, level: Level, msg: String) {
        self.tx.send((None, Some(level), Some(msg))).unwrap();
    }
}

unsafe impl Sync for Logger {}

#[cfg(test)]
mod test_log {
    use super::{Logger, Level};
    use std::sync::mpsc::{TryRecvError, channel};

    #[test]
    fn log_levels() {
        assert!(Level::Debug.contains(Level::Debug));
        assert!(Level::Debug.contains(Level::Verbose));
        assert!(Level::Debug.contains(Level::Notice));
        assert!(Level::Debug.contains(Level::Warning));

        assert!(!Level::Verbose.contains(Level::Debug));
        assert!(Level::Verbose.contains(Level::Verbose));
        assert!(Level::Verbose.contains(Level::Notice));
        assert!(Level::Verbose.contains(Level::Warning));

        assert!(!Level::Notice.contains(Level::Debug));
        assert!(!Level::Notice.contains(Level::Verbose));
        assert!(Level::Notice.contains(Level::Notice));
        assert!(Level::Notice.contains(Level::Warning));

        assert!(!Level::Warning.contains(Level::Debug));
        assert!(!Level::Warning.contains(Level::Verbose));
        assert!(!Level::Warning.contains(Level::Notice));
        assert!(Level::Warning.contains(Level::Warning));
    }

    #[test]
    fn log_something() {
        let (tx, rx) = channel();
        let logger = Logger::channel(Level::Debug, tx);
        logger.log(Level::Debug, "hello world".to_owned());
        assert_eq!(rx.recv().unwrap(), b"hello world\n");
    }

    #[test]
    fn dont_log_something() {
        let (tx, rx) = channel();
        let logger = Logger::channel(Level::Warning, tx);
        logger.log(Level::Debug, "hello world".to_owned());
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
        sender.send((Level::Debug, "hello world".to_owned())).unwrap();
        assert_eq!(rx.recv().unwrap(), b"hello world\n");
    }
}
