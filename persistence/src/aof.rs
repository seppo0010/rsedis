use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::usize;

use parser::ParsedCommand;

pub struct Aof {
    fp: File,
    dbindex: usize,
}

impl Aof {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Aof> {
        Ok(Aof {
            fp: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?,
            dbindex: usize::MAX,
        })
    }

    pub fn select(&mut self, dbindex: usize) -> io::Result<()> {
        if self.dbindex != dbindex {
            // TODO: use logarithms to know the length?
            let n = format!("{}", dbindex);
            write!(self.fp, "*2\r\n$6\r\nSELECT\r\n${}\r\n{}\r\n", n.len(), n)?;
            self.dbindex = dbindex;
        }
        Ok(())
    }

    pub fn truncate(&mut self, pos: usize) -> bool {
        if self.fp.set_len(pos as u64).is_err() {
            return false;
        }
        self.fp.seek(SeekFrom::Start(pos as u64)).is_ok()
    }

    pub fn write(&mut self, dbindex: usize, command: &ParsedCommand) -> io::Result<()> {
        self.select(dbindex)?;
        self.fp.write_all(command.get_data())?;
        Ok(())
    }
}

impl io::Read for Aof {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.fp.read(buf)
    }
}

#[cfg(test)]
mod test_aof {
    use std::env::temp_dir;
    use std::fs::File;
    use std::io::Read;
    use std::io::Write;

    use super::Aof;
    use parser::parse;

    #[test]
    fn test_write() {
        let mut path = temp_dir();
        path.push("aoftest");

        {
            let command = parse(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n").unwrap().0;

            let mut w = Aof::new(path.as_path()).unwrap();
            w.write(10, &command).unwrap()
        }
        {
            let mut data = String::with_capacity(100);
            File::open(path.as_path())
                .unwrap()
                .read_to_string(&mut data)
                .unwrap();
            assert_eq!(
                data,
                "*2\r\n$6\r\nSELECT\r\n$2\r\n10\r\n*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
            );
        }
    }

    #[test]
    fn test_read() {
        let mut path = temp_dir();
        path.push("aoftest2");
        File::create(path.as_path())
            .unwrap()
            .write(b"hello world")
            .unwrap();

        let mut r = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '!' as u8];
        let mut aof = Aof::new(path.as_path()).unwrap();
        assert_eq!(11, aof.read(&mut r).unwrap());
        assert_eq!(&r, b"hello world!");
    }
}
