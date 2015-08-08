use std::fs::File;
use std::io;
use std::io::Write;
use std::path::Path;
use std::usize;

use parser::ParsedCommand;

pub struct AofWriter {
    fp: File,
    dbindex: usize,
}

impl AofWriter {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<AofWriter> {
        Ok(AofWriter {
            fp: try!(File::create(path)),
            dbindex: usize::MAX,
        })
    }

    pub fn select(&mut self, dbindex: usize) -> io::Result<()> {
        if self.dbindex != dbindex {
            // TODO: use logarithms to know the length?
            let n = format!("{}", dbindex);
            try!(write!(self.fp, "*2\r\n$6\r\nSELECT\r\n${}\r\n{}\r\n", n.len(), n));
            self.dbindex = dbindex;
        }
        Ok(())
    }
    pub fn write(&mut self, dbindex: usize, command: &ParsedCommand) -> io::Result<()> {
        try!(self.select(dbindex));
        try!(self.fp.write(command.get_data()));
        Ok(())
    }
}

#[cfg(test)]
mod test_aof {
    use std::env::temp_dir;
    use std::fs::File;
    use std::io::Read;

    use parser::parse;
    use super::AofWriter;

    #[test]
    fn test_write() {
        let mut path = temp_dir();
        path.push("aoftest");

        {
            let command = parse(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n").unwrap().0;

            let mut w = AofWriter::new(path.as_path()).unwrap();
            w.write(10, &command).unwrap()
        }
        {
            let mut data = String::with_capacity(100);;
            File::open(path.as_path()).unwrap().read_to_string(&mut data).unwrap();
            assert_eq!(data, "*2\r\n$6\r\nSELECT\r\n$2\r\n10\r\n*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
        }
    }
}
