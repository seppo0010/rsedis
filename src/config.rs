use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;

pub struct Config {
    pub bind: Vec<String>,
    pub port: u16,
}

impl Config {
    pub fn mock(port: u16) -> Config {
        Config {
            bind: vec!["127.0.0.1".to_owned()],
            port: port,
        }
    }

    pub fn new(confpath: Option<String>) -> Config {
        let mut bind = Vec::new();
        let mut port = 6379;
        match confpath {
            Some(fname) => {
                let path = Path::new(&*fname);
                let file = BufReader::new(File::open(&path).unwrap());
                for line_iter in file.lines() {
                    let lline = line_iter.unwrap();
                    let line = lline.trim();
                    if line.len() > 0 && &line[0..0] == "#" {
                        continue;
                    }
                    if &line[0..4] == "bind" {
                        bind.extend(line[4..].split(' ').filter(|x| x.trim().len() > 0).map(|x| x.trim().to_owned()));
                    }
                    else if &line[0..4] == "port" {
                        port = line[4..].trim().parse::<u16>().unwrap();
                    }
                }
                if bind.len() == 0 {
                    bind.push("0.0.0.0".to_owned());
                }
            },
            None => bind.push("127.0.0.1".to_owned()),
        };
        Config {
            bind: bind,
            port: port,
        }
    }

    pub fn addresses(&self) -> Vec<(&str, u16)> {
        self.bind.iter().map(|s| (&s[..], self.port)).collect::<Vec<_>>()
    }
}
