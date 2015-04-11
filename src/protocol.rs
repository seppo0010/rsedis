use std::str::from_utf8;

pub struct Argument {
    pub pos: usize,
    pub len: usize,
}

pub struct Parser<'a> {
    pub data: &'a[u8],
    pub argc: usize,
    pub argv: Vec<Argument>
}

impl<'a> Parser<'a> {
    pub fn new(data: &[u8], argc: usize, argv: Vec<Argument>) -> Parser {
        return Parser {
            data: data,
            argc: argc,
            argv: argv,
        };
    }

    pub fn get_str(&self, pos: usize) -> Result<&str, i32> {
        if pos >= self.argc {
            return Err(0);
        }
        let arg = &self.argv[pos];
        let res = from_utf8(&self.data[arg.pos..arg.pos+arg.len]);
        if res.is_err() {
            return Err(1);
        }
        return Ok(res.unwrap());
    }
}

pub fn parse_int(input: &[u8], len: usize) -> Result<(usize, usize), i32> {
    let mut i = 0;
    let mut argc = 0;
    while input[i] as char != '\r' {
        let c = input[i] as char;
        if c < '0' || c > '9' {
            return Err(0);
        }
        argc *= 10;
        argc += input[i] as usize - '0' as usize;
        i += 1;
        if i == len {
            // insufficiente data
            return Err(1);
        }
    }
    i += 1;
    if input[i] as char != '\n' {
        return Err(0);
    }
    return Ok((argc, i + 1));
}

pub fn parse(input: &[u8], len: usize) -> Result<Parser, i32> {
    if input[0] as char != '*' {
        return Err(0);
    } else {
        let mut pos = 1;
        let (argc, intlen) = try!(parse_int(&input[pos..len], len - pos));
        pos += intlen;
        let mut argv = Vec::new();
        for i in 0..argc {
            if input[pos] as char != '$' {
                return Err(0);
            }
            pos += 1;
            let (arglen, arglenlen) = try!(parse_int(&input[pos..len], len - pos));
            pos += arglenlen;
            let arg = Argument {
                pos: pos,
                len: arglen,
            };
            argv.push(arg);
            pos += arglen + 2;
            if pos > len || (pos == len && i != argc - 1) {
                return Err(1);
            }
        }
        Ok(Parser::new(input, argc, argv))
    }
}
