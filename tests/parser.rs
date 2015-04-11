extern crate rsedis;

#[cfg(test)]
mod test {
    use rsedis::parser::parse;
    use rsedis::parser::ParseError;

    #[test]
    fn parse_valid() {
        let message = b"*2\r\n$3\r\nfoo\r\n$4\r\nbarz\r\n";
        let r = parse(message, message.len());
        assert!(r.is_ok());
        let parser = r.unwrap();
        assert_eq!(parser.argc, 2);
        assert_eq!(parser.get_str(0).unwrap(), "foo");
        assert_eq!(parser.get_str(1).unwrap(), "barz");
    }

    #[test]
    fn parse_incomplete() {
        let message = b"*2\r\n$3\r\nfoo";
        let r = parse(message, message.len());
        assert!(r.is_err());
        match r.unwrap_err() {
            ParseError::Incomplete => {},
            _ => assert!(false)
        }
    }

    #[test]
    fn parse_invalid() {
        let message = b"-2\r\n$3\r\nfoo";
        let r = parse(message, message.len());
        assert!(r.is_err());
        match r.unwrap_err() {
            ParseError::BadProtocol => {},
            _ => assert!(false)
        }
    }
}
