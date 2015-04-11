extern crate rsedis;

#[cfg(test)]
mod test {
    use rsedis::protocol::parse;

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
        // let e = r.unwrap_err();
        // assert_eq!(e, 0);
    }

    #[test]
    fn parse_invalid() {
        let message = b"-2\r\n$3\r\nfoo";
        let r = parse(message, message.len());
        assert!(r.is_err());
        // let e = r.unwrap_err();
        // assert_eq!(e, 1);
    }
}
