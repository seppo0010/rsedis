extern crate libc;
extern crate rand;
extern crate time;

use std::fmt;

use libc::types::os::arch::c95::c_int;
use rand::{thread_rng, Rng};
use time::get_time;

/// Are two chars the same? Optionally ignoring the case.
///
/// # Examples
///
/// ```
/// use util::match_char;
/// assert!( match_char(&('a' as u8), &('a' as u8), false));
/// assert!( match_char(&('a' as u8), &('a' as u8), true));
/// assert!(!match_char(&('a' as u8), &('A' as u8), false));
/// assert!( match_char(&('a' as u8), &('A' as u8), true));
/// ```
#[must_use]
pub fn match_char(e1: &u8, e2: &u8, ignore_case: bool) -> bool {
    if ignore_case {
        // FIXME: redis uses tolower() which is locale aware
        e1.to_ascii_lowercase() == e2.to_ascii_lowercase()
    } else {
        e1 == e2
    }
}

/// Whether an element matches a glob-like pattern. Optionally ignores case.
///
/// # Examples
///
/// ```
/// use util::glob_match;
/// assert!(glob_match(&b"".to_vec(), &b"".to_vec(), false));
/// assert!(glob_match(&b"foo*baz".to_vec(), &b"foobarbaz".to_vec(), false));
/// assert!(!glob_match(&b"foo*baz".to_vec(), &b"foobazbar".to_vec(), false));
/// assert!(glob_match(&b"fooba?".to_vec(), &b"foobar".to_vec(), false));
/// assert!(glob_match(&b"fooba?".to_vec(), &b"foobaz".to_vec(), false));
/// assert!(!glob_match(&b"fooba?".to_vec(), &b"foofoo".to_vec(), false));
/// ```
#[must_use]
pub fn glob_match(pattern: &[u8], element: &[u8], ignore_case: bool) -> bool {
    let mut patternpos = 0;
    let mut elementpos = 0;

    let star = b'*';
    let question_mark = b'?';
    let backslash = b'\\';
    let open_bracket = b'[';
    let close_bracket = b']';

    while patternpos < pattern.len() {
        match pattern[patternpos] {
            x if x == star => {
                while patternpos + 1 < pattern.len() && pattern[patternpos + 1] == star {
                    patternpos += 1;
                }
                if patternpos == pattern.len() {
                    return true;
                }
                for i in elementpos..(element.len() + 1) {
                    if glob_match(
                        &pattern[patternpos + 1..].to_vec(),
                        &element[i..].to_vec(),
                        ignore_case,
                    ) {
                        return true;
                    }
                }
                return false;
            }
            x if x == question_mark => {
                if elementpos >= element.len() {
                    return false;
                }
                elementpos += 1;
            }
            x if x == backslash => {
                patternpos += 1;
                if elementpos >= element.len() {
                    return false;
                }
                if !match_char(&pattern[patternpos], &element[elementpos], ignore_case) {
                    return false;
                }
                elementpos += 1;
            }
            x if x == open_bracket => {
                patternpos += 1;
                let not = pattern[patternpos] == b'^';
                if not {
                    patternpos += 1;
                }
                let mut matched = false;
                loop {
                    if pattern[patternpos] == backslash {
                        patternpos += 1;
                        if pattern[patternpos] == element[elementpos] {
                            matched = true;
                        }
                    } else if pattern[patternpos] == close_bracket {
                        break;
                    } else if patternpos >= pattern.len() {
                        patternpos += 1;
                        break;
                    } else if pattern.len() >= patternpos + 3 && pattern[patternpos + 1] == b'-' {
                        let mut start = pattern[patternpos];
                        let mut end = pattern[patternpos + 2];
                        let mut c = element[elementpos];
                        if start > end {
                            std::mem::swap(&mut start, &mut end);
                        }
                        if ignore_case {
                            start = start.to_ascii_lowercase();
                            end = end.to_ascii_lowercase();
                            c = c.to_ascii_lowercase();
                        }
                        patternpos += 2;
                        if c >= start && c <= end {
                            matched = true;
                        }
                    } else if match_char(&pattern[patternpos], &element[elementpos], ignore_case) {
                        matched = true;
                    }

                    patternpos += 1;
                }
                if not {
                    matched = !matched;
                }
                if !matched {
                    return false;
                }
                elementpos += 1;
            }
            _ => {
                if elementpos >= element.len() {
                    return false;
                }
                if !match_char(&pattern[patternpos], &element[elementpos], ignore_case) {
                    return false;
                }
                elementpos += 1;
            }
        }
        patternpos += 1;
        if elementpos == element.len() {
            for i in &pattern[patternpos..pattern.len()] {
                if *i != star {
                    break;
                }
            }
            break;
        }
    }

    patternpos == pattern.len() && elementpos == element.len()
}

/// Current timestamp in microseconds
pub fn ustime() -> i64 {
    let tv = get_time();
    tv.sec * 1000000 + (tv.nsec / 1000) as i64
}

/// Current timestamp in milliseconds
pub fn mstime() -> i64 {
    ustime() / 1000
}

/// Parses a config line string.
///
/// # Examples
/// ```
/// # use util::splitargs;
/// #
/// let res = splitargs(b"hello world").unwrap();
/// assert_eq!(res, vec![b"hello".to_vec(), b"world".to_vec()]);
/// ```
///
/// ```
/// # use util::splitargs;
/// #
/// let res = splitargs(b"foo \"bar baz\"").unwrap();
/// assert_eq!(res, vec![b"foo".to_vec(), b"bar baz".to_vec()]);
/// ```
pub fn splitargs(args: &[u8]) -> Result<Vec<Vec<u8>>, ()> {
    let mut i = 0;
    let mut result = Vec::new();
    for _ in args {
        while i < args.len() && (args[i] == 0 || (args[i] as char).is_whitespace()) {
            i += 1;
        }
        if i >= args.len() {
            break;
        }
        let mut inq = false;
        let mut insq = false;
        let mut done = false;
        let mut current = Vec::new();
        while !done && i < args.len() {
            let p = args[i] as char;
            if inq {
                if p == '\\' && (args[i + 1] as char) == 'x' {
                    if let Some(c1) = (args[i + 2] as char).to_digit(16) {
                        if let Some(c2) = (args[i + 3] as char).to_digit(16) {
                            current.push((c1 * 16 + c2) as u8);
                            i += 3;
                        }
                    }
                } else if p == '"' {
                    // closing quote must be followed by a space or nothing at all
                    i += 1;
                    if i != args.len() && !(args[i] as char).is_whitespace() {
                        return Err(());
                    }
                    inq = false;
                    done = true;
                } else if p == '\\' && i + 1 < args.len() {
                    i += 1;
                    let c = match args[i] as char {
                        'n' => b'\n',
                        'r' => b'\r',
                        't' => b'\t',
                        // FIXME: Rust does not recognize '\a' and '\b'?
                        'b' => b'b',
                        'a' => b'a',
                        c => c as u8,
                    };
                    current.push(c);
                } else {
                    current.push(args[i]);
                }
            } else if insq {
                if p == '\'' {
                    // closing quote must be followed by a space or nothing at all
                    i += 1;
                    if i != args.len() && !(args[i] as char).is_whitespace() {
                        return Err(());
                    }
                    insq = false;
                    done = true;
                } else if p == '\\' && i + 1 < args.len() && (args[i + 1] as char == '\'') {
                    current.push(args[i + 1]);
                    i += 1;
                } else {
                    current.push(args[i]);
                }
            } else {
                match p as char {
                    ' ' => done = true,
                    '\t' => done = true,
                    '\n' => done = true,
                    '\r' => done = true,
                    '\0' => done = true,
                    '"' => inq = true,
                    '\'' => insq = true,
                    _ => current.push(args[i]),
                }
            }
            i += 1;
        }
        result.push(current);
        if i >= args.len() {
            if inq || insq {
                return Err(());
            }
            break;
        }
    }
    Ok(result)
}

/// Creates an array of four `u8` from a `u32`.
pub fn htonl(v: u32) -> [u8; 4] {
    // maybe it should use C api instead?
    [
        ((v >> 24) & 0xFF) as u8,
        ((v >> 16) & 0xFF) as u8,
        ((v >> 8) & 0xFF) as u8,
        (v & 0xFF) as u8,
    ]
}

fn is_print(c: char) -> bool {
    unsafe { libc::funcs::c95::ctype::isprint(c as c_int) != 0 }
}
pub fn format_repr(f: &mut fmt::Formatter, s: &[u8]) -> Result<(), fmt::Error> {
    f.write_str("\"")?;
    for c in s {
        match *c {
            0x07 => {
                f.write_str("\\a")?;
                continue;
            }
            0x08 => {
                f.write_str("\\b")?;
                continue;
            }
            _ => (),
        };
        match *c as char {
            '\\' => f.write_str("\\\\"),
            '\"' => f.write_str("\\\""),
            '\n' => f.write_str("\\n"),
            '\r' => f.write_str("\\r"),
            '\t' => f.write_str("\\t"),
            x => {
                if is_print(x) {
                    write!(f, "{}", x)
                } else {
                    write!(f, "\\x{:02x}", x as u8)
                }
            }
        }?
    }
    f.write_str("\"")
}

/// Transform a number from 0 to 15 to an heximal character
fn numtohex(num: u8) -> char {
    assert!(num < 16);
    b"0123456789abcdef"[num as usize] as char
}

/// Generates a random [0-9A-F]{len} string.
///
/// # Examples
/// ```
/// # use util::get_random_hex_chars;
/// #
/// let s = get_random_hex_chars(31);
/// assert_eq!(s.len(), 31);
/// ```
pub fn get_random_hex_chars(len: usize) -> String {
    let binarylen = (len + 1) / 2;
    let mut v = Vec::with_capacity(binarylen);
    unsafe {
        v.set_len(binarylen);
    }
    thread_rng().fill_bytes(&mut *v);
    let mut s = String::with_capacity(binarylen * 2);
    for c in v {
        s.push(numtohex(c % 16));
        s.push(numtohex(c / 16));
    }
    if binarylen * 2 != len {
        s.pop();
    }
    s
}

#[cfg(test)]
mod test_util {
    use std::thread::sleep;
    use std::{u32, u8};

    use super::{glob_match, htonl, mstime, splitargs};
    use std::time::Duration;

    #[test]
    fn mstime_sleep() {
        let start = mstime();
        sleep(Duration::from_millis(100));
        let end = mstime();
        assert!(start < end && start + 100 <= end && start + 500 > end);
    }

    #[test]
    fn glob_match_empty() {
        assert!(glob_match(&b"".to_vec(), &b"".to_vec(), true));
    }

    #[test]
    fn glob_match_star() {
        assert!(glob_match(&b"*".to_vec(), &b"".to_vec(), true));
        assert!(glob_match(&b"*".to_vec(), &b"hello world".to_vec(), true));
        assert!(glob_match(&b"**".to_vec(), &b"hello world".to_vec(), true));
        assert!(glob_match(
            &b"hello*".to_vec(),
            &b"hello world".to_vec(),
            true
        ));
        assert!(glob_match(
            &b"*world".to_vec(),
            &b"hello world".to_vec(),
            true
        ));
        assert!(!glob_match(
            &b"foo*".to_vec(),
            &b"hello world".to_vec(),
            true
        ));
        assert!(!glob_match(
            &b"*bar".to_vec(),
            &b"hello world".to_vec(),
            true
        ));
        assert!(!glob_match(&b"*bar".to_vec(), &b"".to_vec(), true));
    }

    #[test]
    fn glob_match_question_mark() {
        assert!(!glob_match(&b"?".to_vec(), &b"".to_vec(), true));
        assert!(glob_match(&b"?".to_vec(), &b"a".to_vec(), true));
        assert!(!glob_match(&b"?".to_vec(), &b"aa".to_vec(), true));
        assert!(glob_match(&b"a?".to_vec(), &b"aa".to_vec(), true));
    }

    #[test]
    fn glob_match_backslash() {
        assert!(glob_match(&b"\\*asd".to_vec(), &b"*asd".to_vec(), true));
        assert!(!glob_match(&b"\\*d".to_vec(), &b"*asd".to_vec(), true));
        assert!(glob_match(&b"\\?a".to_vec(), &b"?a".to_vec(), true));
        assert!(!glob_match(&b"\\?a".to_vec(), &b"ba".to_vec(), true));
    }

    #[test]
    fn glob_match_brackets() {
        assert!(glob_match(&b"[abc]".to_vec(), &b"a".to_vec(), true));
        assert!(!glob_match(&b"[^abc]".to_vec(), &b"a".to_vec(), true));
        assert!(glob_match(&b"[abc]b".to_vec(), &b"ab".to_vec(), true));
        assert!(!glob_match(&b"[abc]".to_vec(), &b"ab".to_vec(), true));
        assert!(glob_match(&b"[\\]*]".to_vec(), &b"]".to_vec(), true));
        assert!(!glob_match(&b"[\\]*]".to_vec(), &b"a".to_vec(), true));
    }

    #[test]
    fn splitargs_quotes() {
        assert_eq!(
            splitargs(&b"\"\\x9f\"".to_vec()).unwrap(),
            vec![vec![159u8]]
        );
        assert_eq!(splitargs(&b"\"\"".to_vec()).unwrap(), vec![vec![]]);
        assert_eq!(
            splitargs(&b"\"\\thello\\n\"".to_vec()).unwrap(),
            vec![b"\thello\n".to_vec()]
        );
        assert!(splitargs(&b"\"a".to_vec()).is_err());
    }

    #[test]
    fn splitargs_singlequotes() {
        assert_eq!(
            splitargs(&b"\'\\x9f\'".to_vec()).unwrap(),
            vec![b"\\x9f".to_vec()]
        );
        assert_eq!(splitargs(&b"\'\'".to_vec()).unwrap(), vec![vec![]]);
        assert_eq!(
            splitargs(&b"\'\\\'\'".to_vec()).unwrap(),
            vec![b"'".to_vec()]
        );
        assert_eq!(
            splitargs(&b"\'\\thello\\n\'".to_vec()).unwrap(),
            vec![b"\\thello\\n".to_vec()]
        );
        assert!(splitargs(&b"\'a".to_vec()).is_err());
    }

    #[test]
    fn splitargs_misc() {
        assert_eq!(
            splitargs(&b"hello world".to_vec()).unwrap(),
            vec![b"hello".to_vec(), b"world".to_vec()]
        );
        assert_eq!(
            splitargs(&b"\'hello\' world".to_vec()).unwrap(),
            vec![b"hello".to_vec(), b"world".to_vec()]
        );
        assert_eq!(
            splitargs(&b"\"hello\" world".to_vec()).unwrap(),
            vec![b"hello".to_vec(), b"world".to_vec()]
        );
        assert!(splitargs(&b"\"hello\"world".to_vec()).is_err());
        assert!(splitargs(&b"\'hello\'world".to_vec()).is_err());
    }

    #[test]
    fn htonl_basic() {
        assert_eq!(htonl(1), [0, 0, 0, 1]);
        assert_eq!(htonl(256), [0, 0, 1, 0]);
        assert_eq!(htonl(u32::MAX), [u8::MAX, u8::MAX, u8::MAX, u8::MAX]);
    }
}
