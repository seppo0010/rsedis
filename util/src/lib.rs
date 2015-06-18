extern crate time;

use std::ascii::AsciiExt;

use time::get_time;

#[must_use]
fn match_char(e1: &u8, e2: &u8, ignore_case: bool) -> bool {
    if ignore_case {
        // FIXME: redis uses tolower() which is locale aware
        return e1.to_ascii_lowercase() == e2.to_ascii_lowercase();
    } else {
        return e1 == e2;
    }
}

pub fn glob_match(pattern: &Vec<u8>, element: &Vec<u8>, ignore_case: bool) -> bool {
    let mut patternpos = 0;
    let mut elementpos = 0;
    let star = 42u8; /* '*' */
    let question_mark = 63u8; /* '?' */
    let backslash = 92u8; /* '\\' */
    let open_bracket = 91u8; /* '[' */
    let close_bracket = 93u8; /* '[' */
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
                    if glob_match(&pattern[patternpos + 1..].to_vec(), &element[i..].to_vec(), ignore_case) {
                        return true;
                    }
                }
                return false;
            },
            x if x == question_mark => {
                if elementpos >= element.len() {
                    return false;
                }
                elementpos += 1;
            },
            x if x == backslash => {
                patternpos += 1;
                if elementpos >= element.len(){
                    return false;
                }
                if !match_char(&pattern[patternpos], &element[elementpos], ignore_case) {
                    return false;
                }
                elementpos += 1;
            },
            x if x == open_bracket => {
                patternpos += 1;
                let not = pattern[patternpos] == ('^' as u8);
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
                    } else if pattern.len() >= patternpos + 3 && pattern[patternpos + 1] == ('-' as u8) {
                        let mut start = pattern[patternpos];
                        let mut end = pattern[patternpos + 2];
                        let mut c = element[elementpos];
                        if start > end {
                            let t = start;
                            start = end;
                            end = t;
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
                    } else {
                        if match_char(&pattern[patternpos], &element[elementpos], ignore_case) {
                            matched = true;
                        }
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
            },
            _ => {
                if elementpos >= element.len(){
                    return false;
                }
                if !match_char(&pattern[patternpos], &element[elementpos], ignore_case) {
                    return false;
                }
                elementpos += 1;
            },
        }
        patternpos += 1;
        if elementpos == element.len() {
            for p in patternpos..pattern.len() {
                if pattern[p] != star {
                    break;
                }
            }
            break;
        }
    }

    if patternpos == pattern.len() && elementpos == element.len() {
        return true;
    }
    return false;
}

pub fn ustime() -> i64 {
    let tv = get_time();
    tv.sec * 1000000 + tv.nsec as i64
}

pub fn mstime() -> i64 {
    ustime() / 1000
}

pub fn splitargs(args: &Vec<u8>) -> Result<Vec<Vec<u8>>, ()> {
    let mut i = 0;
    let mut result = Vec::new();
    for _ in args {
        while i < args.len() && (args[i] == 0 || (args[i] as char).is_whitespace()) { i += 1; }
        if i >=  args.len() { break; }
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
                } else if p == '"'{
                    // closing quote must be followed by a space or nothing at all
                    i += 1;
                    if i != args.len() && !(args[i] as char).is_whitespace() { return Err(()); }
                    inq = false;
                    done = true;
                } else if p == '\\' && i + 1 < args.len() {
                    i += 1;
                    let c = match args[i] as char {
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        // FIXME: Rust does not recognize '\a' and '\b'?
                        'b' => 'b',
                        'a' => 'a',
                        c => c as char,
                    } as u8;
                    current.push(c);
                } else {
                    current.push(args[i]);
                }
            } else if insq {
                if p == '\'' {
                    // closing quote must be followed by a space or nothing at all
                    i += 1;
                    if i != args.len() && !(args[i] as char).is_whitespace() { return Err(()); }
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
        if i >=  args.len() {
            if inq || insq {
                return Err(());
            }
            break;
        }
    }
    Ok(result)
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
    assert!(glob_match(&b"hello*".to_vec(), &b"hello world".to_vec(), true));
    assert!(glob_match(&b"*world".to_vec(), &b"hello world".to_vec(), true));
    assert!(!glob_match(&b"foo*".to_vec(), &b"hello world".to_vec(), true));
    assert!(!glob_match(&b"*bar".to_vec(), &b"hello world".to_vec(), true));
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
    assert_eq!(splitargs(&b"\"\\x9f\"".to_vec()).unwrap(), vec![vec![159u8]]);
    assert_eq!(splitargs(&b"\"\"".to_vec()).unwrap(), vec![vec![]]);
    assert_eq!(splitargs(&b"\"\\thello\\n\"".to_vec()).unwrap(), vec![b"\thello\n".to_vec()]);
    assert!(splitargs(&b"\"a".to_vec()).is_err());
}

#[test]
fn splitargs_singlequotes() {
    assert_eq!(splitargs(&b"\'\\x9f\'".to_vec()).unwrap(), vec![b"\\x9f".to_vec()]);
    assert_eq!(splitargs(&b"\'\'".to_vec()).unwrap(), vec![vec![]]);
    assert_eq!(splitargs(&b"\'\\\'\'".to_vec()).unwrap(), vec![b"'".to_vec()]);
    assert_eq!(splitargs(&b"\'\\thello\\n\'".to_vec()).unwrap(), vec![b"\\thello\\n".to_vec()]);
    assert!(splitargs(&b"\'a".to_vec()).is_err());
}

#[test]
fn splitargs_misc() {
    assert_eq!(splitargs(&b"hello world".to_vec()).unwrap(), vec![b"hello".to_vec(), b"world".to_vec()]);
    assert_eq!(splitargs(&b"\'hello\' world".to_vec()).unwrap(), vec![b"hello".to_vec(), b"world".to_vec()]);
    assert_eq!(splitargs(&b"\"hello\" world".to_vec()).unwrap(), vec![b"hello".to_vec(), b"world".to_vec()]);
    assert!(splitargs(&b"\"hello\"world".to_vec()).is_err());
    assert!(splitargs(&b"\'hello\'world".to_vec()).is_err());
}
