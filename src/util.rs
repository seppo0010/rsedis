use std::ascii::AsciiExt;
use std::cmp::Ord;
use std::cmp::Ordering;

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

/**
 * Float is a wrapper around f64 to implement ordering and equality.
 * f64 does not implement those traits because comparing floats has problems
 * but in the context of rsedis this basic implementation should be enough.
 **/
#[derive(Debug, Clone)]
pub struct Float {
    f: f64,
}

impl Float {
    pub fn new(f: f64) -> Float {
        Float {f: f}
    }

    pub fn get(&self) -> &f64 {
        &self.f
    }

    pub fn get_mut(&mut self) -> &mut f64 {
        &mut self.f
    }
}

impl Eq for Float {}

impl PartialEq for Float {
    fn eq(&self, other: &Self) -> bool {
        self.f == other.f
    }
}

impl Ord for Float {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialOrd for Float {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(if self.f < other.f { Ordering::Less }
        else if self.f > other.f { Ordering::Greater }
        else { Ordering::Equal })
    }

    fn lt(&self, other: &Self) -> bool { self.f < other.f }
    fn le(&self, other: &Self) -> bool { self.f <= other.f }
    fn gt(&self, other: &Self) -> bool { self.f > other.f }
    fn ge(&self, other: &Self) -> bool { self.f >= other.f }
}
