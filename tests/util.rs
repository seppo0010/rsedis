extern crate rsedis;

use rsedis::util::glob_match;

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
