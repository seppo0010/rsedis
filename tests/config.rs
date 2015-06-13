extern crate rsedis;

use std::fs::create_dir;
use std::fs::File;
use std::io::Write;

use rand::random;

use rsedis::config::Config;
use rsedis::util::mstime;

macro_rules! config {
    ($str: expr) => ({
        let dirpath = format!("tmp/{}", mstime());
        let filepath = format!("{}/{}.conf", dirpath, random::<u64>());
        match create_dir("tmp") { _ => () }
        match create_dir(dirpath) { _ => () }
        match File::create(filepath.clone()).unwrap().write_all($str) { _ => () }
        Config::new(Some(filepath)).unwrap()
    })
}

#[test]
fn parse_bind() {
    let config = config!(b"bind 1.2.3.4\nbind 5.6.7.8");
    assert_eq!(config.bind, vec!["1.2.3.4", "5.6.7.8"]);
    assert_eq!(config.port, 6379);
}

#[test]
fn parse_port() {
    let config = config!(b"port 12345");
    assert_eq!(config.bind, vec!["127.0.0.1"]);
    assert_eq!(config.port, 12345);
}

#[test]
fn parse_bind_port() {
    let config = config!(b"bind 127.0.0.1\nport 12345");
    assert_eq!(config.bind, vec!["127.0.0.1"]);
    assert_eq!(config.port, 12345);
}

#[test]
fn parse_daemonize_yes() {
    let config = config!(b"daemonize yes");
    assert!(config.daemonize);
}

#[test]
fn parse_daemonize_no() {
    let config = config!(b"daemonize no");
    assert!(!config.daemonize);
}
