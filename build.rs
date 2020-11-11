use std::fs::File;
use std::io::Write;
use std::process::Command;
use std::str::from_utf8;
use std::{env, path};

fn main() {
    let path = path::Path::new(&env::var_os("OUT_DIR").unwrap()).join("release.rs");

    let mut f = File::create(path).unwrap();

    {
        let hash = match Command::new("git")
            .arg("show-ref")
            .arg("--head")
            .arg("--hash=8")
            .output()
        {
            Ok(o) => {
                if o.stdout.len() >= 8 {
                    String::from(from_utf8(&o.stdout[0..8]).unwrap())
                } else {
                    String::from("00000000")
                }
            }
            Err(_) => String::from("00000000"),
        };
        writeln!(f, "pub const GIT_SHA1: &str = \"{}\";", &hash[0..8]).unwrap();
    }

    {
        let dirty = match Command::new("git")
            .arg("diff")
            .arg("--no-ext-diff")
            .output()
        {
            Ok(o) => !o.stdout.is_empty(),
            Err(_) => true,
        };
        writeln!(
            f,
            "pub const GIT_DIRTY: bool = {};",
            if dirty { "true" } else { "false " }
        )
        .unwrap();
    }

    {
        let version = match Command::new("rustc").arg("--version").output() {
            Ok(o) => String::from(from_utf8(&o.stdout).unwrap().trim()),
            Err(_) => String::new(),
        };
        writeln!(f, "pub const RUSTC_VERSION: &str = {:?};", version).unwrap();
    }
}
