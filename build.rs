use std::fs::File;
use std::io::Write;
use std::process::Command;
use std::str::from_utf8;

fn main() {
    let mut f = File::create("src/release.rs").unwrap();

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
        write!(
            f,
            "pub const GIT_SHA1: &'static str = \"{}\";\n",
            &hash[0..8]
        )
        .unwrap();
    }

    {
        let dirty = match Command::new("git")
            .arg("diff")
            .arg("--no-ext-diff")
            .output()
        {
            Ok(o) => o.stdout.len() > 0,
            Err(_) => true,
        };
        write!(
            f,
            "pub const GIT_DIRTY: bool = {};\n",
            if dirty { "true" } else { "false " }
        )
        .unwrap();
    }

    {
        let version = match Command::new("rustc").arg("--version").output() {
            Ok(o) => String::from(from_utf8(&o.stdout).unwrap().trim()),
            Err(_) => String::new(),
        };
        write!(
            f,
            "pub const RUSTC_VERSION: &'static str = {:?};\n",
            version
        )
        .unwrap();
    }
}
