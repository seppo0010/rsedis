#[cfg(unix)]
extern crate libc;
#[cfg(unix)]
pub mod unix;
#[cfg(unix)]
pub mod utsname;
#[cfg(unix)]
pub use unix::*;

#[cfg(windows)]
extern crate winapi;
#[cfg(windows)]
pub mod win;
#[cfg(windows)]
pub use win::*;

#[cfg(not(any(unix, windows)))]
pub mod other;

#[test]
fn getpid_test() {
    getpid();
}

#[test]
fn getos_test() {
    getos();
}
