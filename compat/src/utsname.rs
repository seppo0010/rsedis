// Taken from nix

use libc::c_char;
use std::ffi::CStr;
use std::mem;
use std::str::from_utf8_unchecked;

mod ffi {
    use super::UtsName;
    use libc::c_int;

    extern "C" {
        pub fn uname(buf: *mut UtsName) -> c_int;
    }
}

#[cfg(target_os = "linux")]
const UTSNAME_LEN: usize = 65;
#[cfg(target_os = "macos")]
const UTSNAME_LEN: usize = 256;

#[repr(C)]
#[derive(Copy)]
pub struct UtsName {
    sysname: [c_char; UTSNAME_LEN],
    nodename: [c_char; UTSNAME_LEN],
    release: [c_char; UTSNAME_LEN],
    version: [c_char; UTSNAME_LEN],
    machine: [c_char; UTSNAME_LEN],
    _domainname: [c_char; UTSNAME_LEN],
}

// workaround for `derive(Clone)` not working for fixed-length arrays
impl Clone for UtsName {
    fn clone(&self) -> UtsName {
        *self
    }
}

impl UtsName {
    pub fn sysname(&self) -> &str {
        to_str(&(&self.sysname as *const c_char) as *const *const c_char)
    }

    pub fn nodename(&self) -> &str {
        to_str(&(&self.nodename as *const c_char) as *const *const c_char)
    }

    pub fn release(&self) -> &str {
        to_str(&(&self.release as *const c_char) as *const *const c_char)
    }

    pub fn version(&self) -> &str {
        to_str(&(&self.version as *const c_char) as *const *const c_char)
    }

    pub fn machine(&self) -> &str {
        to_str(&(&self.machine as *const c_char) as *const *const c_char)
    }
}

pub fn uname() -> UtsName {
    unsafe {
        let mut ret = mem::MaybeUninit::uninit();
        ffi::uname(ret.as_mut_ptr());
        ret.assume_init()
    }
}

#[inline]
fn to_str<'a>(s: *const *const c_char) -> &'a str {
    unsafe {
        let res = CStr::from_ptr(*s).to_bytes();
        from_utf8_unchecked(res)
    }
}

#[cfg(target_os = "linux")]
#[test]
pub fn test_uname() {
    assert_eq!(uname().sysname(), "Linux");
}

#[cfg(target_os = "macos")]
#[test]
pub fn test_uname() {
    assert_eq!(uname().sysname(), "Darwin");
}
