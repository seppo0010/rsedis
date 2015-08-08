use libc::funcs::posix88::unistd;
use utsname::uname;

pub fn getpid() -> u32 {
    unsafe { unistd::getpid() as u32 }
}

pub fn getos() -> (String, String, String) {
    let name = uname();
    (name.sysname().to_owned(), name.release().to_owned(), name.machine().to_owned())
}
