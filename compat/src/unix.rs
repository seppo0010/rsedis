use libc::funcs::posix88::unistd;

pub fn getpid() -> u32 {
    unsafe { unistd::getpid() as u32 }
}
