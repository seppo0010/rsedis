use kernel32::GetCurrentProcessId;

pub fn getpid() -> u32 {
    unsafe { GetCurrentProcessId() as u32 }
}
