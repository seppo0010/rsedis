use winapi::um::processthreadsapi::GetCurrentProcessId;

pub fn getpid() -> u32 {
    unsafe { GetCurrentProcessId() as u32 }
}

pub fn getos() -> (String, String, String) {
    (
        "Windows".to_owned(),
        "Unknown".to_owned(),
        "Unknown".to_owned(),
    )
}
