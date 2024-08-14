mod ledger {
    mod internal {
        extern {
            pub fn logInfo(addr: *const u8, size: usize);
        }
    }

    pub fn logInfo(msg: &str) {
      unsafe { internal::logInfo(msg.as_ptr(), msg.len()); }
    }
}

#[no_mangle]
pub fn alloc(len: usize) -> *mut u8 {
    let mut buf = Vec::with_capacity(len);
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);

    return ptr;
}

#[no_mangle]
pub fn main() {
    ledger::logInfo("hello-world");
}
