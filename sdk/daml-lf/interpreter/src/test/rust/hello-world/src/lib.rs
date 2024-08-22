mod ledger {
    mod internal {
        #[repr(C, packed)]
        pub struct ByteString {
            pub ptr: *const u8,
            pub size: usize,
        }

        extern {
            #[allow(non_snake_case)]
            pub fn logInfo(msg: &ByteString);
        }
    }

    #[allow(non_snake_case)]
    pub fn logInfo(msg: &str) {
      unsafe {
        internal::logInfo(&internal::ByteString { ptr: msg.as_ptr(), size: msg.len() });
      }
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
