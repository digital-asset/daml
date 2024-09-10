#[repr(C, packed)]
#[allow(non_snake_case)]
pub struct ByteString {
    pub ptr: *const u8,
    pub size: usize,
}

extern {
    #[allow(non_snake_case)]
    pub fn logInfo(msg: &ByteString);

    #[allow(non_snake_case)]
    pub fn createContract<'a>(templateTyCon: &'a ByteString, arg: &'a ByteString) -> &'a ByteString;

    #[allow(non_snake_case)]
    pub fn fetchContractArg<'a>(templateTyCon: &'a ByteString, contractId: &'a ByteString, timeout: &'a ByteString) -> &'a ByteString;

    #[allow(non_snake_case)]
    pub fn exerciseChoice<'a>(templateTyCon: &'a ByteString, contractId: &'a ByteString, choiceName: &'a ByteString, choiceArg: &'a ByteString) -> &'a ByteString;
}

#[no_mangle]
pub fn alloc(len: usize) -> *mut u8 {
    let mut buf = Vec::with_capacity(len);
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);

    return ptr;
}

#[no_mangle]
pub unsafe fn dealloc(ptr: *mut u8, size: usize) {
    let data = Vec::from_raw_parts(ptr, size, size);
    std::mem::drop(data);
}
