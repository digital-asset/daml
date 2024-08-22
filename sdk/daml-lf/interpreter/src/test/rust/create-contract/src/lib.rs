#[path = "lf/value.rs"]
mod lf;

mod ledger {
    use protobuf::Message;

    mod internal {
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
        }
    }

    #[allow(non_snake_case)]
    pub fn logInfo(msg: &str) {
      unsafe {
        internal::logInfo(&internal::ByteString { ptr: msg.as_ptr(), size: msg.len() });
      }
    }

    #[allow(non_snake_case)]
    pub fn createContract(templateTyCon: crate::lf::Identifier, arg: crate::lf::Value) -> String {
      unsafe {
        let templateTyConBytes = templateTyCon.write_to_bytes().unwrap();
        let argBytes = arg.write_to_bytes().unwrap();
        let templateTyConByteString = internal::ByteString { ptr: templateTyConBytes.as_ptr(), size: templateTyConBytes.len() };
        let argByteString = internal::ByteString { ptr: argBytes.as_ptr(), size: argBytes.len() };

        let contractIdByteString = internal::createContract(&templateTyConByteString, &argByteString);

        return utils::to_String(contractIdByteString.ptr, contractIdByteString.size);
      }
    }

    pub trait Template<T> {
        fn new(arg: crate::lf::Value) -> T;

        // pub fn precond(arg: crate::lf::Value) -> bool;

        // pub fn signatories(arg: crate::lf::Value) -> [crate::lf::Party];

        // pub fn observers(arg: crate::lf::Value) -> [crate::lf::Party];
    }

    pub mod utils {
        pub fn get_field(mut r: crate::lf::Value, f: usize) -> crate::lf::Value {
          if r.has_record() {
            return r.take_record().fields[f].value.clone().unwrap();
          } else {
            panic!();
          }
        }

        #[allow(non_snake_case)]
        pub unsafe fn to_String(ptr: *const u8, size: usize) -> String {
          return String::from_utf8(std::slice::from_raw_parts(ptr, size).to_vec()).unwrap();
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

#[allow(non_snake_case)]
struct SimpleTemplate {
    owner: String,
    count: i64,
}

impl ledger::Template<SimpleTemplate> for SimpleTemplate {
    fn new(arg: lf::Value) -> SimpleTemplate {
      // SimpleTemplates are created using paired (record) arguments
      let owner = ledger::utils::get_field(arg.clone(), 0).take_party();
      let count = ledger::utils::get_field(arg, 1).int64();

      return SimpleTemplate {
          owner: owner,
          count: count,
      };
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub fn main() {
    use protobuf::MessageField;

    let mut arg = lf::Value::new();
    let mut argRec = lf::value::Record::new();
    let mut argRecFields = Vec::new();
    let mut ownerField = lf::value::record::Field::new();
    let mut countField = lf::value::record::Field::new();
    let mut templateId = lf::Identifier::new();
    let mut owner = lf::Value::new();
    let mut count = lf::Value::new();

    owner.set_party(String::from("bob"));

    ownerField.value = MessageField::some(owner);

    count.set_int64(42i64);

    countField.value = MessageField::some(count);

    argRecFields.push(ownerField);
    argRecFields.push(countField);
    argRec.fields = argRecFields;
    arg.set_record(argRec);

    templateId.package_id = String::from("cae3f9de0ee19fa89d4b65439865e1942a3a98b50c86156c3ab1b09e8266c833");
    templateId.module_name = Vec::from([String::from("create_contract")]);
    templateId.name = Vec::from([String::from("SimpleTemplate"), String::from("new")]);

    let contractId = ledger::createContract(templateId, arg);

    ledger::logInfo(&format!("created contract with ID {}", contractId));
}
