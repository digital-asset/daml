use ledger::Template;

#[path = "lf/value.rs"]
mod lf;

mod ledger {
    use protobuf::Message;

    pub mod internal {
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
    }

    #[allow(non_snake_case)]
    pub fn logInfo(msg: &str) {
      unsafe {
        internal::logInfo(&internal::ByteString { ptr: msg.as_ptr(), size: msg.len() });
      }
    }

    #[allow(non_snake_case)]
    pub fn createContract(templateTyCon: crate::lf::Identifier, arg: crate::lf::Value) -> crate::lf::Value {
      unsafe {
        let templateTyConBytes = templateTyCon.write_to_bytes().unwrap();
        let argBytes = arg.write_to_bytes().unwrap();
        let templateTyConByteString = internal::ByteString { ptr: templateTyConBytes.as_ptr(), size: templateTyConBytes.len() };
        let argByteString = internal::ByteString { ptr: argBytes.as_ptr(), size: argBytes.len() };

        let contractIdByteString = internal::createContract(&templateTyConByteString, &argByteString);

        return utils::to_Value(contractIdByteString.ptr, contractIdByteString.size);
      }
    }

    #[allow(unused)]
    pub trait Template<T> {
        fn new(arg: crate::lf::Value) -> T;

        fn precond(arg: crate::lf::Value) -> crate::lf::Value {
            let mut result = crate::lf::Value::new();

            result.set_bool(true);

            return result;
        }

        fn signatories(arg: crate::lf::Value) -> crate::lf::Value {
            let mut result = crate::lf::Value::new();
            let empty = crate::lf::value::List::new();

            result.set_list(empty);

            return result;
        }

        fn observers(arg: crate::lf::Value) -> crate::lf::Value {
            let mut result = crate::lf::Value::new();
            let empty = crate::lf::value::List::new();

            result.set_list(empty);

            return result;
        }
    }

    pub mod utils {
        use protobuf::Message;

        pub fn get_field(mut r: crate::lf::Value, f: usize) -> crate::lf::Value {
          if r.has_record() {
            return r.take_record().fields[f].value.clone().unwrap();
          } else {
            panic!();
          }
        }

        #[allow(non_snake_case)]
        pub unsafe fn to_Value(ptr: *const u8, size: usize) -> crate::lf::Value {
          return crate::lf::Value::parse_from_bytes(std::slice::from_raw_parts(ptr, size)).unwrap();
        }
    }
}

#[allow(non_snake_case, unused)]
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

    #[allow(unused)]
    fn signatories(arg: lf::Value) -> lf::Value {
         let mut obs = lf::Value::new();
         let mut result = lf::Value::new();
         let mut list = lf::value::List::new();

         obs.set_party(String::from("bob"));
         list.elements = vec![obs];
         result.set_list(list);

         return result;
    }
}

#[allow(non_snake_case, unused)]
impl SimpleTemplate {
    #[no_mangle]
    pub unsafe fn SimpleTemplate_precond(argPtr: *const ledger::internal::ByteString) -> *mut ledger::internal::ByteString {
        use protobuf::Message;

        let arg = ledger::utils::to_Value((*argPtr).ptr, (*argPtr).size);
        let result = SimpleTemplate::precond(arg);
        let resultBytes = result.write_to_bytes().unwrap();
        let boxedResult = Box::new(ledger::internal::ByteString { ptr: resultBytes.as_ptr(), size: resultBytes.len() });

        std::mem::forget(resultBytes);

        return Box::into_raw(boxedResult);
    }

    #[no_mangle]
    pub unsafe fn SimpleTemplate_signatories(argPtr: *const ledger::internal::ByteString) -> *mut ledger::internal::ByteString {
        use protobuf::Message;

        let arg = ledger::utils::to_Value((*argPtr).ptr, (*argPtr).size);
        let result = SimpleTemplate::signatories(arg);
        let resultBytes = result.write_to_bytes().unwrap();
        let boxedResult = Box::new(ledger::internal::ByteString { ptr: resultBytes.as_ptr(), size: resultBytes.len() });

        std::mem::forget(resultBytes);

        return Box::into_raw(boxedResult);
    }

    #[no_mangle]
    pub unsafe fn SimpleTemplate_observers(argPtr: *const ledger::internal::ByteString) -> *mut ledger::internal::ByteString {
        use protobuf::Message;

        let arg = ledger::utils::to_Value((*argPtr).ptr, (*argPtr).size);
        let result = SimpleTemplate::observers(arg);
        let resultBytes = result.write_to_bytes().unwrap();
        let boxedResult = Box::new(ledger::internal::ByteString { ptr: resultBytes.as_ptr(), size: resultBytes.len() });

        std::mem::forget(resultBytes);

        return Box::into_raw(boxedResult);
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub fn main() {
    use hex;
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

    let mut contractId = ledger::createContract(templateId, arg);

    ledger::logInfo(&format!("created contract with ID {}", hex::encode(contractId.take_contract_id())));
}
