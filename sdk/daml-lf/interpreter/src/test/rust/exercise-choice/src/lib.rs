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

            #[allow(non_snake_case)]
            pub fn fetchContractArg<'a>(templateTyCon: &'a ByteString, contractId: &'a ByteString, timeout: &'a ByteString) -> &'a ByteString;

            #[allow(non_snake_case)]
            pub fn exerciseChoice<'a>(templateTyCon: &'a ByteString, contractId: &'a ByteString, choiceName: &'a ByteString, choiceArg: &'a ByteString, consuming: &'a ByteString) -> &'a ByteString;
        }
    }

    #[allow(non_snake_case)]
    pub fn logInfo(msg: &str) {
      unsafe {
        internal::logInfo(&internal::ByteString { ptr: msg.as_ptr(), size: msg.len() });
      }
    }

    #[allow(non_snake_case, unused)]
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

    #[allow(non_snake_case, unused)]
    pub fn fetchContractArg(templateTyCon: crate::lf::Identifier, contractId: crate::lf::Value, timeout: &str) -> crate::lf::Value {
      unsafe {
        if contractId.has_contract_id() {
            let templateTyConBytes = templateTyCon.write_to_bytes().unwrap();
            let contractIdBytes = contractId.write_to_bytes().unwrap();
            let templateTyConByteString = internal::ByteString { ptr: templateTyConBytes.as_ptr(), size: templateTyConBytes.len() };
            let contractIdByteString = internal::ByteString { ptr: contractIdBytes.as_ptr(), size: contractIdBytes.len() };
            let timeoutByteString = internal::ByteString { ptr: timeout.as_ptr(), size: timeout.len() };

            let contractArgByteString = internal::fetchContractArg(&templateTyConByteString, &contractIdByteString, &timeoutByteString);

            return utils::to_Value(contractArgByteString.ptr, contractArgByteString.size);
        } else {
            panic!();
        }
      }
    }

    #[allow(non_snake_case)]
    pub fn exerciseChoice(templateTyCon: crate::lf::Identifier, contractId: crate::lf::Value, choiceName: &str, choiceArg: crate::lf::Value, consuming: bool) -> crate::lf::Value {
      unsafe {
        if contractId.has_contract_id() {
            let templateTyConBytes = templateTyCon.write_to_bytes().unwrap();
            let contractIdBytes = contractId.write_to_bytes().unwrap();
            let choiceArgBytes = choiceArg.write_to_bytes().unwrap();
            let consumingBytes: [u8; 1] = [u8::from(consuming); 1];
            let templateTyConByteString = internal::ByteString { ptr: templateTyConBytes.as_ptr(), size: templateTyConBytes.len() };
            let contractIdByteString = internal::ByteString { ptr: contractIdBytes.as_ptr(), size: contractIdBytes.len() };
            let choiceNameByteString = internal::ByteString { ptr: choiceName.as_ptr(), size: choiceName.len() };
            let choiceArgByteString = internal::ByteString { ptr: choiceArgBytes.as_ptr(), size: choiceArgBytes.len() };
            let consumingByteString = internal::ByteString { ptr: consumingBytes.as_ptr(), size: consumingBytes.len() };

            let resultByteString = internal::exerciseChoice(&templateTyConByteString, &contractIdByteString, &choiceNameByteString, &choiceArgByteString, &consumingByteString);

            return utils::to_Value(resultByteString.ptr, resultByteString.size);
        } else {
            panic!();
        }
      }
    }

    #[allow(unused)]
    pub trait Template<T> {
        fn new(arg: crate::lf::Value) -> T;

        #[allow(non_snake_case)]
        fn getTemplateId(&self) -> crate::lf::Identifier;

        #[allow(non_snake_case)]
        fn toLfValue(&self) -> crate::lf::Value;

        // pub fn precond(arg: crate::lf::Value) -> bool;

        // pub fn signatories(arg: crate::lf::Value) -> [crate::lf::Party];

        // pub fn observers(arg: crate::lf::Value) -> [crate::lf::Party];
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

    #[allow(non_snake_case)]
    fn getTemplateId(&self) -> lf::Identifier {
        let mut templateId = lf::Identifier::new();

        templateId.package_id = String::from("cae3f9de0ee19fa89d4b65439865e1942a3a98b50c86156c3ab1b09e8266c833");
        templateId.module_name = Vec::from([String::from("exercise_choice")]);
        templateId.name = Vec::from([String::from("SimpleTemplate"), String::from("new")]);

        return templateId;
    }

    #[allow(non_snake_case)]
    fn toLfValue(&self) -> lf::Value {
        use protobuf::MessageField;

        let mut arg = lf::Value::new();
        let mut argRec = lf::value::Record::new();
        let mut argRecFields = Vec::new();
        let mut ownerField = lf::value::record::Field::new();
        let mut countField = lf::value::record::Field::new();
        let mut owner = lf::Value::new();
        let mut count = lf::Value::new();

        owner.set_party(self.owner.clone());

        ownerField.value = MessageField::some(owner);

        count.set_int64(self.count);

        countField.value = MessageField::some(count);

        argRecFields.push(ownerField);
        argRecFields.push(countField);
        argRec.fields = argRecFields;
        arg.set_record(argRec);

        return arg;
    }
}

// TODO: implement choice macro for generating low level choice function wrappers

#[no_mangle]
#[allow(non_snake_case)]
// TODO: add choice macro annotation?
pub fn SimpleTemplate_increment(contractArg: lf::Value, choiceArg: lf::Value) -> lf::Value {
    use ledger::Template;

    assert!(choiceArg.has_unit());

    let contract = SimpleTemplate::new(contractArg);

    let updatedContract = SimpleTemplate {
        owner: contract.owner,
        count: contract.count + 1,
    };

    return ledger::createContract(SimpleTemplate::getTemplateId(&updatedContract), SimpleTemplate::toLfValue(&updatedContract));
}
// TODO: following low level choice wrapper function should be generated by choice macro
#[no_mangle]
#[allow(non_snake_case)]
pub unsafe fn SimpleTemplate_increment_choice(contractArgPtr: *const ledger::internal::ByteString, choiceArgPtr: *const ledger::internal::ByteString) -> *mut ledger::internal::ByteString {
    use protobuf::Message;

    let contractArg = ledger::utils::to_Value((*contractArgPtr).ptr, (*contractArgPtr).size);
    let choiceArg = ledger::utils::to_Value((*choiceArgPtr).ptr, (*choiceArgPtr).size);
    let result = SimpleTemplate_increment(contractArg, choiceArg);
    let resultBytes = result.write_to_bytes().unwrap();
    let boxedResult = Box::new(ledger::internal::ByteString { ptr: resultBytes.as_ptr(), size: resultBytes.len() });

    std::mem::forget(resultBytes);

    return Box::into_raw(boxedResult);
}

#[allow(non_snake_case)]
// TODO: add choice macro annotation?
pub fn SimpleTemplate_decrement(contractArg: lf::Value, choiceArg: lf::Value) -> lf::Value {
    use ledger::Template;

    assert!(choiceArg.has_unit());

    let contract = SimpleTemplate::new(contractArg);

    let updatedContract = SimpleTemplate {
        owner: contract.owner,
        count: contract.count - 1,
    };

    return ledger::createContract(SimpleTemplate::getTemplateId(&updatedContract), SimpleTemplate::toLfValue(&updatedContract));
}
// TODO: following low level choice wrapper function should be generated by choice macro
#[no_mangle]
#[allow(non_snake_case)]
pub unsafe fn SimpleTemplate_decrement_choice(contractArgPtr: *const ledger::internal::ByteString, choiceArgPtr: *const ledger::internal::ByteString) -> *mut ledger::internal::ByteString {
    use protobuf::Message;

    let contractArg = ledger::utils::to_Value((*contractArgPtr).ptr, (*contractArgPtr).size);
    let choiceArg = ledger::utils::to_Value((*choiceArgPtr).ptr, (*choiceArgPtr).size);
    let result = SimpleTemplate_decrement(contractArg, choiceArg);
    let resultBytes = result.write_to_bytes().unwrap();
    let boxedResult = Box::new(ledger::internal::ByteString { ptr: resultBytes.as_ptr(), size: resultBytes.len() });

    std::mem::forget(resultBytes);

    return Box::into_raw(boxedResult);
}

#[no_mangle]
#[allow(non_snake_case)]
pub fn main() {
    use hex;
    use protobuf::MessageField;
    use ledger::Template;

    let mut contractArg = lf::Value::new();
    let mut contractArgRec = lf::value::Record::new();
    let mut contractArgRecFields = Vec::new();
    let mut ownerField = lf::value::record::Field::new();
    let mut countField = lf::value::record::Field::new();
    let mut owner = lf::Value::new();
    let mut count = lf::Value::new();

    owner.set_party(String::from("bob"));

    ownerField.value = MessageField::some(owner);

    count.set_int64(42i64);

    countField.value = MessageField::some(count);

    contractArgRecFields.push(ownerField);
    contractArgRecFields.push(countField);
    contractArgRec.fields = contractArgRecFields;
    contractArg.set_record(contractArgRec);

    let contractInst = SimpleTemplate::new(contractArg);

    let templateId = SimpleTemplate::getTemplateId(&contractInst);

    let mut contractId = lf::Value::new();
    let choiceName = "SimpleTemplate_increment";
    let mut choiceArg = lf::Value::new();

    contractId.set_contract_id(hex::decode("0083d63f9d6c27eb34b37890d0f365c99505f32f06727fbefa2931f9d99d51f9ac").unwrap());

    choiceArg.set_unit(choiceArg.unit().clone());

    let mut newContractId = ledger::exerciseChoice(templateId, contractId.clone(), choiceName, choiceArg, true);

    ledger::logInfo(&format!("result of exercising choice {} on contract ID {} is {}", choiceName, hex::encode(contractId.take_contract_id()), hex::encode(newContractId.take_contract_id())));
}
