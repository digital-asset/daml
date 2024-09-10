use protobuf::Message;
use protobuf::MessageField;
use std::collections::HashMap;

use crate::ledger::internal;
use crate::ledger::utils;
use crate::protobuf::value as lf;

#[allow(non_snake_case)]
pub fn logInfo(msg: &str) {
  unsafe {
    internal::logInfo(&internal::ByteString { ptr: msg.as_ptr(), size: msg.len() });
  }
}

#[allow(non_snake_case, unused)]
pub fn createContract(templateTyCon: lf::Identifier, arg: lf::Value) -> lf::Value {
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
pub fn fetchContractArg(templateTyCon: lf::Identifier, contractId: lf::Value, timeout: &str) -> lf::Value {
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
pub fn exerciseChoice(templateTyCon: lf::Identifier, contractId: lf::Value, choiceName: &str, choiceArg: lf::Value) -> lf::Value {
  unsafe {
    if contractId.has_contract_id() {
        let templateTyConBytes = templateTyCon.write_to_bytes().unwrap();
        let contractIdBytes = contractId.write_to_bytes().unwrap();
        let choiceArgBytes = choiceArg.write_to_bytes().unwrap();
        let templateTyConByteString = internal::ByteString { ptr: templateTyConBytes.as_ptr(), size: templateTyConBytes.len() };
        let contractIdByteString = internal::ByteString { ptr: contractIdBytes.as_ptr(), size: contractIdBytes.len() };
        let choiceNameByteString = internal::ByteString { ptr: choiceName.as_ptr(), size: choiceName.len() };
        let choiceArgByteString = internal::ByteString { ptr: choiceArgBytes.as_ptr(), size: choiceArgBytes.len() };

        let resultByteString = internal::exerciseChoice(&templateTyConByteString, &contractIdByteString, &choiceNameByteString, &choiceArgByteString);

        return utils::to_Value(resultByteString.ptr, resultByteString.size);
    } else {
        panic!();
    }
  }
}

#[allow(unused, non_snake_case)]
pub trait Choice {
    fn consuming(&self) -> lf::Value; // lf::value::Bool

    fn exercise(&self, contractArg: lf::Value, choiceArg: lf::Value) -> lf::Value; // lf::value::ContractId

    fn controllers(&self, contractArg: lf::Value, choiceArg: lf::Value) -> lf::Value {
        let mut result = lf::Value::new();
        let empty = lf::value::List::new();

        result.set_list(empty);

        return result; // lf::value::List<lf::value::Party>
    }

    fn observers(&self, contractArg: lf::Value, choiceArg: lf::Value) -> lf::Value {
        let mut result = lf::Value::new();
        let empty = lf::value::List::new();

        result.set_list(empty);

        return result; // lf::value::List<lf::value::Party>
    }

    fn authorizers(&self, contractArg: lf::Value, choiceArg: lf::Value) -> lf::Value {
        let mut result = lf::Value::new();
        let mut opt = lf::value::Optional::new();

        opt.value = MessageField::none();
        result.set_optional(opt);

        return result; // lf::value::Optional<lf::value::List<lf::value::Party>>
    }
}

#[allow(unused)]
pub trait Template<T> {
    fn new(arg: lf::Value) -> T;

    #[allow(non_snake_case)]
    fn templateId() -> lf::Identifier;

    fn choices() -> HashMap<String, Box<dyn Choice>> {
        return HashMap::new();
    }

    // TODO: this method should be part of a rust/protobuf DSL?
    #[allow(non_snake_case)]
    fn toLfValue(&self) -> lf::Value;

    fn precond(arg: lf::Value) -> lf::Value {
        let mut result = lf::Value::new();

        result.set_bool(true);

        return result; // lf::value::Boolean
    }

    fn signatories(arg: lf::Value) -> lf::Value {
        let mut result = lf::Value::new();
        let empty = lf::value::List::new();

        result.set_list(empty);

        return result; // lf::value::List<lf::value::Party>
    }

    fn observers(arg: lf::Value) -> lf::Value {
        let mut result = lf::Value::new();
        let empty = lf::value::List::new();

        result.set_list(empty);

        return result; // lf::value::List<lf::value::Party>
    }
}
