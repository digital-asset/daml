use crate::ledger::api::Template;
use crate::ledger;
use crate::templates;

impl templates::SimpleTemplate_increment {
    #[no_mangle]
    #[allow(non_snake_case)]
    pub unsafe fn SimpleTemplate_increment_choice_exercise(contractArgPtr: *const ledger::internal::ByteString, choiceArgPtr: *const ledger::internal::ByteString) -> *mut ledger::internal::ByteString {
        use protobuf::Message;

        let contractArg = ledger::utils::to_Value((*contractArgPtr).ptr, (*contractArgPtr).size);
        let choiceArg = ledger::utils::to_Value((*choiceArgPtr).ptr, (*choiceArgPtr).size);
        let result = templates::SimpleTemplate::choices().get(&String::from("SimpleTemplate_increment")).unwrap().exercise(contractArg, choiceArg);
        let resultBytes = result.write_to_bytes().unwrap();
        let boxedResult = Box::new(ledger::internal::ByteString { ptr: resultBytes.as_ptr(), size: resultBytes.len() });

        std::mem::forget(resultBytes);

        return Box::into_raw(boxedResult);
    }

    #[no_mangle]
    #[allow(non_snake_case)]
    pub unsafe fn SimpleTemplate_increment_choice_controllers(contractArgPtr: *const ledger::internal::ByteString, choiceArgPtr: *const ledger::internal::ByteString) -> *mut ledger::internal::ByteString {
        use protobuf::Message;

        let contractArg = ledger::utils::to_Value((*contractArgPtr).ptr, (*contractArgPtr).size);
        let choiceArg = ledger::utils::to_Value((*choiceArgPtr).ptr, (*choiceArgPtr).size);
        let result = templates::SimpleTemplate::choices().get(&String::from("SimpleTemplate_increment")).unwrap().controllers(contractArg, choiceArg);
        let resultBytes = result.write_to_bytes().unwrap();
        let boxedResult = Box::new(ledger::internal::ByteString { ptr: resultBytes.as_ptr(), size: resultBytes.len() });

        std::mem::forget(resultBytes);

        return Box::into_raw(boxedResult);
    }

    #[no_mangle]
    #[allow(non_snake_case)]
    pub unsafe fn SimpleTemplate_increment_choice_observers(contractArgPtr: *const ledger::internal::ByteString, choiceArgPtr: *const ledger::internal::ByteString) -> *mut ledger::internal::ByteString {
        use protobuf::Message;

        let contractArg = ledger::utils::to_Value((*contractArgPtr).ptr, (*contractArgPtr).size);
        let choiceArg = ledger::utils::to_Value((*choiceArgPtr).ptr, (*choiceArgPtr).size);
        let result = templates::SimpleTemplate::choices().get(&String::from("SimpleTemplate_increment")).unwrap().observers(contractArg, choiceArg);
        let resultBytes = result.write_to_bytes().unwrap();
        let boxedResult = Box::new(ledger::internal::ByteString { ptr: resultBytes.as_ptr(), size: resultBytes.len() });

        std::mem::forget(resultBytes);

        return Box::into_raw(boxedResult);
    }

    #[no_mangle]
    #[allow(non_snake_case)]
    pub unsafe fn SimpleTemplate_increment_choice_authorizers(contractArgPtr: *const ledger::internal::ByteString, choiceArgPtr: *const ledger::internal::ByteString) -> *mut ledger::internal::ByteString {
        use protobuf::Message;

        let contractArg = ledger::utils::to_Value((*contractArgPtr).ptr, (*contractArgPtr).size);
        let choiceArg = ledger::utils::to_Value((*choiceArgPtr).ptr, (*choiceArgPtr).size);
        let result = templates::SimpleTemplate::choices().get(&String::from("SimpleTemplate_increment")).unwrap().authorizers(contractArg, choiceArg);
        let resultBytes = result.write_to_bytes().unwrap();
        let boxedResult = Box::new(ledger::internal::ByteString { ptr: resultBytes.as_ptr(), size: resultBytes.len() });

        std::mem::forget(resultBytes);

        return Box::into_raw(boxedResult);
    }
}

impl templates::SimpleTemplate_decrement {
    #[no_mangle]
    #[allow(non_snake_case)]
    pub unsafe fn SimpleTemplate_decrement_choice_exercise(contractArgPtr: *const ledger::internal::ByteString, choiceArgPtr: *const ledger::internal::ByteString) -> *mut ledger::internal::ByteString {
        use protobuf::Message;

        let contractArg = ledger::utils::to_Value((*contractArgPtr).ptr, (*contractArgPtr).size);
        let choiceArg = ledger::utils::to_Value((*choiceArgPtr).ptr, (*choiceArgPtr).size);
        let result = templates::SimpleTemplate::choices().get(&String::from("SimpleTemplate_decrement")).unwrap().exercise(contractArg, choiceArg);
        let resultBytes = result.write_to_bytes().unwrap();
        let boxedResult = Box::new(ledger::internal::ByteString { ptr: resultBytes.as_ptr(), size: resultBytes.len() });

        std::mem::forget(resultBytes);

        return Box::into_raw(boxedResult);
    }

    #[no_mangle]
    #[allow(non_snake_case)]
    pub unsafe fn SimpleTemplate_decrement_choice_controllers(contractArgPtr: *const ledger::internal::ByteString, choiceArgPtr: *const ledger::internal::ByteString) -> *mut ledger::internal::ByteString {
        use protobuf::Message;

        let contractArg = ledger::utils::to_Value((*contractArgPtr).ptr, (*contractArgPtr).size);
        let choiceArg = ledger::utils::to_Value((*choiceArgPtr).ptr, (*choiceArgPtr).size);
        let result = templates::SimpleTemplate::choices().get(&String::from("SimpleTemplate_decrement")).unwrap().controllers(contractArg, choiceArg);
        let resultBytes = result.write_to_bytes().unwrap();
        let boxedResult = Box::new(ledger::internal::ByteString { ptr: resultBytes.as_ptr(), size: resultBytes.len() });

        std::mem::forget(resultBytes);

        return Box::into_raw(boxedResult);
    }

    #[no_mangle]
    #[allow(non_snake_case)]
    pub unsafe fn SimpleTemplate_decrement_choice_observers(contractArgPtr: *const ledger::internal::ByteString, choiceArgPtr: *const ledger::internal::ByteString) -> *mut ledger::internal::ByteString {
        use protobuf::Message;

        let contractArg = ledger::utils::to_Value((*contractArgPtr).ptr, (*contractArgPtr).size);
        let choiceArg = ledger::utils::to_Value((*choiceArgPtr).ptr, (*choiceArgPtr).size);
        let result = templates::SimpleTemplate::choices().get(&String::from("SimpleTemplate_decrement")).unwrap().observers(contractArg, choiceArg);
        let resultBytes = result.write_to_bytes().unwrap();
        let boxedResult = Box::new(ledger::internal::ByteString { ptr: resultBytes.as_ptr(), size: resultBytes.len() });

        std::mem::forget(resultBytes);

        return Box::into_raw(boxedResult);
    }

    #[no_mangle]
    #[allow(non_snake_case)]
    pub unsafe fn SimpleTemplate_decrement_choice_authorizers(contractArgPtr: *const ledger::internal::ByteString, choiceArgPtr: *const ledger::internal::ByteString) -> *mut ledger::internal::ByteString {
        use protobuf::Message;

        let contractArg = ledger::utils::to_Value((*contractArgPtr).ptr, (*contractArgPtr).size);
        let choiceArg = ledger::utils::to_Value((*choiceArgPtr).ptr, (*choiceArgPtr).size);
        let result = templates::SimpleTemplate::choices().get(&String::from("SimpleTemplate_decrement")).unwrap().authorizers(contractArg, choiceArg);
        let resultBytes = result.write_to_bytes().unwrap();
        let boxedResult = Box::new(ledger::internal::ByteString { ptr: resultBytes.as_ptr(), size: resultBytes.len() });

        std::mem::forget(resultBytes);

        return Box::into_raw(boxedResult);
    }
}

#[allow(non_snake_case, unused)]
impl templates::SimpleTemplate {
    #[no_mangle]
    pub unsafe fn SimpleTemplate_precond(argPtr: *const ledger::internal::ByteString) -> *mut ledger::internal::ByteString {
        use protobuf::Message;

        let arg = ledger::utils::to_Value((*argPtr).ptr, (*argPtr).size);
        let result = templates::SimpleTemplate::precond(arg);
        let resultBytes = result.write_to_bytes().unwrap();
        let boxedResult = Box::new(ledger::internal::ByteString { ptr: resultBytes.as_ptr(), size: resultBytes.len() });

        std::mem::forget(resultBytes);

        return Box::into_raw(boxedResult);
    }

    #[no_mangle]
    pub unsafe fn SimpleTemplate_signatories(argPtr: *const ledger::internal::ByteString) -> *mut ledger::internal::ByteString {
        use protobuf::Message;

        let arg = ledger::utils::to_Value((*argPtr).ptr, (*argPtr).size);
        let result = templates::SimpleTemplate::signatories(arg);
        let resultBytes = result.write_to_bytes().unwrap();
        let boxedResult = Box::new(ledger::internal::ByteString { ptr: resultBytes.as_ptr(), size: resultBytes.len() });

        std::mem::forget(resultBytes);

        return Box::into_raw(boxedResult);
    }

    #[no_mangle]
    pub unsafe fn SimpleTemplate_observers(argPtr: *const ledger::internal::ByteString) -> *mut ledger::internal::ByteString {
        use protobuf::Message;

        let arg = ledger::utils::to_Value((*argPtr).ptr, (*argPtr).size);
        let result = templates::SimpleTemplate::observers(arg);
        let resultBytes = result.write_to_bytes().unwrap();
        let boxedResult = Box::new(ledger::internal::ByteString { ptr: resultBytes.as_ptr(), size: resultBytes.len() });

        std::mem::forget(resultBytes);

        return Box::into_raw(boxedResult);
    }
}
