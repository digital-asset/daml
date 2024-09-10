use hex;
use ::protobuf::MessageField;
use ledger::api::Template;

mod codegen;

mod ledger;

mod protobuf;

mod templates;

use protobuf::value as lf;

#[no_mangle]
#[allow(non_snake_case)]
pub fn main() {
    let mut arg = lf::Value::new();
    let mut argRec = lf::value::Record::new();
    let mut argRecFields = Vec::new();
    let mut ownerField = lf::value::record::Field::new();
    let mut countField = lf::value::record::Field::new();
    let mut owner = lf::Value::new();
    let mut count = lf::Value::new();
    let mut choiceArg = lf::Value::new();

    owner.set_party(String::from("bob"));

    ownerField.value = MessageField::some(owner);

    count.set_int64(42i64);

    countField.value = MessageField::some(count);

    argRecFields.push(ownerField);
    argRecFields.push(countField);
    argRec.fields = argRecFields;
    arg.set_record(argRec);

    // Create contract using Daml evaluation
    let mut contractId = ledger::api::createContract(templates::SimpleTemplate::damlTemplateId(), arg);

    ledger::api::logInfo(&format!("Daml contract creation with ID {}", hex::encode(contractId.clone().take_contract_id())));

    choiceArg.set_unit(choiceArg.unit().clone());

    let mut newContractId1 = ledger::api::exerciseChoice(templates::SimpleTemplate::damlTemplateId(), contractId.clone(), "SimpleTemplate_increment", choiceArg.clone());
    ledger::api::logInfo(&format!("result of exercising Daml choice {} on contract ID {} is {}", "SimpleTemplate_increment", hex::encode(contractId.take_contract_id()), hex::encode(newContractId1.clone().take_contract_id())));

    // Exercise contract using Wasm evaluation
    let mut newContractId2 = ledger::api::exerciseChoice(templates::SimpleTemplate::templateId(), newContractId1.clone(), "SimpleTemplate_increment", choiceArg.clone());

    ledger::api::logInfo(&format!("result of exercising WASM choice {} on contract ID {} is {}", "SimpleTemplate_increment", hex::encode(newContractId1.take_contract_id()), hex::encode(newContractId2.take_contract_id())));
}
