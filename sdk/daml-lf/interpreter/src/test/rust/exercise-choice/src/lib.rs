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

    let mut contractId = lf::Value::new();
    let mut choiceArg = lf::Value::new();

    contractId.set_contract_id(hex::decode("0083d63f9d6c27eb34b37890d0f365c99505f32f06727fbefa2931f9d99d51f9ac").unwrap());

    choiceArg.set_unit(choiceArg.unit().clone());

    let mut newContractId = ledger::api::exerciseChoice(templates::SimpleTemplate::templateId(), contractId.clone(), "SimpleTemplate_increment", choiceArg, true);

    ledger::api::logInfo(&format!("result of exercising choice {} on contract ID {} is {}", "SimpleTemplate_increment", hex::encode(contractId.take_contract_id()), hex::encode(newContractId.take_contract_id())));
}
