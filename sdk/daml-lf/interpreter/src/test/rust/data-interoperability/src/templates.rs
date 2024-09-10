use crate::ledger::api::Template;
use crate::ledger;
use crate::protobuf::value as lf;
use protobuf::MessageField;
use std::collections::HashMap;

#[allow(unused, non_snake_case, non_camel_case_types)]
pub struct SimpleTemplate_increment;

#[allow(non_snake_case)]
impl ledger::api::Choice for SimpleTemplate_increment {
    fn consuming(&self) -> lf::Value {
        let mut result = lf::Value::new();

        result.set_bool(true);

        return result;
    }

    fn exercise(&self, contractArg: lf::Value, choiceArg: lf::Value) -> lf::Value {
        assert!(choiceArg.has_unit());

        let contract = SimpleTemplate::new(contractArg);

        let updatedContract = SimpleTemplate {
            owner: contract.owner,
            count: contract.count + 1,
        };

        return ledger::api::createContract(SimpleTemplate::templateId(), updatedContract.toLfValue());
    }

    fn controllers(&self, contractArg: lf::Value, choiceArg: lf::Value) -> lf::Value {
        assert!(choiceArg.has_unit());

        let owner = ledger::utils::get_field(contractArg, 0);
        let mut result = lf::Value::new();
        let mut list = lf::value::List::new();

        list.elements = vec![owner];
        result.set_list(list);

        return result;
    }

    #[allow(unused)]
    fn observers(&self, contractArg: lf::Value, choiceArg: lf::Value) -> lf::Value {
        assert!(choiceArg.has_unit());

        let mut obs = lf::Value::new();
        let mut result = lf::Value::new();
        let mut list = lf::value::List::new();

        obs.set_party(String::from("alice"));
        list.elements = vec![obs];
        result.set_list(list);

        return result;
    }

    #[allow(unused)]
    fn authorizers(&self, contractArg: lf::Value, choiceArg: lf::Value) -> lf::Value {
        assert!(choiceArg.has_unit());

        let mut obs = lf::Value::new();
        let mut result = lf::Value::new();
        let mut list = lf::value::List::new();
        let mut optList = lf::Value::new();
        let mut opt = lf::value::Optional::new();

        obs.set_party(String::from("bob"));
        list.elements = vec![obs];
        optList.set_list(list);
        opt.value = MessageField::some(optList);
        result.set_optional(opt);

        return result;
    }
}

#[allow(non_snake_case, unused)]
pub struct SimpleTemplate {
    owner: String,
    count: i64,
}

impl ledger::api::Template<SimpleTemplate> for SimpleTemplate {
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
    fn templateId() -> lf::Identifier {
        let mut templateId = lf::Identifier::new();

        templateId.package_id = String::from("cae3f9de0ee19fa89d4b65439865e1942a3a98b50c86156c3ab1b09e8266c833");
        templateId.module_name = Vec::from([String::from("wasm_module")]);
        templateId.name = Vec::from([String::from("SimpleTemplate"), String::from("new")]);

        return templateId;
    }

    fn choices() -> HashMap<String, Box<dyn ledger::api::Choice>> {
        let mut result = HashMap::new();

        result.insert(String::from("SimpleTemplate_increment"), Box::new(SimpleTemplate_increment) as Box<dyn ledger::api::Choice>);

        return result;
    }

    #[allow(unused)]
    fn signatories(arg: lf::Value) -> lf::Value {
         let mut bob = lf::Value::new();
         let mut charlie = lf::Value::new();
         let mut result = lf::Value::new();
         let mut list = lf::value::List::new();

         bob.set_party(String::from("bob"));
         list.elements = vec![bob];
         result.set_list(list);

         return result;
    }

    // TODO: this should be part of a rust/protobuf DSL?
    #[allow(non_snake_case)]
    fn toLfValue(&self) -> lf::Value {
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

impl SimpleTemplate {
    #[allow(non_snake_case)]
    pub fn damlTemplateId() -> lf::Identifier {
        let mut templateId = lf::Identifier::new();

        templateId.package_id = String::from("feadceaea7984045371ed7f47f4343319e6cd76332682789125a547979118412");
        templateId.module_name = Vec::from([String::from("SimpleTemplate")]);
        templateId.name = Vec::from([String::from("SimpleTemplate")]);

        return templateId;
    }
}
