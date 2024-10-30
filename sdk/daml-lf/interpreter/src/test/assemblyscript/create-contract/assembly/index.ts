// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as api from "./ledger/api";
import * as internal from "./ledger/internal";
import { SimpleTemplate, SimpleTemplateCompanion } from "./templates";

export function main(): void {
  let contract: api.Contract<SimpleTemplate> = SimpleTemplate.create(
    "alice",
    42,
  ).save<SimpleTemplate>();

  api.logInfo(`created contract with ID ${contract.contractId()}`);
}

// The following are code generated export functions for each api.Template instance

export function SimpleTemplate_increment_choice_observers(
  contractArg: internal.ByteString,
  choiceArg: internal.ByteString,
): internal.ByteString {
  let template = new SimpleTemplateCompanion().fromLfValue<SimpleTemplate>(
    api.LfValue.fromProtobuf(contractArg.toProtobuf()),
  );
  return internal.ByteString.fromProtobuf(
    api.LfValueSet.fromArray(
      template
        .choices()
        .get("SimpleTemplate_increment")
        .apply(api.LfValue.fromProtobuf(choiceArg.toProtobuf()))
        .observers()
        .values()
        .map<api.LfValueParty>(party => new api.LfValueParty(party)),
    ).toProtobuf(),
  );
}

export function SimpleTemplate_increment_choice_authorizers(
  contractArg: internal.ByteString,
  choiceArg: internal.ByteString,
): internal.ByteString {
  let template = new SimpleTemplateCompanion().fromLfValue<SimpleTemplate>(
    api.LfValue.fromProtobuf(contractArg.toProtobuf()),
  );
  return internal.ByteString.fromProtobuf(
    api.LfValueSet.fromArray(
      template
        .choices()
        .get("SimpleTemplate_increment")
        .apply(api.LfValue.fromProtobuf(choiceArg.toProtobuf()))
        .authorizers()
        .values()
        .map<api.LfValueParty>(party => new api.LfValueParty(party)),
    ).toProtobuf(),
  );
}

export function SimpleTemplate_precond(
  arg: internal.ByteString,
): internal.ByteString {
  let contractArg = api.LfValue.fromProtobuf(arg.toProtobuf());
  let template = new SimpleTemplateCompanion().fromLfValue<SimpleTemplate>(
    contractArg,
  );
  let precondition =
    new SimpleTemplateCompanion().isValidArg(contractArg) && template.precond();
  return internal.ByteString.fromProtobuf(
    new api.LfValueBool(precondition).toProtobuf(),
  );
}

export function SimpleTemplate_signatories(
  contractArg: internal.ByteString,
): internal.ByteString {
  let template = new SimpleTemplateCompanion().fromLfValue<SimpleTemplate>(
    api.LfValue.fromProtobuf(contractArg.toProtobuf()),
  );
  return internal.ByteString.fromProtobuf(
    api.LfValueSet.fromArray(
      template
        .signatories()
        .values()
        .map<api.LfValueParty>(party => new api.LfValueParty(party)),
    ).toProtobuf(),
  );
}

export function SimpleTemplate_observers(
  contractArg: internal.ByteString,
): internal.ByteString {
  let template = new SimpleTemplateCompanion().fromLfValue<SimpleTemplate>(
    api.LfValue.fromProtobuf(contractArg.toProtobuf()),
  );
  return internal.ByteString.fromProtobuf(
    api.LfValueSet.fromArray(
      template
        .observers()
        .values()
        .map<api.LfValueParty>(party => new api.LfValueParty(party)),
    ).toProtobuf(),
  );
}

export function SimpleTemplate_increment_consuming_property(
  contractArg: internal.ByteString,
): internal.ByteString {
  let template = new SimpleTemplateCompanion().fromLfValue<SimpleTemplate>(
    api.LfValue.fromProtobuf(contractArg.toProtobuf()),
  );
  return internal.ByteString.fromProtobuf(
    new api.LfValueBool(
      template.choices().get("SimpleTemplate_increment").consuming(),
    ).toProtobuf(),
  );
}

export function SimpleTemplate_increment_choice_controllers(
  contractArg: internal.ByteString,
  choiceArg: internal.ByteString,
): internal.ByteString {
  let template = new SimpleTemplateCompanion().fromLfValue<SimpleTemplate>(
    api.LfValue.fromProtobuf(contractArg.toProtobuf()),
  );
  return internal.ByteString.fromProtobuf(
    api.LfValueSet.fromArray(
      template
        .choices()
        .get("SimpleTemplate_increment")
        .apply(api.LfValue.fromProtobuf(choiceArg.toProtobuf()))
        .controllers()
        .values()
        .map<api.LfValueParty>(party => new api.LfValueParty(party)),
    ).toProtobuf(),
  );
}

export function SimpleTemplate_increment_choice_exercise(
  contractArg: internal.ByteString,
  choiceArg: internal.ByteString,
): internal.ByteString {
  let template = new SimpleTemplateCompanion().fromLfValue<SimpleTemplate>(
    api.LfValue.fromProtobuf(contractArg.toProtobuf()),
  );
  return template
    .choices()
    .get("SimpleTemplate_increment")
    .apply(api.LfValue.fromProtobuf(choiceArg.toProtobuf()))
    .exercise<api.Contract<SimpleTemplate>>()
    .toByteString();
}
