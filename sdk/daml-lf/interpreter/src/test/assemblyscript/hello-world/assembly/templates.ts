// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as api from "./ledger/api";

class SimpleTemplate_increment extends api.Choice<
  api.Contract<SimpleTemplate>
> {
  private owner: string;
  private count: i64;
  private n: i64;

  constructor(owner: string, count: i64, n: i64) {
    super(SimpleTemplate.toLfValue(owner, count), toLfValueInt(n));
    this.owner = owner;
    this.count = count;
    this.n = n;
  }

  static toLfValue(n: i64): api.LfValue {
    return new api.LfValueInt(n);
  }

  exercise(): api.Contract<SimpleTemplate> {
    api.logInfo(
      `called AssemblyScript SimpleTemplate_increment(${n}) with count = ${count}`,
    );

    return new SimpleTemplate(owner, count + n).create();
  }
}

class SimpleTemplate_increment_closure extends api.ConsumingChoice<
  api.Contract<SimpleTemplate>
> {
  private owner: string;
  private count: i64;

  constructor(owner: string, count: i64) {
    super(SimpleTemplate.toLfValue(owner, count));
    this.owner = owner;
    this.count = count;
  }

  apply(n: i64): SimpleTemplate_increment {
    return new SimpleTemplate_increment(owner, count, n);
  }
}

export class SimpleTemplate extends api.Template<SimpleTemplate> {
  private owner: string;
  private count: i64;

  constructor(owner: string, count: i64) {
    super(toLfValue(owner, count));
    this.owner = owner;
    this.count = count;
  }

  static toLfValue(owner: string, count: i64): api.LfValue {
    return new api.LfValueRecord(
      new Map()
        .set("owner", new api.LfValueParty(owner))
        .set("count", new api.LfValueInt(count)),
    );
  }

  static fromLfValue(arg: api.LfValue): SimpleTemplate {
    if (isValidArg(arg)) {
      let owner = arg.map.entries[0].value.party;
      let count = arg.map.entries[1].value.int64;
      return new SimpleTemplate(owner, count);
    } else {
      throw new Error(
        `${arg} is an invalid contract argument type for SimpleTemplate`,
      );
    }
  }

  static templateId(): api.LfIdentifier {
    return new api.LfIdentifier(
      (module = "SimpleTemplate"),
      (name = "fromLfValue"),
    );
  }

  static isValidArg(arg: api.LfValue): bool {
    if (isRecord(arg) && arg.record.fields.length == 2) {
      let owner = arg.record.fields[0];
      let count = arg.record.fields[1];
      return isParty(owner.value) && isInt64(count.value);
    } else {
      return false;
    }
  }

  signatories(): Set<string> {
    return new Set().add(owner);
  }

  choices(): Map<string, SimpleTemplate_increment_closure> {
    return super
      .choices()
      .set(
        "SimpleTemplate_increment",
        new SimpleTemplate_increment_closure(owner, count),
      );
  }
}

// The following are code generated export functions

export function SimpleTemplate_precond(
  arg: internal.ByteString,
): internal.ByteString {
  let contractArg = LfValue.fromProtobuf(arg.toProtobuf());
  let template = SimpleTemplate.fromLfValue(contractArg);
  let precondition =
    SimpleTemplate.isValidArg(contractArg) && template.precond();
  return internal.ByteString.fromProtobuf(
    new api.LfValueBool(precondition).toProtobuf(),
  );
}

export function SimpleTemplate_signatories(
  contractArg: internal.ByteString,
): internal.ByteString {
  let template = SimpleTemplate.fromLfValue(
    LfValue.fromProtobuf(contractArg.toProtobuf()),
  );
  return internal.ByteString.fromProtobuf(
    new api.LfValueSet.fromArray(
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
  let template = SimpleTemplate.fromLfValue(
    LfValue.fromProtobuf(contractArg.toProtobuf()),
  );
  return internal.ByteString.fromProtobuf(
    new api.LfValueSet.fromArray(
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
  let template = SimpleTemplate.fromLfValue(
    LfValue.fromProtobuf(contractArg.toProtobuf()),
  );
  return internal.ByteString.fromProtobuf(
    new api.LfValueBool(
      template.choices().get("SimpleTemplate_increment").consuming,
    ).toProtobuf(),
  );
}

export function SimpleTemplate_increment_choice_controllers(
  contractArg: internal.ByteString,
  choiceArg: internal.ByteString,
): internal.ByteString {
  let template = SimpleTemplate.fromLfValue(
    LfValue.fromProtobuf(contractArg.toProtobuf()),
  );
  return internal.ByteString.fromProtobuf(
    new api.LfValueSet.fromArray(
      template
        .choices()
        .get("SimpleTemplate_increment")
        .apply(LfValue.fromProtobuf(choiceArg.toProtobuf()))
        .controllers()
        .values()
        .map<api.LfValueParty>(party => new api.LfValueParty(party)),
    ).toProtobuf(),
  );
}

export function SimpleTemplate_increment_choice_observers(
  contractArg: internal.ByteString,
  choiceArg: internal.ByteString,
): internal.ByteString {
  let template = SimpleTemplate.fromLfValue(
    LfValue.fromProtobuf(contractArg.toProtobuf()),
  );
  return internal.ByteString.fromProtobuf(
    new api.LfValueSet.fromArray(
      template
        .choices()
        .get("SimpleTemplate_increment")
        .apply(LfValue.fromProtobuf(choiceArg.toProtobuf()))
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
  let template = SimpleTemplate.fromLfValue(
    LfValue.fromProtobuf(contractArg.toProtobuf()),
  );
  return internal.ByteString.fromProtobuf(
    new api.LfValueSet.fromArray(
      template
        .choices()
        .get("SimpleTemplate_increment")
        .apply(LfValue.fromProtobuf(choiceArg.toProtobuf()))
        .authorizers()
        .values()
        .map<api.LfValueParty>(party => new api.LfValueParty(party)),
    ).toProtobuf(),
  );
}
