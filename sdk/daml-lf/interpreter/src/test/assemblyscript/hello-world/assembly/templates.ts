// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as api from "./ledger/api";

class SimpleTemplate_increment extends api.Choice<api.LfValueContractId> {
  private owner: api.LfValueParty;
  private count: api.LfValueInt;
  private n: api.LfValueInt;

  constructor(
    owner: api.LfValueParty,
    count: api.LfValueInt,
    n: api.LfValueInt,
  ) {
    super(n);
    this.owner = owner;
    this.count = count;
    this.n = n;
  }

  exercise(): api.LfValueContractId {
    api.logInfo(
      `called AssemblyScript SimpleTemplate_increment(${n.value()}) with count = ${count.value()}`,
    );

    return new SimpleTemplate(
      owner,
      new api.LfValueInt(count.value() + n.value()),
    ).create<SimpleTemplate>();
  }
}

class SimpleTemplate_increment_closure extends api.ConsumingChoice<SimpleTemplate_increment> {
  private owner: api.LfValueParty;
  private count: api.LfValueInt;

  constructor(owner: api.LfValueParty, count: api.LfValueInt) {
    this.owner = owner;
    this.count = count;
  }

  apply(n: api.LfValueInt): SimpleTemplate_increment {
    return new SimpleTemplate_increment(owner, count, n);
  }
}

export class SimpleTemplate extends api.Template<SimpleTemplate> {
  private owner: string;
  private count: i64;

  constructor(owner: string, count: i64) {
    super(
      api.LfValueMap(
        new Map()
          .set("owner", new api.LfValueParty(owner))
          .set("count", new api.LfValueInt(count)),
      ),
    );
    this.owner = owner;
    this.count = count;
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
    if (isMap(arg) && arg.map.entries.length == 2) {
      let owner = arg.map.entries[0];
      let count = arg.map.entries[1];
      if (owner.key.text != "owner" || !isParty(owner.value)) {
        return false;
      }
      if (count.key.text != "count" || !isInt64(count.value)) {
        return false;
      }
      return true;
    } else {
      return false;
    }
  }

  signatories(): Set<api.LfValueParty> {
    return new Set().add(owner);
  }

  choices(): Map<string, SimpleTemplate_increment_closure> {
    return super
      .choices()
      .set(
        "SimpleTemplate_increment",
        new SimpleTemplate_increment_partial(owner, count),
      );
  }
}

// The following are code generated export functions

export function SimpleTemplate_precond(
  arg: internal.ByteString,
): internal.ByteString {
  let contractArg = LfValue.fromProtobuf(contractArg.toProtobuf());
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
    new api.LfValueSet(template.signatories()).toProtobuf(),
  );
}

export function SimpleTemplate_observers(
  contractArg: internal.ByteString,
): internal.ByteString {
  let template = SimpleTemplate.fromLfValue(
    LfValue.fromProtobuf(contractArg.toProtobuf()),
  );
  return internal.ByteString.fromProtobuf(
    new api.LfValueSet(template.observers()).toProtobuf(),
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
    new api.LfValueSet(
      template
        .choices()
        .get("SimpleTemplate_increment")
        .apply(LfValue.fromProtobuf(choiceArg.toProtobuf()))
        .controllers(),
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
    new api.LfValueSet(
      template
        .choices()
        .get("SimpleTemplate_increment")
        .apply(LfValue.fromProtobuf(choiceArg.toProtobuf()))
        .observers(),
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
    new api.LfValueSet(
      template
        .choices()
        .get("SimpleTemplate_increment")
        .apply(LfValue.fromProtobuf(choiceArg.toProtobuf()))
        .authorizers(),
    ).toProtobuf(),
  );
}

// TODO: is the following actually needed?
export function SimpleTemplate_increment_choice_exercise(
  contractArg: internal.ByteString,
  choiceArg: internal.ByteString,
): internal.ByteString {
  let template = SimpleTemplate.fromLfValue(
    LfValue.fromProtobuf(contractArg.toProtobuf()),
  );
  return internal.ByteString.fromProtobuf(
    template
      .choices()
      .get("SimpleTemplate_increment")
      .apply(LfValue.fromProtobuf(choiceArg.toProtobuf()))
      .exercise()
      .toProtobuf(),
  );
}
