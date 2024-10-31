// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as api from "./ledger/api";

class SimpleTemplate_increment extends api.Choice {
  private owner: string;
  private count: i64;
  private n: i64;

  constructor(contractArg: api.LfValue, choiceArg: api.LfValue) {
    super(contractArg, choiceArg);
    let lfContractArg = SimpleTemplate.fromLfValue(contractArg);
    let lfChoiceArg = SimpleTemplate_increment.fromLfValue(choiceArg);
    let owner = lfContractArg.value().keys()[0];
    let count = lfContractArg.value().keys()[1];
    this.owner = (lfContractArg.value().get(owner) as api.LfValueParty).value();
    this.count = (lfContractArg.value().get(count) as api.LfValueInt).value();
    this.n = lfChoiceArg.value();
  }

  static create(owner: string, count: i64, n: i64): SimpleTemplate_increment {
    return new SimpleTemplate_increment(
      SimpleTemplate.toLfValue(owner, count),
      SimpleTemplate_increment.toLfValue(n),
    );
  }

  static toLfValue(n: i64): api.LfValue {
    return new api.LfValueInt(n);
  }

  static fromLfValue(arg: api.LfValue): api.LfValueInt {
    if (SimpleTemplate_increment.isValidArg(arg)) {
      return arg as api.LfValueInt;
    } else {
      throw new Error(
        `${arg} is an invalid argument type for choice SimpleTemplate_increment`,
      );
    }
  }

  static isValidArg(arg: api.LfValue): bool {
    return arg instanceof api.LfValueInt;
  }

  exercise(): api.Contract<SimpleTemplate> {
    api.logInfo(
      `called AssemblyScript SimpleTemplate_increment(${n}) with count = ${count}`,
    );

    return new SimpleTemplate(owner, count + n).create();
  }
}

class SimpleTemplate_increment_closure extends api.ConsumingChoice {
  private owner: string;
  private count: i64;

  constructor(arg: api.LfValue) {
    super(arg);
    let template = new SimpleTemplateCompanion().fromLfValue<SimpleTemplate>(
      arg,
    );
    this.owner = template.owner;
    this.count = template.count;
  }

  static create(owner: string, count: i64): SimpleTemplate_increment_closure {
    let template = SimpleTemplate.create(owner, count);
    return new SimpleTemplate_increment_closure(
      template._companion.toLfValue(template),
    );
  }

  apply<R>(n: R): api.Choice {
    if (isInteger<R>()) {
      return SimpleTemplate_increment.create(this.owner, this.count, n as i64);
    } else {
      return super.apply<R>(n);
    }
  }
}

export class SimpleTemplate extends api.Template {
  private owner: string;
  private count: i64;

  constructor(companion: api.TemplateCompanion, arg: api.LfValue) {
    super(companion, arg);
    let template = companion.fromLfValue<SimpleTemplate>(arg);
    this.owner = template.owner;
    this.count = template.count;
  }

  static create(owner: string, count: i64): SimpleTemplate {
    let lfArg = new api.LfValueRecord(
      new Map<string, api.LfValue>()
        .set("owner", new api.LfValueParty(owner))
        .set("count", new api.LfValueInt(count)),
    );
    return new SimpleTemplate(new SimpleTemplateCompanion(), lfArg);
  }

  signatories(): Set<string> {
    return new Set<string>().add(this.owner);
  }

  choices(): Map<string, api.ChoiceClosure> {
    return super
      .choices()
      .set(
        "SimpleTemplate_increment",
        SimpleTemplate_increment_closure.create(this.owner, this.count),
      );
  }
}

export class SimpleTemplateCompanion extends api.TemplateCompanion {
  templateId(): api.LfIdentifier {
    return new api.LfIdentifier("SimpleTemplate", "fromLfValue");
  }

  toLfValue<T extends Template>(template: T): api.LfValue {
    if (template instanceof SimpleTemplate) {
      return new api.LfValueRecord(
        new Map<string, api.LfValue>()
          .set("owner", new api.LfValueParty(template.owner))
          .set("count", new api.LfValueInt(template.count)),
      );
    } else {
      return super.toLfValue<T>(template);
    }
  }

  fromLfValue<T extends Template>(value: api.LfValue): T {
    if (this.isValidArg(value)) {
      return new SimpleTemplate(this, value);
    } else {
      throw new Error(
        `${value} is an invalid contract argument type for SimpleTemplate`,
      );
    }
  }

  isValidArg(arg: api.LfValue): bool {
    if (
      arg instanceof api.LfValueRecord &&
      (arg as api.LfValueRecord).value().keys().length == 2
    ) {
      let owner = (arg as api.LfValueRecord).value().keys()[0];
      let count = (arg as api.LfValueRecord).value().keys()[1];
      return (
        (arg as api.LfValueRecord).value().get(owner) instanceof
          api.LfValueParty &&
        (arg as api.LfValueRecord).value().get(count) instanceof api.LfValueInt
      );
    } else {
      return false;
    }
  }
}
