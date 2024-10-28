// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as internal from "./internal";
import * as proto from "../../protobuf/com/digitalasset/daml/lf/value/Value";
import * as protoList from "../../protobuf/com/digitalasset/daml/lf/value/Value/List";
import * as protoMap from "../../protobuf/com/digitalasset/daml/lf/value/Value/Map";
import * as protoMapEntry from "../../protobuf/com/digitalasset/daml/lf/value/Value/Map/Entry";
import * as protoOptional from "../../protobuf/com/digitalasset/daml/lf/value/Value/Optional";
import * as protoRecord from "../../protobuf/com/digitalasset/daml/lf/value/Value/Record";
import * as protoRecordField from "../../protobuf/com/digitalasset/daml/lf/value/Value/Record/Field";
import * as protoIdentifier from "../../protobuf/com/digitalasset/daml/lf/value/Identifier";
import { Empty } from "../../protobuf/google/protobuf/Empty";

export function logInfo(msg: string): void {
  let msgByteStr = internal.ByteString.fromString(msg);
  msgByteStr.alloc();
  internal.logInfo(msgByteStr.heapPtr());
  msgByteStr.dealloc();
}

export class LfIdentifier {
  private _packageId: string;
  private _module: string;
  private _name: string;

  constructor(module: string, name: string, packageId: string = "()") {
    this._packageId = packageId;
    this._module = module;
    this._name = name;
  }

  static fromProtobuf(value: protoIdentifier.Identifier): LfIdentifier {
    return new LfIdentifier(
      value.packageId,
      value.moduleName.join("."),
      value.name.join("."),
    );
  }

  toProtobuf(): protoIdentifier.Identifier {
    return new protoIdentifier.Identifier(
      this._packageId,
      this._module.split("."),
      this._name.split("."),
    );
  }

  toString(): string {
    return `${this._packageId}:${this.module}:${this.name}`;
  }
}

export class LfValue {
  static fromProtobuf(value: proto.Value): LfValue {
    throw new Error("Unimplemented");
  }

  toProtobuf(): proto.Value {
    throw new Error("Unimplemented");
  }

  toString(): string {
    throw new Error("Unimplemented");
  }
}

export class LfValueUnit extends LfValue {
  static fromProtobuf(value: proto.Value): LfValueUnit {
    if (isUnit(value)) {
      return new LfValueUnit();
    } else {
      throw new Error(`${protoValueToString(value)} is not a unit value`);
    }
  }

  value(): Empty {
    return new Empty();
  }

  toProtobuf(): proto.Value {
    let result = new proto.Value();
    result.unit = new Empty();
    return result;
  }

  toString(): string {
    return "()";
  }
}

export class LfValueBool extends LfValue {
  private _value: bool;

  constructor(value: bool) {
    super();
    this._value = value;
  }

  static fromProtobuf(value: proto.Value): LfValueBool {
    if (isBool(value)) {
      return new LfValueBool(value.bool);
    } else {
      throw new Error(`${protoValueToString(value)} is not a boolean value`);
    }
  }

  value(): bool {
    return this._value;
  }

  toProtobuf(): proto.Value {
    let result = new proto.Value();
    result.bool = this._value;
    return result;
  }

  toString(): string {
    return `${this.value()}`;
  }
}

export class LfValueInt extends LfValue {
  private _value: i64;

  constructor(value: i64) {
    super();
    this._value = value;
  }

  static fromProtobuf(value: proto.Value): LfValueInt {
    if (isInt64(value)) {
      return new LfValueInt(value.int64);
    } else {
      throw new Error(`${protoValueToString(value)} is not a integer value`);
    }
  }

  value(): i64 {
    return this._value;
  }

  toProtobuf(): proto.Value {
    let result = new proto.Value();
    result.int64 = this._value;
    return result;
  }

  toString(): string {
    return `${this.value()}`;
  }
}

export class LfValueParty extends LfValue {
  private _party: string;

  constructor(party: string) {
    super();
    this._party = party;
  }

  static fromProtobuf(value: proto.Value): LfValueParty {
    if (isParty(value)) {
      return new LfValueParty(value.party);
    } else {
      throw new Error(`${protoValueToString(value)} is not a party value`);
    }
  }

  value(): string {
    return this._party;
  }

  toProtobuf(): proto.Value {
    let result = new proto.Value();
    result.party = this._party;
    return result;
  }

  toString(): string {
    return `${this.value()}`;
  }
}

export class LfValueString extends LfValue {
  private _value: string;

  constructor(value: string) {
    super();
    this._value = value;
  }

  static fromProtobuf(value: proto.Value): LfValueString {
    if (isText(value)) {
      return new LfValueString(value.text);
    } else {
      throw new Error(`${protoValueToString(value)} is not a string value`);
    }
  }

  value(): string {
    return this._value;
  }

  toProtobuf(): proto.Value {
    let result = new proto.Value();
    result.text = this._value;
    return result;
  }

  toString(): string {
    return `${this.value()}`;
  }
}

export class LfValueContractId extends LfValue {
  private _contractId: Uint8Array;

  constructor(contractId: string) {
    super();
    this._contractId = Uint8Array.wrap(String.UTF8.encode(contractId)); // FIXME: need to perform the hex code conversion!
  }

  static fromProtobuf(value: proto.Value): LfValueContractId {
    if (isContractId(value)) {
      return new LfValueContractId(String.UTF8.decode(value.contractId.buffer)); // FIXME:
    } else {
      throw new Error(
        `${protoValueToString(value)} is not a contract ID value`,
      );
    }
  }

  value(): string {
    return String.UTF8.decode(this._contractId.buffer); // FIXME: need to return hex encoded string here!
  }

  toProtobuf(): proto.Value {
    let result = new proto.Value();
    result.contractId = this._contractId;
    return result;
  }

  toString(): string {
    return `${this.value()}`;
  }
}

export class LfValueOptional extends LfValue {
  private _value: LfValue | null;

  constructor(value: LfValue | null) {
    super();
    this._value = value;
  }

  static fromProtobuf(value: proto.Value): LfValueOptional {
    if (isOptional(value)) {
      let optValue =
        value.optional.value == null
          ? null
          : LfValue.fromProtobuf(value.optional.value);

      return new LfValueOptional(optValue);
    } else {
      throw new Error(`${protoValueToString(value)} is not an optional value`);
    }
  }

  value(): LfValue | null {
    return this._value;
  }

  toProtobuf(): proto.Value {
    let optValue = this._value == null ? null : this._value.toProtobuf();
    let result = new proto.Value();
    result.optional = new protoOptional.Optional(optValue);
    return result;
  }

  toString(): string {
    return `${this.value()}`;
  }
}

export class LfValueSet<V extends LfValue> extends LfValue {
  private _value: Set<V>;

  constructor(value: Set<V> = new Set<V>()) {
    super();
    this._value = value;
  }

  static fromArray<V extends LfValue>(value: Array<V>): LfValueSet<V> {
    let values = new Set<V>();

    for (let i = 0; i < value.length; i++) {
      values.add(value[i]);
    }

    return new LfValueSet<V>(values);
  }

  static fromProtobuf<V extends LfValue>(value: proto.Value): LfValueSet<V> {
    if (isList(value)) {
      let values = new Set<V>();

      value.list.elements.forEach(v => values.add(LfValue.fromProtobuf(v)));

      return new LfValueSet<V>(values);
    } else {
      throw new Error(`${protoValueToString(value)} is not a set value`);
    }
  }

  value(): Set<V> {
    return this._value;
  }

  toProtobuf(): proto.Value {
    let result = new proto.Value();
    result.list = new protoList.List(
      this._value.values().map<proto.Value>(v => v.toProtobuf()),
    );
    return result;
  }

  toString(): string {
    return `${this.value()}`;
  }
}

export class LfValueMap extends LfValue {
  private _value: Map<LfValue, LfValue>;

  constructor(value: Map<LfValue, LfValue> = new Map<LfValue, LfValue>()) {
    super();
    this._value = value;
  }

  static fromProtobuf(value: proto.Value): LfValueMap {
    if (isMap(value)) {
      let map = new Map<LfValue, LfValue>();

      value.map.entries.forEach(e =>
        map.set(LfValue.fromProtobuf(e.key), LfValue.fromProtobuf(e.value)),
      );

      return new LfValueMap(map);
    } else {
      throw new Error(`${protoValueToString(value)} is not a map value`);
    }
  }

  value(): Map<LfValue, LfValue> {
    return this._value;
  }

  toProtobuf(): proto.Value {
    let result = new proto.Value();
    result.map = new protoMap.Map(
      this._value
        .keys()
        .map<protoMapEntry.Entry>(
          k =>
            new protoMapEntry.Entry(k.toProtobuf(), _value.get(k).toProtobuf()),
        ),
    );
    return result;
  }

  toString(): string {
    return `${this.value()}`;
  }
}

export class LfValueRecord extends LfValue {
  private _value: Map<string, LfValue>;

  constructor(value: Map<string, LfValue>) {
    super();
    this._value = value;
  }

  static fromProtobuf(value: proto.Value): LfValueRecord {
    if (isRecord(value)) {
      let record = new Map<string, LfValue>();

      value.record.fields.forEach((f, i) =>
        record.set(`_${i + 1}`, LfValue.fromProtobuf(f.value)),
      );

      return new LfValueRecord(record);
    } else {
      throw new Error(`${protoValueToString(value)} is not a record value`);
    }
  }

  value(): Map<string, LfValue> {
    return this._value;
  }

  toProtobuf(): proto.Value {
    let result = new proto.Value();
    result.record = new protoRecord.Record(
      this._value
        .values()
        .map<protoRecordField.Field>(
          v => new protoRecordField.Field(v.toProtobuf()),
        ),
    );
    return result;
  }

  toString(): string {
    return `${this.value()}`;
  }
}

export class Choice {
  private _contractArg: LfValue;
  private _choiceArg: LfValue;

  constructor(contractArg: LfValue, choiceArg: LfValue = new LfValueUnit()) {
    this._contractArg = contractArg;
    this._choiceArg = choiceArg;
  }

  arg(): LfValue {
    return this._arg;
  }

  controllers(): Set<string> {
    return new Set<string>();
  }

  observers(): Set<string> {
    return new Set<string>();
  }

  authorizers(): Set<string> {
    return new Set<string>();
  }

  exercise<R extends internal.ToByteString>(): R {
    throw new Error("Unimplemented");
  }
}

export class ChoiceClosure {
  private _contractArg: LfValue;

  constructor(contractArg: LfValue) {
    this._contractArg = contractArg;
  }

  consuming(): bool {
    throw new Error("Unimplemented");
  }

  apply<A>(choiceArg: A): Choice {
    throw new Error("Unimplemented");
  }
}

export class ConsumingChoice extends ChoiceClosure {
  constructor(contractArg: LfValue) {
    super(contractArg);
  }

  consuming(): bool {
    return true;
  }
}

export class NonConsumingChoice<R> extends ChoiceClosure {
  constructor(contractArg: LfValue) {
    super(contractArg);
  }

  consuming(): bool {
    return false;
  }
}

export class Template {
  private _companion: TemplateCompanion;
  private _arg: LfValue;

  constructor(companion: TemplateCompanion, arg: LfValue) {
    this._companion = companion;
    this._arg = arg;
  }

  arg(): LfValue {
    return this._arg;
  }

  save<T extends Template>(): Contract<T> {
    let templateIdByteStr = internal.ByteString.fromProtobufIdentifier(
      this._companion.templateId().toProtobuf(),
    );
    let argByteStr = internal.ByteString.fromProtobuf(this.arg().toProtobuf());
    templateIdByteStr.alloc();
    argByteStr.alloc();
    let contractId = LfValueContractId.fromProtobuf(
      internal.ByteString.fromI32(
        internal.createContract(
          templateIdByteStr.heapPtr(),
          argByteStr.heapPtr(),
        ),
      ).toProtobuf(),
    );
    argByteStr.dealloc();
    templateIdByteStr.dealloc();

    return new Contract<T>(this._companion, contractId);
  }

  precond(): bool {
    return true;
  }

  signatories(): Set<string> {
    throw new Error("Unimplemented");
  }

  observers(): Set<string> {
    return new Set<string>();
  }

  choices(): Map<string, ChoiceClosure> {
    return new Map<string, ChoiceClosure>();
  }
}

export class TemplateCompanion {
  templateId(): LfIdentifier {
    throw new Error("Unimplemented");
  }

  isValidArg(arg: LfValue): bool {
    throw new Error("Unimplemented");
  }

  toLfValue<T extends Template>(arg: T): LfValue {
    throw new Error("Unimplemented");
  }

  fromLfValue<T extends Template>(arg: LfValue): T {
    throw new Error("Unimplemented");
  }
}

export class Contract<T extends Template> implements internal.ToByteString {
  private _contractId: LfValueContractId;
  private _companion: TemplateCompanion;

  constructor(companion: TemplateCompanion, contractId: LfValueContractId) {
    this._contractId = contractId;
    this._companion = companion;
  }

  contractId(): LfValueContractId {
    return this._contractId;
  }

  toByteString(): internal.ByteString {
    return internal.ByteString.fromProtobuf(this._contractId.toProtobuf());
  }

  fetch(): T {
    let templateIdByteStr = internal.ByteString.fromProtobufIdentifier(
      this._companion.templateId().toProtobuf(),
    );
    let contractIdByteStr = internal.ByteString.fromProtobuf(
      this.contractId().toProtobuf(),
    );
    templateIdByteStr.alloc();
    contractIdByteStr.alloc();
    let arg = LfValue.fromProtobuf(
      internal.ByteString.fromI32(
        internal.fetchContractArg(templateIdByteStr, contractIdByteStr),
      ).toProtobuf(),
    );
    contractIdByteStr.dealloc();
    templateIdByteStr.dealloc();

    return this._companion.fromLfValue(arg);
  }

  exercise(choiceName: string, arg: LfValue): LfValue {
    let templateIdByteStr = internal.ByteString.fromProtobufIdentifier(
      this._companion.templateId().toProtobuf(),
    );
    let contractIdByteStr = internal.ByteString.fromProtobuf(
      this.contractId().toProtobuf(),
    );
    let choiceNameByteStr = internal.ByteString.fromString(choiceName);
    let argByteStr = internal.ByteString.fromProtobuf(arg.toProtobuf());
    templateIdByteStr.alloc();
    contractIdByteStr.alloc();
    choiceNameByteStr.alloc();
    argByteStr.alloc();
    let result = LfValue.fromProtobuf(
      internal.ByteString.fromI32(
        internal.exerciseContract(
          templateIdByteStr,
          contractIdByteStr,
          choiceNameByteStr,
          argByteStr,
        ),
      ).toProtobuf(),
    );
    argByteStr.dealloc();
    choiceNameByteStr.dealloc();
    contractIdByteStr.dealloc();
    templateIdByteStr.dealloc();

    return result;
  }
}

export function isUnit(value: proto.Value): bool {
  return (
    value.unit == Empty &&
    value.bool == false &&
    value.int64 == 0 &&
    value.date == 0 &&
    value.timestamp == 0 &&
    value.numeric == "" &&
    value.party == "" &&
    value.text == "" &&
    value.contractId.length == 0 &&
    value.optional == null &&
    value.list == null &&
    value.map == null &&
    value.textMap == null &&
    value.record == null &&
    value.enum == null
  );
}

export function isBool(value: proto.Value): bool {
  return (
    value.unit == null &&
    value.int64 == 0 &&
    value.date == 0 &&
    value.timestamp == 0 &&
    value.numeric == "" &&
    value.party == "" &&
    value.text == "" &&
    value.contractId.length == 0 &&
    value.optional == null &&
    value.list == null &&
    value.map == null &&
    value.textMap == null &&
    value.record == null &&
    value.enum == null
  );
}

export function isInt64(value: proto.Value): bool {
  return (
    value.unit == null &&
    value.bool == false &&
    value.date == 0 &&
    value.timestamp == 0 &&
    value.numeric == "" &&
    value.party == "" &&
    value.text == "" &&
    value.contractId.length == 0 &&
    value.optional == null &&
    value.list == null &&
    value.map == null &&
    value.textMap == null &&
    value.record == null &&
    value.enum == null
  );
}

export function isParty(value: proto.Value): bool {
  return (
    value.unit == null &&
    value.bool == false &&
    value.int64 == 0 &&
    value.date == 0 &&
    value.timestamp == 0 &&
    value.numeric == "" &&
    value.party != "" &&
    value.text == "" &&
    value.contractId.length == 0 &&
    value.optional == null &&
    value.list == null &&
    value.map == null &&
    value.textMap == null &&
    value.record == null &&
    value.enum == null
  );
}

export function isText(value: proto.Value): bool {
  return (
    value.unit == null &&
    value.bool == false &&
    value.int64 == 0 &&
    value.date == 0 &&
    value.timestamp == 0 &&
    value.numeric == "" &&
    value.party == "" &&
    value.contractId.length == 0 &&
    value.optional == null &&
    value.list == null &&
    value.map == null &&
    value.textMap == null &&
    value.record == null &&
    value.enum == null
  );
}

export function isContractId(value: proto.Value): bool {
  return (
    value.unit == null &&
    value.bool == false &&
    value.int64 == 0 &&
    value.date == 0 &&
    value.timestamp == 0 &&
    value.numeric == "" &&
    value.party == "" &&
    value.text == "" &&
    value.contractId.length != 0 &&
    value.optional == null &&
    value.list == null &&
    value.map == null &&
    value.textMap == null &&
    value.record == null &&
    value.enum == null
  );
}

export function isOptional(value: proto.Value): bool {
  return (
    value.unit == null &&
    value.bool == false &&
    value.int64 == 0 &&
    value.date == 0 &&
    value.timestamp == 0 &&
    value.numeric == "" &&
    value.party == "" &&
    value.text == "" &&
    value.contractId.length == 0 &&
    value.optional != null &&
    value.list == null &&
    value.map == null &&
    value.textMap == null &&
    value.record == null &&
    value.enum == null
  );
}

export function isList(value: proto.Value): bool {
  return (
    value.unit == null &&
    value.bool == false &&
    value.int64 == 0 &&
    value.date == 0 &&
    value.timestamp == 0 &&
    value.numeric == "" &&
    value.party == "" &&
    value.text == "" &&
    value.contractId.length == 0 &&
    value.optional == null &&
    value.list != null &&
    value.map == null &&
    value.textMap == null &&
    value.record == null &&
    value.enum == null
  );
}

export function isMap(value: proto.Value): bool {
  return (
    value.unit == null &&
    value.bool == false &&
    value.int64 == 0 &&
    value.date == 0 &&
    value.timestamp == 0 &&
    value.numeric == "" &&
    value.party == "" &&
    value.text == "" &&
    value.contractId.length == 0 &&
    value.optional == null &&
    value.list == null &&
    value.map != null &&
    value.textMap == null &&
    value.record == null &&
    value.enum == null
  );
}

export function isRecord(value: proto.Value): bool {
  return (
    value.unit == null &&
    value.bool == false &&
    value.int64 == 0 &&
    value.date == 0 &&
    value.timestamp == 0 &&
    value.numeric == "" &&
    value.party == "" &&
    value.text == "" &&
    value.contractId.length == 0 &&
    value.optional == null &&
    value.list == null &&
    value.map == null &&
    value.textMap == null &&
    value.record != null &&
    value.enum == null
  );
}

function protoValueToString(value: proto.Value): string {
  return String.UTF8.decode(proto.encodeValue(value).buffer);
}
