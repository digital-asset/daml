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
      _packageId,
      _module.split("."),
      _name.split("."),
    );
  }
}

export class LfValue {
  static fromProtobuf(value: proto.Value): LfValue {
    throw new Error("Unimplemented");
  }

  toProtobuf(): proto.Value {
    throw new Error("Unimplemented");
  }
}

export class LfValueUnit extends LfValue {
  static fromProtobuf(value: proto.Value): LfValueUnit {
    if (isUnit(value)) {
      return new LfValueUnit();
    } else {
      throw new Error(`${value} is not a unit value`);
    }
  }

  value(): Empty {
    return new Empty();
  }

  toProtobuf(): proto.Value {
    return new proto.Value((unit = new Empty()));
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
      throw new Error(`${value} is not a boolean value`);
    }
  }

  value(): bool {
    return _value;
  }

  toProtobuf(): proto.Value {
    return new proto.Value((bool = _value));
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
      throw new Error(`${value} is not a integer value`);
    }
  }

  value(): i64 {
    return _value;
  }

  toProtobuf(): proto.Value {
    return new proto.Value((int64 = _value));
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
      throw new Error(`${value} is not a party value`);
    }
  }

  value(): string {
    return _party;
  }

  toProtobuf(): proto.Value {
    return new proto.Value((party = _party));
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
      throw new Error(`${value} is not a string value`);
    }
  }

  value(): string {
    return _value;
  }

  toProtobuf(): proto.Value {
    return new proto.Value((text = _value));
  }
}

export class LfValueContractId extends LfValue {
  private _contractId: Uint8Array;

  constructor(contractId: string) {
    super();
    this._contractId = String.UTF8.encode(contractId); // FIXME: need to perform the hex code conversion!
  }

  static fromProtobuf(value: proto.Value): LfValueContractId {
    if (isContractId(value)) {
      return new LfValueContractId(value.contractId); // FIXME:
    } else {
      throw new Error(`${value} is not a contract ID value`);
    }
  }

  value(): string {
    return _contractId; // FIXME: need to return hex encoded string here!
  }

  toProtobuf(): proto.Value {
    return new proto.Value((contractId = _contractId));
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
      throw new Error(`${value} is not an optional value`);
    }
  }

  value(): LfValue | null {
    return _value;
  }

  toProtobuf(): proto.Value {
    let optValue = _value == null ? null : _value.toProtobuf();

    return new proto.Value((optional = new protoOptional.Optional(optValue)));
  }
}

export class LfValueSet extends LfValue {
  private _value: Set<LfValue>;

  constructor(value: Set<LfValue> = new Set<lfValue>()) {
    super();
    this._value = value;
  }

  static fromArray(value: Array<LfValue>): LfValueSet {
    let values = new Set<LfValue>();

    value.forEach(v => values.add(v));

    return new LfValueSet(values);
  }

  static fromProtobuf(value: proto.Value): LfValueSet {
    if (isList(value)) {
      let values = new Set<LfValue>();

      value.list.elements.forEach(v => values.add(LfValue.fromProtobuf(v)));

      return new LfValueSet(values);
    } else {
      throw new Error(`${value} is not a set value`);
    }
  }

  value(): Set<LfValue> {
    return _value;
  }

  toProtobuf(): proto.Value {
    return new proto.Value(
      (list = new protoList.List(
        _value.values().map<proto.Value>(v => v.toProtobuf()),
      )),
    );
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
      throw new Error(`${value} is not a map value`);
    }
  }

  value(): Map<LfValue, LfValue> {
    return _value;
  }

  toProtobuf(): proto.Value {
    return new proto.Value(
      (map = new protoMap.Map(
        _value
          .keys()
          .map<proto.Value>(
            k =>
              new protoMapEntry.Entry(
                k.toProtobuf(),
                _value.get(k).toProtobuf(),
              ),
          ),
      )),
    );
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
      throw new Error(`${value} is not a record value`);
    }
  }

  value(): Map<string, LfValue> {
    return _value;
  }

  toProtobuf(): proto.Value {
    return new proto.Value(
      (record = new protoRecord.Record(
        _value
          .values()
          .map<proto.Value>(v => new protoRecordField.Field(v.toProtobuf())),
      )),
    );
  }
}

export class Choice<R> {
  private _contractArg: LfValue;
  private _choiceArg: LfValue;

  constructor(contractArg: LfValue, choiceArg: LfValue = new LfValueUnit()) {
    this._contractArg = contractArg;
    this._choiceArg = choiceArg;
  }

  arg(): LfValue {
    return _arg;
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

  exercise(): R {
    throw new Error("Unimplemented");
  }
}

export class ChoiceClosure<R> {
  private _contractArg: LfValue;

  static consuming: bool;

  constructor(contractArg: LfValue) {
    this._contractArg = contractArg;
  }

  apply(choiceArg: LfValue): Choice<R> {
    throw new Error("Unimplemented");
  }
}

export class ConsumingChoice<R> extends ChoiceClosure<R> {
  static consuming: bool = true;

  constructor(contractArg: LfValue) {
    super(contractArg);
  }
}

export class NonConsumingChoice<R> extends ChoiceClosure<R> {
  static consuming: bool = false;

  constructor(contractArg: LfValue) {
    super(contractArg);
  }
}

export class Template<T> {
  private _arg: LfValue;

  static fromLfValue(arg: LfValue): Template<T> {
    throw new Error("Unimplemented");
  }

  static isValidArg(arg: LfValue): bool {
    throw new Error("Unimplemented");
  }

  static templateId(): LfIdentifier {
    throw new Error("Unimplemented");
  }

  arg(): LfValue {
    return _arg;
  }

  create(): Contract<T> {
    let templateIdByteStr = internal.ByteString.fromProtobufIdentifier(
      T.templateId().toProtobuf(),
    );
    let argByteStr = internal.ByteString.fromProtobuf(arg().toProtobuf());
    templateIdByteStr.alloc();
    argByteStr.alloc();
    let contractId = LfValue.fromProtobuf(
      internal
        .createContract(templateIdByteStr.heapPtr(), argByteStr.heapPtr())
        .toProtobuf(),
    );
    argByteStr.dealloc();
    templateIdByteStr.dealloc();

    return new Contract<T>(contractId);
  }

  precond(): bool {
    return true;
  }

  signatories(): Set<string> {
    throw new Error("Unimplemented");
  }

  observers(): Set<string> {
    return new Set<LfValueParty>();
  }

  choices(): Map<string, ChoiceClosure<LfValue>> {
    return new Map<string, ChoiceClosure<LfValue>>();
  }
}

export class Contract<T> {
  private _contractId: LfValueContractId;

  constructor(contractId: LfValueContractId) {
    this._contractId = contractId;
  }

  contractId(): LfValueContractId {
    return _contractId;
  }

  fetch(): T {
    let templateIdByteStr = internal.ByteString.fromProtobufIdentifier(
      T.templateId().toProtobuf(),
    );
    let contractIdByteStr = internal.ByteString.fromProtobuf(
      contractId().toProtobuf(),
    );
    templateIdByteStr.alloc();
    contractIdByteStr.alloc();
    let arg = LfValue.fromProtobuf(
      internal
        .fetchContractArg(templateIdByteStr, contractIdByteStr)
        .toProtobuf(),
    );
    contractIdByteStr.dealloc();
    templateIdByteStr.dealloc();

    return T.fromLfValue(arg);
  }

  exercise(choiceName: string, arg: LfValue): LfValue {
    let templateIdByteStr = internal.ByteString.fromProtobufIdentifier(
      T.templateId().toProtobuf(),
    );
    let contractIdByteStr = internal.ByteString.fromProtobuf(
      contractId().toProtobuf(),
    );
    let choiceNameByteStr = internal.ByteString.fromString(choiceName);
    let argByteStr = internal.ByteString.fromProtobuf(arg.toProtobuf());
    templateIdByteStr.alloc();
    contractIdByteStr.alloc();
    choiceNameByteStr.alloc();
    argByteStr.alloc();
    let result = LfValue.fromProtobuf(
      internal
        .exerciseContract(
          templateIdByteStr,
          contractIdByteStr,
          choiceNameByteStr,
          argByteStr,
        )
        .toProtobuf(),
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
