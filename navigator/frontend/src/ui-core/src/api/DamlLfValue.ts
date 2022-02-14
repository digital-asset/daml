// Copyright (c) 2020, Digital Asset (Switzerland) GmbH and/or its affiliates.
// All rights reserved.

import Moment from "moment";
import { NonExhaustiveMatch } from "../util";
import {
  DamlLfEnum,
  DamlLfIdentifier,
  DamlLfPrimType,
  DamlLfRecord,
  DamlLfType,
  DamlLfVariant,
} from "./DamlLfType";

// --------------------------------------------------------------------------------------------------------------------
// Type definitions
// --------------------------------------------------------------------------------------------------------------------

export interface DamlLfRecordField {
  label: string;
  value: DamlLfValue;
}

export type DamlLfValueText = { type: "text"; value: string };
export type DamlLfValueInt64 = { type: "int64"; value: string };
export type DamlLfValueNumeric = { type: "numeric"; value: string };
export type DamlLfValueBool = { type: "bool"; value: boolean };
export type DamlLfValueContractId = { type: "contractid"; value: string };
export type DamlLfValueTimestamp = { type: "timestamp"; value: string };
export type DamlLfValueDate = { type: "date"; value: string };
export type DamlLfValueParty = { type: "party"; value: string };
export type DamlLfValueUnit = { type: "unit" };
export type DamlLfValueOptional = {
  type: "optional";
  value: DamlLfValue | null;
};
export type DamlLfValueList = { type: "list"; value: DamlLfValue[] };
export type DamlLfValueRecord = {
  type: "record";
  id: DamlLfIdentifier;
  fields: DamlLfRecordField[];
};
export type DamlLfValueVariant = {
  type: "variant";
  id: DamlLfIdentifier;
  constructor: string;
  value: DamlLfValue;
};
export type DamlLfValueEnum = {
  type: "enum";
  id: DamlLfIdentifier;
  constructor: string;
};
export type DamlLfValueUndefined = { type: "undefined" };
export type DamlLfValueTextMap = {
  type: "textmap";
  value: DamlLfValueTextMapEntry[];
};
export type DamlLfValueTextMapEntry = { key: string; value: DamlLfValue };
export type DamlLfValueGenMap = {
  type: "genmap";
  value: DamlLfValueGenMapEntry[];
};
export type DamlLfValueGenMapEntry = { key: DamlLfValue; value: DamlLfValue };

export type DamlLfValue =
  | DamlLfValueText
  | DamlLfValueInt64
  | DamlLfValueNumeric
  | DamlLfValueBool
  | DamlLfValueContractId
  | DamlLfValueTimestamp
  | DamlLfValueDate
  | DamlLfValueParty
  | DamlLfValueUnit
  | DamlLfValueOptional
  | DamlLfValueList
  | DamlLfValueTextMap
  | DamlLfValueGenMap
  | DamlLfValueRecord
  | DamlLfValueVariant
  | DamlLfValueEnum
  | DamlLfValueUndefined;

// --------------------------------------------------------------------------------------------------------------------
// Constructors
// --------------------------------------------------------------------------------------------------------------------

const valueUndef: DamlLfValueUndefined = { type: "undefined" };
const valueUnit: DamlLfValueUnit = { type: "unit" };

export function text(value: string): DamlLfValueText {
  return { type: "text", value };
}
export function int64(value: string): DamlLfValueInt64 {
  return { type: "int64", value };
}
export function numeric(value: string): DamlLfValueNumeric {
  return { type: "numeric", value };
}
export function bool(value: boolean): DamlLfValueBool {
  return { type: "bool", value };
}
export function contractid(value: string): DamlLfValueContractId {
  return { type: "contractid", value };
}
export function timestamp(value: string): DamlLfValueTimestamp {
  return { type: "timestamp", value };
}
export function date(value: string): DamlLfValueDate {
  return { type: "date", value };
}
export function party(value: string): DamlLfValueParty {
  return { type: "party", value };
}
export function unit(): DamlLfValueUnit {
  return valueUnit;
}
export function optional(value: DamlLfValue | null): DamlLfValueOptional {
  return { type: "optional", value };
}
export function list(value: DamlLfValue[]): DamlLfValueList {
  return { type: "list", value };
}
export function textmap(value: DamlLfValueTextMapEntry[]): DamlLfValueTextMap {
  return { type: "textmap", value };
}
export function textMapEntry(
  key: string,
  value: DamlLfValue,
): DamlLfValueTextMapEntry {
  return { key, value };
}
export function genmap(value: DamlLfValueGenMapEntry[]): DamlLfValueGenMap {
  return { type: "genmap", value };
}
export function genMapEntry(
  key: DamlLfValue,
  value: DamlLfValue,
): DamlLfValueGenMapEntry {
  return { key, value };
}
export function record(
  id: DamlLfIdentifier,
  fields: DamlLfRecordField[],
): DamlLfValueRecord {
  return { type: "record", id, fields };
}
export function variant(
  id: DamlLfIdentifier,
  constructor: string,
  value: DamlLfValue,
): DamlLfValueVariant {
  return { type: "variant", id, constructor, value };
}
export function enumCon(
  id: DamlLfIdentifier,
  constructor: string,
): DamlLfValueEnum {
  return { type: "enum", id, constructor };
}
export function undef(): DamlLfValueUndefined {
  return valueUndef;
}

// --------------------------------------------------------------------------------------------------------------------
// Utility functions
// --------------------------------------------------------------------------------------------------------------------

/** Return the primitive value at the given path. If the path is invalid, returns undef. */
export function evalPath(
  value: DamlLfValue,
  path: string[],
  index: number = 0,
): string | boolean | {} | undefined {
  const notFound = undefined;
  const isLast = index === path.length - 1;
  switch (value.type) {
    case "text":
      return isLast ? value.value : notFound;
    case "int64":
      return isLast ? value.value : notFound;
    case "numeric":
      return isLast ? value.value : notFound;
    case "bool":
      return isLast ? value.value : notFound;
    case "contractid":
      return isLast ? value.value : notFound;
    case "timestamp":
      return isLast ? value.value : notFound;
    case "date":
      return isLast ? value.value : notFound;
    case "party":
      return isLast ? value.value : notFound;
    case "unit":
      return isLast ? {} : notFound;
    case "optional":
      if (isLast) {
        return value;
      } else if (path[index] === "Some") {
        return value.value !== null
          ? evalPath(value.value, path, index + 1)
          : notFound;
      } else if (path[index] === "None") {
        return value.value === null ? {} : notFound;
      } else {
        return notFound;
      }
    case "list":
      if (isLast) {
        return value;
      } else {
        const listIndex = parseInt(path[index]);
        if (isNaN(listIndex)) {
          return notFound;
        } else if (listIndex < 0 || listIndex >= value.value.length) {
          return notFound;
        } else {
          return evalPath(value.value[listIndex], path, index + 1);
        }
      }
    case "record":
      if (isLast) {
        return value;
      } else {
        const field = value.fields.filter(f => f.label === path[index]);
        return field.length === 1
          ? evalPath(field[0].value, path, index + 1)
          : notFound;
      }
    case "variant":
      if (isLast) {
        return value;
      } else if (path[index] === value.constructor) {
        return evalPath(value.value, path, index + 1);
      } else {
        return notFound;
      }
    case "enum":
      if (isLast) {
        return value;
      } else {
        return notFound;
      }
    case "textmap":
      if (isLast) {
        return value;
      } else {
        const key = path[index];
        if (value.value === null) {
          return notFound;
        } else {
          const fList = value.value.filter(e => e.key === key);
          if (fList.length !== 1) {
            return notFound;
          } else {
            return evalPath(fList[0].value, path, index + 1);
          }
        }
      }
    case "genmap":
      if (isLast) {
        return value;
      } else if (index === path.length - 2) {
        return notFound;
      } else {
        const listIndex = parseInt(path[index]);
        if (isNaN(listIndex)) {
          return notFound;
        } else if (listIndex < 0 || listIndex >= value.value.length) {
          return notFound;
        } else {
          const entry = value.value[listIndex];
          switch (path[index + 1]) {
            case "key":
              return evalPath(entry.key, path, index + 2);
            case "value":
              return evalPath(entry.value, path, index + 2);
            default:
              return notFound;
          }
        }
      }
    case "undefined":
      return notFound;
    default:
      throw new NonExhaustiveMatch(value);
  }
}

/**
 * Returns an initial value for the given type.
 * For most types this is 'undefined', and the user is required to enter a value.
 * For some (e.g., 'unit'), there is a sensible default valuec that doesn't require user input.
 */
export function initialValue(type: DamlLfType): DamlLfValue {
  switch (type.type) {
    case "typevar":
      return undef();
    case "typecon":
      return undef();
    case "numeric":
      return undef();
    case "primitive": {
      const n: DamlLfPrimType = type.name;
      switch (n) {
        case "text":
          return undef();
        case "int64":
          return undef();
        case "bool":
          return bool(false);
        case "contractid":
          return undef();
        case "timestamp":
          return undef();
        case "date":
          return undef();
        case "party":
          return undef();
        case "unit":
          return unit();
        case "optional":
          return optional(null);
        case "list":
          return list([]);
        case "textmap":
          return textmap([]);
        case "genmap":
          return genmap([]);
        default:
          throw new NonExhaustiveMatch(n);
      }
    }
    default:
      throw new NonExhaustiveMatch(type);
  }
}

type JSON = Record<string, unknown> | string | boolean | null | JSON[];

/** A user-readeable representation of the given value. */
export function toJSON(value: DamlLfValue): JSON {
  switch (value.type) {
    case "text":
      return value.value;
    case "int64":
      return value.value;
    case "numeric":
      return value.value;
    case "bool":
      return value.value;
    case "contractid":
      return value.value;
    case "timestamp":
      return value.value;
    case "date":
      return value.value;
    case "party":
      return value.value;
    case "unit":
      return {};
    case "optional":
      return value.value === null ? null : toJSON(value.value);
    case "list":
      return value.value.map(e => toJSON(e));
    case "record": {
      const r: { [label: string]: JSON } = {};
      value.fields.forEach(f => (r[f.label] = toJSON(f.value)));
      return r;
    }
    case "variant":
      return { [value.constructor]: toJSON(value.value) };
    case "enum":
      return value.constructor;
    case "textmap":
      return value.value.map(e => ({ key: e.key, value: toJSON(e.value) }));
    case "genmap":
      return value.value.map(e => ({
        key: toJSON(e.key),
        value: toJSON(e.value),
      }));
    case "undefined":
      return "???";
    default:
      throw new NonExhaustiveMatch(value);
  }
}

export function initialRecordValue(
  id: DamlLfIdentifier,
  dataType: DamlLfRecord,
): DamlLfValueRecord {
  return record(
    id,
    dataType.fields.map(f => ({ label: f.name, value: initialValue(f.value) })),
  );
}
export function initialVariantValue(
  id: DamlLfIdentifier,
  dataType: DamlLfVariant,
): DamlLfValueVariant {
  return variant(
    id,
    dataType.fields[0].name,
    initialValue(dataType.fields[0].value),
  );
}
export function initialEnumValue(
  id: DamlLfIdentifier,
  dataType: DamlLfEnum,
): DamlLfValueEnum {
  return enumCon(id, dataType.constructors[0]);
}

const momentDateFormat = "YYYY-MM-DD";

export function toMoment(value: DamlLfValue): Moment.Moment | undefined {
  switch (value.type) {
    case "timestamp": {
      // Iso8601 UTC time
      const momentTime = Moment.utc(value.value);
      return momentTime.isValid() ? momentTime : undefined;
    }
    case "date": {
      // Iso8601 local date
      const momentDate = Moment(value.value, momentDateFormat);
      return momentDate.isValid() ? momentDate : undefined;
    }
    default:
      return undefined;
  }
}

export function fromMoment(
  value: Moment.Moment,
  type: "timestamp",
): DamlLfValueTimestamp;
export function fromMoment(value: Moment.Moment, type: "date"): DamlLfValueDate;
export function fromMoment(
  value: Moment.Moment,
  type: "timestamp" | "date",
): DamlLfValueTimestamp | DamlLfValueDate;
export function fromMoment(
  value: Moment.Moment,
  type: "timestamp" | "date",
): DamlLfValueTimestamp | DamlLfValueDate {
  switch (type) {
    case "timestamp":
      return timestamp(value.utc().format());
    case "date":
      return date(value.format(momentDateFormat));
    default:
      throw new NonExhaustiveMatch(type);
  }
}
