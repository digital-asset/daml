// Copyright (c) 2019, Digital Asset (Switzerland) GmbH and/or its affiliates.
// All rights reserved.

import * as Moment from 'moment';
import { NonExhaustiveMatch } from '../util'
import { DamlLfDataType, DamlLfIdentifier, DamlLfRecord, DamlLfType, DamlLfVariant } from './DamlLfType';

// --------------------------------------------------------------------------------------------------------------------
// Type definitions
// --------------------------------------------------------------------------------------------------------------------

export interface DamlLfRecordField {
  label: string;
  value: DamlLfValue;
}

export type DamlLfValueText       = { type: 'text', value: string }
export type DamlLfValueInt64      = { type: 'int64', value: string }
export type DamlLfValueDecimal    = { type: 'decimal', value: string }
export type DamlLfValueBool       = { type: 'bool', value: boolean }
export type DamlLfValueContractId = { type: 'contractid', value: string }
export type DamlLfValueTimestamp  = { type: 'timestamp', value: string }
export type DamlLfValueDate       = { type: 'date', value: string }
export type DamlLfValueParty      = { type: 'party', value: string }
export type DamlLfValueUnit       = { type: 'unit' }
export type DamlLfValueOptional   = { type: 'optional', value: DamlLfValue | null }
export type DamlLfValueList       = { type: 'list', value: DamlLfValue[] }
export type DamlLfValueRecord     = { type: 'record', id: DamlLfIdentifier, fields: DamlLfRecordField[] }
export type DamlLfValueVariant    = { type: 'variant', id: DamlLfIdentifier, constructor: string, value: DamlLfValue }
export type DamlLfValueUndefined  = { type: 'undefined' }

export type DamlLfValue
  = DamlLfValueText
  | DamlLfValueInt64
  | DamlLfValueDecimal
  | DamlLfValueBool
  | DamlLfValueContractId
  | DamlLfValueTimestamp
  | DamlLfValueDate
  | DamlLfValueParty
  | DamlLfValueUnit
  | DamlLfValueOptional
  | DamlLfValueList
  | DamlLfValueRecord
  | DamlLfValueVariant
  | DamlLfValueUndefined

// --------------------------------------------------------------------------------------------------------------------
// Constructors
// --------------------------------------------------------------------------------------------------------------------

const valueUndef: DamlLfValueUndefined = { type: 'undefined' };
const valueUnit: DamlLfValueUnit = { type: 'unit' }

export function text(value: string): DamlLfValueText { return { type: 'text', value }}
export function int64(value: string): DamlLfValueInt64 { return { type: 'int64', value }}
export function decimal(value: string): DamlLfValueDecimal { return { type: 'decimal', value }}
export function bool(value: boolean): DamlLfValueBool { return { type: 'bool', value }}
export function contractid(value: string): DamlLfValueContractId { return { type: 'contractid', value }}
export function timestamp(value: string): DamlLfValueTimestamp { return { type: 'timestamp', value }}
export function date(value: string): DamlLfValueDate { return { type: 'date', value }}
export function party(value: string): DamlLfValueParty { return { type: 'party', value }}
export function unit(): DamlLfValueUnit { return valueUnit }
export function optional(value: DamlLfValue | null): DamlLfValueOptional { return { type: 'optional', value }}
export function list(value: DamlLfValue[]): DamlLfValueList { return { type: 'list', value }}
export function record(id: DamlLfIdentifier, fields: DamlLfRecordField[]): DamlLfValueRecord {
  return { type: 'record', id, fields }
}
export function variant(id: DamlLfIdentifier, constructor: string, value: DamlLfValue): DamlLfValueVariant {
  return { type: 'variant', id, constructor, value }
}
export function undef(): DamlLfValueUndefined { return valueUndef }

// --------------------------------------------------------------------------------------------------------------------
// Utility functions
// --------------------------------------------------------------------------------------------------------------------

/** Return the primitive value at the given path. If the path is invalid, returns undef. */
export function evalPath(value: DamlLfValue, path: string[], index: number = 0): string | boolean | {} | undefined {
  const notFound = undefined
  const isLast = index === path.length - 1
  switch (value.type) {
    case 'text':        return isLast ? value.value : notFound;
    case 'int64':       return isLast ? value.value : notFound;
    case 'decimal':     return isLast ? value.value : notFound;
    case 'bool':        return isLast ? value.value : notFound;
    case 'contractid':  return isLast ? value.value : notFound;
    case 'timestamp':   return isLast ? value.value : notFound;
    case 'date':        return isLast ? value.value : notFound;
    case 'party':       return isLast ? value.value : notFound;
    case 'unit':        return isLast ? {} : notFound;
    case 'optional':
      if (isLast) {
        return value;
      } else if (path[index] === 'Some') {
        return value.value !== null ? evalPath(value.value, path, index + 1) : notFound;
      } else if (path[index] === 'None') {
        return value.value === null ? {} : notFound;
      } else {
        return notFound;
      }
    case 'list':
      if (isLast) {
        return value;
      } else {
        const listIndex = parseInt(path[index])
        if (isNaN(listIndex)) {
          return notFound;
        } else if (listIndex < 0 || listIndex >= value.value.length) {
          return notFound;
        } else {
          return value.value[listIndex];
        }
      }
    case 'record':
      if (isLast) {
        return value;
      } else {
        const field = value.fields.filter((f) => f.label === path[index]);
        return field.length === 1 ? evalPath(field[0].value, path, index + 1) : notFound;
      }
    case 'variant':
      if (isLast) {
        return value;
      } else if (path[index] === value.constructor) {
        return evalPath(value.value, path, index + 1);
      } else {
        return notFound;
      }
    case 'undefined': return notFound;
    default: throw new NonExhaustiveMatch(value);
  }
}

/**
 * Returns an initial value for the given type.
 * For most types this is 'undefined', and the user is required to enter a value.
 * For some (e.g., 'unit'), there is a sensible default valuec that doesn't require user input.
 */
export function initialValue(type: DamlLfType): DamlLfValue {
  switch (type.type) {
    case 'typevar': return undef();
    case 'typecon': return undef();
    case 'primitive': switch (type.name) {
      case 'text':        return undef();
      case 'int64':       return undef();
      case 'decimal':     return undef();
      case 'bool':        return undef();
      case 'contractid':  return undef();
      case 'timestamp':   return undef();
      case 'date':        return undef();
      case 'party':       return undef();
      case 'unit':        return unit();
      case 'optional':    return optional(null);
      case 'list':        return list([]);
      default: throw new NonExhaustiveMatch(type.name);
    }
    default: throw new NonExhaustiveMatch(type);
  }
}

type JSON = Object | null;

/** A user-readeable representation of the given value. */
export function toJSON(value: DamlLfValue): JSON {
  switch (value.type) {
    case 'text':        return value.value;
    case 'int64':       return value.value;
    case 'decimal':     return value.value;
    case 'bool':        return value.value;
    case 'contractid':  return value.value;
    case 'timestamp':   return value.value;
    case 'date':        return value.value;
    case 'party':       return value.value;
    case 'unit':        return {};
    case 'optional':    return value.value === null ? null : toJSON(value.value);
    case 'list':        return value.value.map((e) => toJSON(e))
    case 'record':
      const r: {[label: string]: JSON} = {};
      value.fields.forEach((f) => r[f.label] = toJSON(f.value));
      return r;
    case 'variant':
      return {[value.constructor]: toJSON(value.value)}
    case 'undefined': return '???';
    default: throw new NonExhaustiveMatch(value);
  }
}

export function initialDataTypeValue(id: DamlLfIdentifier, dataType: DamlLfRecord): DamlLfValueRecord;
export function initialDataTypeValue(id: DamlLfIdentifier, dataType: DamlLfVariant): DamlLfValueVariant;
export function initialDataTypeValue(id: DamlLfIdentifier, dataType: DamlLfDataType):
  DamlLfValueRecord | DamlLfValueVariant;
export function initialDataTypeValue(id: DamlLfIdentifier, dataType: DamlLfDataType):
  DamlLfValueRecord | DamlLfValueVariant {
  switch (dataType.type) {
    case 'record': return record(id,
      dataType.fields.map((f) => ({label: f.name, value: initialValue(f.value)})));
    case 'variant': return variant(id,
      dataType.fields[0].name, initialValue(dataType.fields[0].value));
    default: throw new NonExhaustiveMatch(dataType);
  }
}

const momentDateFormat = 'YYYY-MM-DD';

export function toMoment(value: DamlLfValue): Moment.Moment | undefined {
  switch (value.type) {
    case 'timestamp':
      // Iso8601 UTC time
      const momentTime = Moment.utc(value.value);
      return momentTime.isValid() ? momentTime : undefined;
    case 'date':
      // Iso8601 local date
      const momentDate = Moment(value.value, momentDateFormat);
      return momentDate.isValid() ? momentDate : undefined;
    default: return undefined;
  }
}

export function fromMoment(value: Moment.Moment, type: 'timestamp'): DamlLfValueTimestamp;
export function fromMoment(value: Moment.Moment, type: 'date'): DamlLfValueDate;
export function fromMoment(value: Moment.Moment, type: 'timestamp' | 'date'): DamlLfValueTimestamp | DamlLfValueDate;
export function fromMoment(value: Moment.Moment, type: 'timestamp' | 'date'): DamlLfValueTimestamp | DamlLfValueDate {
  switch (type) {
    case 'timestamp': return timestamp(value.utc().format());
    case 'date': return date(value.format(momentDateFormat));
    default: throw new NonExhaustiveMatch(type);
  }
}
