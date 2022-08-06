import * as jspb from 'google-protobuf'

import * as google_protobuf_empty_pb from 'google-protobuf/google/protobuf/empty_pb';


export class Value extends jspb.Message {
  getRecord(): Record | undefined;
  setRecord(value?: Record): Value;
  hasRecord(): boolean;
  clearRecord(): Value;

  getVariant(): Variant | undefined;
  setVariant(value?: Variant): Value;
  hasVariant(): boolean;
  clearVariant(): Value;

  getContractId(): string;
  setContractId(value: string): Value;

  getList(): List | undefined;
  setList(value?: List): Value;
  hasList(): boolean;
  clearList(): Value;

  getInt64(): string;
  setInt64(value: string): Value;

  getNumeric(): string;
  setNumeric(value: string): Value;

  getText(): string;
  setText(value: string): Value;

  getTimestamp(): string;
  setTimestamp(value: string): Value;

  getParty(): string;
  setParty(value: string): Value;

  getBool(): boolean;
  setBool(value: boolean): Value;

  getUnit(): google_protobuf_empty_pb.Empty | undefined;
  setUnit(value?: google_protobuf_empty_pb.Empty): Value;
  hasUnit(): boolean;
  clearUnit(): Value;

  getDate(): number;
  setDate(value: number): Value;

  getOptional(): Optional | undefined;
  setOptional(value?: Optional): Value;
  hasOptional(): boolean;
  clearOptional(): Value;

  getMap(): Map | undefined;
  setMap(value?: Map): Value;
  hasMap(): boolean;
  clearMap(): Value;

  getEnum(): Enum | undefined;
  setEnum(value?: Enum): Value;
  hasEnum(): boolean;
  clearEnum(): Value;

  getGenMap(): GenMap | undefined;
  setGenMap(value?: GenMap): Value;
  hasGenMap(): boolean;
  clearGenMap(): Value;

  getSumCase(): Value.SumCase;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Value.AsObject;
  static toObject(includeInstance: boolean, msg: Value): Value.AsObject;
  static serializeBinaryToWriter(message: Value, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Value;
  static deserializeBinaryFromReader(message: Value, reader: jspb.BinaryReader): Value;
}

export namespace Value {
  export type AsObject = {
    record?: Record.AsObject,
    variant?: Variant.AsObject,
    contractId: string,
    list?: List.AsObject,
    int64: string,
    numeric: string,
    text: string,
    timestamp: string,
    party: string,
    bool: boolean,
    unit?: google_protobuf_empty_pb.Empty.AsObject,
    date: number,
    optional?: Optional.AsObject,
    map?: Map.AsObject,
    pb_enum?: Enum.AsObject,
    genMap?: GenMap.AsObject,
  }

  export enum SumCase { 
    SUM_NOT_SET = 0,
    RECORD = 1,
    VARIANT = 2,
    CONTRACT_ID = 3,
    LIST = 4,
    INT64 = 5,
    NUMERIC = 6,
    TEXT = 8,
    TIMESTAMP = 9,
    PARTY = 11,
    BOOL = 12,
    UNIT = 13,
    DATE = 14,
    OPTIONAL = 15,
    MAP = 16,
    ENUM = 17,
    GEN_MAP = 18,
  }
}

export class Record extends jspb.Message {
  getRecordId(): Identifier | undefined;
  setRecordId(value?: Identifier): Record;
  hasRecordId(): boolean;
  clearRecordId(): Record;

  getFieldsList(): Array<RecordField>;
  setFieldsList(value: Array<RecordField>): Record;
  clearFieldsList(): Record;
  addFields(value?: RecordField, index?: number): RecordField;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Record.AsObject;
  static toObject(includeInstance: boolean, msg: Record): Record.AsObject;
  static serializeBinaryToWriter(message: Record, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Record;
  static deserializeBinaryFromReader(message: Record, reader: jspb.BinaryReader): Record;
}

export namespace Record {
  export type AsObject = {
    recordId?: Identifier.AsObject,
    fieldsList: Array<RecordField.AsObject>,
  }
}

export class RecordField extends jspb.Message {
  getLabel(): string;
  setLabel(value: string): RecordField;

  getValue(): Value | undefined;
  setValue(value?: Value): RecordField;
  hasValue(): boolean;
  clearValue(): RecordField;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): RecordField.AsObject;
  static toObject(includeInstance: boolean, msg: RecordField): RecordField.AsObject;
  static serializeBinaryToWriter(message: RecordField, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): RecordField;
  static deserializeBinaryFromReader(message: RecordField, reader: jspb.BinaryReader): RecordField;
}

export namespace RecordField {
  export type AsObject = {
    label: string,
    value?: Value.AsObject,
  }
}

export class Identifier extends jspb.Message {
  getPackageId(): string;
  setPackageId(value: string): Identifier;

  getModuleName(): string;
  setModuleName(value: string): Identifier;

  getEntityName(): string;
  setEntityName(value: string): Identifier;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Identifier.AsObject;
  static toObject(includeInstance: boolean, msg: Identifier): Identifier.AsObject;
  static serializeBinaryToWriter(message: Identifier, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Identifier;
  static deserializeBinaryFromReader(message: Identifier, reader: jspb.BinaryReader): Identifier;
}

export namespace Identifier {
  export type AsObject = {
    packageId: string,
    moduleName: string,
    entityName: string,
  }
}

export class Variant extends jspb.Message {
  getVariantId(): Identifier | undefined;
  setVariantId(value?: Identifier): Variant;
  hasVariantId(): boolean;
  clearVariantId(): Variant;

  getConstructor(): string;
  setConstructor(value: string): Variant;

  getValue(): Value | undefined;
  setValue(value?: Value): Variant;
  hasValue(): boolean;
  clearValue(): Variant;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Variant.AsObject;
  static toObject(includeInstance: boolean, msg: Variant): Variant.AsObject;
  static serializeBinaryToWriter(message: Variant, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Variant;
  static deserializeBinaryFromReader(message: Variant, reader: jspb.BinaryReader): Variant;
}

export namespace Variant {
  export type AsObject = {
    variantId?: Identifier.AsObject,
    constructor: string,
    value?: Value.AsObject,
  }
}

export class Enum extends jspb.Message {
  getEnumId(): Identifier | undefined;
  setEnumId(value?: Identifier): Enum;
  hasEnumId(): boolean;
  clearEnumId(): Enum;

  getConstructor(): string;
  setConstructor(value: string): Enum;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Enum.AsObject;
  static toObject(includeInstance: boolean, msg: Enum): Enum.AsObject;
  static serializeBinaryToWriter(message: Enum, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Enum;
  static deserializeBinaryFromReader(message: Enum, reader: jspb.BinaryReader): Enum;
}

export namespace Enum {
  export type AsObject = {
    enumId?: Identifier.AsObject,
    constructor: string,
  }
}

export class List extends jspb.Message {
  getElementsList(): Array<Value>;
  setElementsList(value: Array<Value>): List;
  clearElementsList(): List;
  addElements(value?: Value, index?: number): Value;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): List.AsObject;
  static toObject(includeInstance: boolean, msg: List): List.AsObject;
  static serializeBinaryToWriter(message: List, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): List;
  static deserializeBinaryFromReader(message: List, reader: jspb.BinaryReader): List;
}

export namespace List {
  export type AsObject = {
    elementsList: Array<Value.AsObject>,
  }
}

export class Optional extends jspb.Message {
  getValue(): Value | undefined;
  setValue(value?: Value): Optional;
  hasValue(): boolean;
  clearValue(): Optional;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Optional.AsObject;
  static toObject(includeInstance: boolean, msg: Optional): Optional.AsObject;
  static serializeBinaryToWriter(message: Optional, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Optional;
  static deserializeBinaryFromReader(message: Optional, reader: jspb.BinaryReader): Optional;
}

export namespace Optional {
  export type AsObject = {
    value?: Value.AsObject,
  }
}

export class Map extends jspb.Message {
  getEntriesList(): Array<Map.Entry>;
  setEntriesList(value: Array<Map.Entry>): Map;
  clearEntriesList(): Map;
  addEntries(value?: Map.Entry, index?: number): Map.Entry;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Map.AsObject;
  static toObject(includeInstance: boolean, msg: Map): Map.AsObject;
  static serializeBinaryToWriter(message: Map, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Map;
  static deserializeBinaryFromReader(message: Map, reader: jspb.BinaryReader): Map;
}

export namespace Map {
  export type AsObject = {
    entriesList: Array<Map.Entry.AsObject>,
  }

  export class Entry extends jspb.Message {
    getKey(): string;
    setKey(value: string): Entry;

    getValue(): Value | undefined;
    setValue(value?: Value): Entry;
    hasValue(): boolean;
    clearValue(): Entry;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): Entry.AsObject;
    static toObject(includeInstance: boolean, msg: Entry): Entry.AsObject;
    static serializeBinaryToWriter(message: Entry, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): Entry;
    static deserializeBinaryFromReader(message: Entry, reader: jspb.BinaryReader): Entry;
  }

  export namespace Entry {
    export type AsObject = {
      key: string,
      value?: Value.AsObject,
    }
  }

}

export class GenMap extends jspb.Message {
  getEntriesList(): Array<GenMap.Entry>;
  setEntriesList(value: Array<GenMap.Entry>): GenMap;
  clearEntriesList(): GenMap;
  addEntries(value?: GenMap.Entry, index?: number): GenMap.Entry;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GenMap.AsObject;
  static toObject(includeInstance: boolean, msg: GenMap): GenMap.AsObject;
  static serializeBinaryToWriter(message: GenMap, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GenMap;
  static deserializeBinaryFromReader(message: GenMap, reader: jspb.BinaryReader): GenMap;
}

export namespace GenMap {
  export type AsObject = {
    entriesList: Array<GenMap.Entry.AsObject>,
  }

  export class Entry extends jspb.Message {
    getKey(): Value | undefined;
    setKey(value?: Value): Entry;
    hasKey(): boolean;
    clearKey(): Entry;

    getValue(): Value | undefined;
    setValue(value?: Value): Entry;
    hasValue(): boolean;
    clearValue(): Entry;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): Entry.AsObject;
    static toObject(includeInstance: boolean, msg: Entry): Entry.AsObject;
    static serializeBinaryToWriter(message: Entry, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): Entry;
    static deserializeBinaryFromReader(message: Entry, reader: jspb.BinaryReader): Entry;
  }

  export namespace Entry {
    export type AsObject = {
      key?: Value.AsObject,
      value?: Value.AsObject,
    }
  }

}

