// Code generated by protoc-gen-as. DO NOT EDIT.
// Versions:
//   protoc-gen-as v1.3.0
//   protoc        v3.8.0

import { Writer, Reader, Protobuf } from "as-proto/assembly";
import { Value } from "../../Value";

export class Field {
  static encode(message: Field, writer: Writer): void {
    const value = message.value;
    if (value !== null) {
      writer.uint32(10);
      writer.fork();
      Value.encode(value, writer);
      writer.ldelim();
    }
  }

  static decode(reader: Reader, length: i32): Field {
    const end: usize = length < 0 ? reader.end : reader.ptr + length;
    const message = new Field();

    while (reader.ptr < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.value = Value.decode(reader, reader.uint32());
          break;

        default:
          reader.skipType(tag & 7);
          break;
      }
    }

    return message;
  }

  value: Value | null;

  constructor(value: Value | null = null) {
    this.value = value;
  }
}

export function encodeField(message: Field): Uint8Array {
  return Protobuf.encode(message, Field.encode);
}

export function decodeField(buffer: Uint8Array): Field {
  return Protobuf.decode<Field>(buffer, Field.decode);
}
