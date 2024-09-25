// Code generated by protoc-gen-as. DO NOT EDIT.
// Versions:
//   protoc-gen-as v1.3.0
//   protoc        v3.8.0

import { Writer, Reader, Protobuf } from "as-proto/assembly";
import { Value } from "../Value";

export class Optional {
  static encode(message: Optional, writer: Writer): void {
    const value = message.value;
    if (value !== null) {
      writer.uint32(10);
      writer.fork();
      Value.encode(value, writer);
      writer.ldelim();
    }
  }

  static decode(reader: Reader, length: i32): Optional {
    const end: usize = length < 0 ? reader.end : reader.ptr + length;
    const message = new Optional();

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

export function encodeOptional(message: Optional): Uint8Array {
  return Protobuf.encode(message, Optional.encode);
}

export function decodeOptional(buffer: Uint8Array): Optional {
  return Protobuf.decode<Optional>(buffer, Optional.decode);
}
