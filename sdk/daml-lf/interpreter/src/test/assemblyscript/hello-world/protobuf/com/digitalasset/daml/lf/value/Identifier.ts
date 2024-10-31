// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Code generated by protoc-gen-as. DO NOT EDIT.
// Versions:
//   protoc-gen-as v1.3.0
//   protoc        v3.8.0

import { Writer, Reader, Protobuf } from "as-proto/assembly";

export class Identifier {
  static encode(message: Identifier, writer: Writer): void {
    writer.uint32(10);
    writer.string(message.packageId);

    const moduleName = message.moduleName;
    if (moduleName.length !== 0) {
      for (let i: i32 = 0; i < moduleName.length; ++i) {
        writer.uint32(18);
        writer.string(moduleName[i]);
      }
    }

    const name = message.name;
    if (name.length !== 0) {
      for (let i: i32 = 0; i < name.length; ++i) {
        writer.uint32(26);
        writer.string(name[i]);
      }
    }
  }

  static decode(reader: Reader, length: i32): Identifier {
    const end: usize = length < 0 ? reader.end : reader.ptr + length;
    const message = new Identifier();

    while (reader.ptr < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.packageId = reader.string();
          break;

        case 2:
          message.moduleName.push(reader.string());
          break;

        case 3:
          message.name.push(reader.string());
          break;

        default:
          reader.skipType(tag & 7);
          break;
      }
    }

    return message;
  }

  packageId: string;
  moduleName: Array<string>;
  name: Array<string>;

  constructor(
    packageId: string = "",
    moduleName: Array<string> = [],
    name: Array<string> = [],
  ) {
    this.packageId = packageId;
    this.moduleName = moduleName;
    this.name = name;
  }
}

export function encodeIdentifier(message: Identifier): Uint8Array {
  return Protobuf.encode(message, Identifier.encode);
}

export function decodeIdentifier(buffer: Uint8Array): Identifier {
  return Protobuf.decode<Identifier>(buffer, Identifier.decode);
}
