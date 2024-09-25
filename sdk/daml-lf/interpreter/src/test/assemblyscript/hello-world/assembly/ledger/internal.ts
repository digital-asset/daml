// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as lf from "../../protobuf/com/digitalasset/daml/lf/value/Value"

export class ByteString {
    ptr: ArrayBuffer;
    size: i32;

    private _heapPtr: i32;

    constructor(data: ArrayBuffer) {
        this.ptr = data
        this.size = data.byteLength
        this._heapPtr = 0
    }

    static fromString(msg: string): ByteString {
        let msgPtr = String.UTF8.encode(msg)

        return new ByteString(msgPtr)
    }

    static fromLfValue(value: lf.Value): ByteString {
        let valuePtr = lf.encodeValue(value)

        return new ByteString(valuePtr)
    }

    toLfValue(): lf.Value {
        let value = lf.decodeValue(this.ptr)

        return value
    }

    alloc(): void {
        if (this._heapPtr == 0) {
            this._heapPtr = i32(heap.alloc(sizeof<i32>() * 2))

            store<ArrayBuffer>(this._heapPtr, this.ptr)
            store<i32>(this._heapPtr + sizeof<i32>(), this.size)
        } else {
            throw new Error("Attempted to allocate an allocated ByteString - need to call dealloc() first")
        }
    }

    heapPtr(): i32 {
        if (this._heapPtr != 0) {
            return this._heapPtr
        } else {
            throw new Error("Attempted to access a null heap pointer - need to call alloc() first")
        }
    }

    dealloc(): void {
        if (this._heapPtr != 0) {
            heap.free(this._heapPtr)
            this._heapPtr = 0
        } else {
            throw new Error("Attempted to deallocate an unallocated ByteString - need to call alloc() first")
        }
    }
}

@external("env", "logInfo")
declare function logInfo(msgPtr: usize): void
export { logInfo }

export function alloc(size: i32): u32 {
    return heap.alloc(size);
}

export function dealloc(ptr: i32, size: i32): void {
    heap.free(ptr);
}

