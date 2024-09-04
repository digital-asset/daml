// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy.wasm

package internal

import com.daml.scalautil.Statement.discard
import com.dylibso.chicory.runtime.{Instance => WasmInstance}
import com.dylibso.chicory.wasm.types.{Value => WasmValue, ValueType => WasmValueType}
import com.google.protobuf.ByteString

object WasmUtils {

  private[wasm] val WasmValueParameterType = List(WasmValueType.I32)
  private[wasm] val WasmUnitResultType = None
  private[wasm] val WasmValueResultType = Some(WasmValueType.I32)
  private[wasm] val i32Size = WasmValueType.I32.size()

  private[wasm] def copyWasmValue(values: Array[WasmValue])(implicit
      instance: WasmInstance
  ): ByteString = {
    copyWasmValues(values, 0)
  }

  private[wasm] def copyWasmValues(values: Array[WasmValue], index: Int)(implicit
      instance: WasmInstance
  ): ByteString = {
    require(0 <= index && index < values.length)

    val byteStringPtr = values(index).asInt()
    val ptr = instance.memory().readI32(byteStringPtr)
    val size = instance.memory().readI32(byteStringPtr + i32Size)

    ByteString.copyFrom(
      instance.memory().readBytes(ptr.asInt(), size.asInt())
    )
  }

  private[wasm] def copyByteString(
      value: ByteString
  )(implicit instance: WasmInstance): Array[WasmValue] = {
    copyByteArray(value.toByteArray)
  }

  private[wasm] def copyByteArray(
      value: Array[Byte]
  )(implicit instance: WasmInstance): Array[WasmValue] = {
    if (value.isEmpty) {
      Array.empty
    } else {
      val alloc = instance.export("alloc")
      val valuePtr = alloc.apply(WasmValue.i32(value.length))(0).asInt
      val byteStringPtr = alloc.apply(WasmValue.i32(2 * i32Size))(0).asInt

      instance.memory().write(valuePtr, value)
      instance.memory().writeI32(byteStringPtr, valuePtr)
      instance.memory().writeI32(byteStringPtr + i32Size, value.length)

      Array(WasmValue.i32(byteStringPtr))
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  private[wasm] def deallocByteString(
      byteStringPtr: WasmValue
  )(implicit instance: WasmInstance): Unit = {
    val dealloc = instance.export("dealloc")
    val valuePtr = instance.memory().readI32(byteStringPtr.asInt())
    val size = instance.memory().readI32(byteStringPtr.asInt() + i32Size)

    discard {
      dealloc.apply(valuePtr, size)
      dealloc.apply(byteStringPtr, WasmValue.i32(2 * i32Size))
    }
  }
}
