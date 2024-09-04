// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy.wasm

import com.digitalasset.daml.lf.transaction.TransactionVersion
import com.digitalasset.daml.lf.value.{Value => LfValue, ValueCoder => LfValueCoder}
import com.dylibso.chicory.runtime.{Instance => WasmInstance}

object WasmRunnerExportFunctions {
  import WasmUtils._

  private[wasm] def wasmChoiceFunction(
      choiceName: String,
      txVersion: TransactionVersion,
  )(contractArg: LfValue, choiceArg: LfValue)(implicit instance: WasmInstance): LfValue = {
    val choice = instance.export(choiceName)
    val contractArgPtr = copyByteString(
      LfValueCoder
        .encodeValue(txVersion, contractArg)
        .fold(err => throw new RuntimeException(err.toString), identity)
    )
    val choiceArgPtr = copyByteString(
      LfValueCoder
        .encodeValue(txVersion, choiceArg)
        .fold(err => throw new RuntimeException(err.toString), identity)
    )
    val choiceResultPtr = choice.apply(contractArgPtr.head, choiceArgPtr.head)
    try {
      if (choiceResultPtr.nonEmpty) {
        LfValueCoder
          .decodeValue(txVersion, copyWasmValue(choiceResultPtr))
          .fold(err => throw new RuntimeException(err.toString), identity)
      } else {
        LfValue.ValueUnit
      }
    } finally {
      deallocByteString(contractArgPtr.head)
      deallocByteString(choiceArgPtr.head)
      deallocByteString(choiceResultPtr.head)
    }
  }

  private[wasm] def wasmTemplateFunction(
      functionName: String,
      txVersion: TransactionVersion,
  )(contractArg: LfValue)(implicit instance: WasmInstance): LfValue = {
    val function = instance.export(functionName)
    val contractArgPtr = copyByteString(
      LfValueCoder
        .encodeValue(txVersion, contractArg)
        .fold(err => throw new RuntimeException(err.toString), identity)
    )
    val resultPtr = function.apply(contractArgPtr.head)
    try {
      if (resultPtr.nonEmpty) {
        LfValueCoder
          .decodeValue(txVersion, copyWasmValue(resultPtr))
          .fold(err => throw new RuntimeException(err.toString), identity)
      } else {
        LfValue.ValueUnit
      }
    } finally {
      deallocByteString(contractArgPtr.head)
      deallocByteString(resultPtr.head)
    }
  }
}
