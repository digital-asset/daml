// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy.wasm

package host

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.speedy.wasm.WasmUtils
import com.digitalasset.daml.lf.value.{Value => LfValue}
import com.dylibso.chicory.runtime.{HostFunction => WasmHostFunction, Instance => WasmInstance}
import com.dylibso.chicory.wasm.types.{Value => WasmValue, ValueType => WasmValueType}
import com.google.protobuf.ByteString

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

trait WasmRunnerHostFunctions {

  def logInfo(msg: String): Unit

  def createContract(templateCons: Ref.TypeConRef, args: LfValue): LfValue.ContractId

  def fetchContractArg(
      templateId: Ref.TypeConRef,
      contractId: LfValue.ContractId,
      timeout: Duration,
  ): LfValue

  def exerciseChoice(
      templateId: Ref.TypeConRef,
      contractId: LfValue.ContractId,
      choiceName: Ref.ChoiceName,
      choiceArg: LfValue,
      consuming: Boolean,
  ): LfValue
}

object WasmRunnerHostFunctions {
  import WasmUtils._

  private[wasm] def wasmFunction(name: String, numOfParams: Int, returnType: Option[WasmValueType])(
      lambda: Array[ByteString] => ByteString
  ): WasmHostFunction = {
    new WasmHostFunction(
      (instance: WasmInstance, args: Array[WasmValue]) => {
        require(args.length == numOfParams)

        copyByteString(
          lambda((0 until numOfParams).map(copyWasmValues(args, _)(instance)).toArray)
        )(instance)
      },
      "env",
      name,
      (0 until numOfParams).flatMap(_ => WasmValueParameterType).asJava,
      returnType.toList.asJava,
    )
  }
}
