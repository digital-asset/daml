// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy.wasm

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.{Value => LfValue}
import com.dylibso.chicory.runtime.{Instance => WasmInstance}

import scala.concurrent.duration.Duration

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
  )(implicit instance: WasmInstance): LfValue
}
