// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy.wasm

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.{Value => LfValue}

trait WasmRunnerHostFunctions {

  def logInfo(msg: String): Unit

  def createContract(templateCons: Ref.TypeConRef, args: LfValue): LfValue.ContractId
}
