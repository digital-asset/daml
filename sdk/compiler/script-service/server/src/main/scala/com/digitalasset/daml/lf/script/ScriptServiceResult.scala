// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package script

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.Location
import com.digitalasset.daml.lf.engine.script.{CurrentSubmission, ScriptMachineLogger}
import com.digitalasset.daml.lf.value.Value

sealed trait ScriptServiceResult extends Product with Serializable {
  def ledger: IdeLedger
}

final case class ScriptServiceSuccess(
    ledger: IdeLedger,
    machineLogger: ScriptMachineLogger,
    duration: Double,
    steps: Int,
    resultValue: Value,
) extends ScriptServiceResult

final case class ScriptServiceError(
    ledger: IdeLedger,
    machineLogger: ScriptMachineLogger,
    currentSubmission: Option[CurrentSubmission],
    stackTrace: ImmArray[Location],
    error: Error,
) extends ScriptServiceResult
