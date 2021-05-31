// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import java.time.Instant

/** Selects a write set from a [[PreExecutionOutput]] based on the current time.
  * If the current time is within the bounds specified by the output, the success write set is
  * chosen, otherwise, the out-of-time-bounds write set is chosen.
  */
final class TimeBasedWriteSetSelector[ReadSet, WriteSet](now: () => Instant)
    extends WriteSetSelector[ReadSet, WriteSet] {

  override def selectWriteSet(
      preExecutionOutput: PreExecutionOutput[ReadSet, WriteSet]
  ): WriteSet = {
    val recordTime = now()
    val withinTimeBounds =
      !recordTime.isBefore(preExecutionOutput.minRecordTime.getOrElse(Instant.MIN)) &&
        !recordTime.isAfter(preExecutionOutput.maxRecordTime.getOrElse(Instant.MAX))
    if (withinTimeBounds) {
      preExecutionOutput.successWriteSet
    } else {
      preExecutionOutput.outOfTimeBoundsWriteSet
    }
  }

}
