// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.committer.transaction.Rejection
import com.daml.logging.{ContextualizedLogger, LoggingContext}

/** Selects a write set from a [[PreExecutionOutput]] based on the current time.
  * If the current time is within the bounds specified by the output, the success write set is
  * chosen, otherwise, the out-of-time-bounds write set is chosen.
  */
final class TimeBasedWriteSetSelector[ReadSet, WriteSet](now: () => Instant)
    extends WriteSetSelector[ReadSet, WriteSet] {

  final private val logger = ContextualizedLogger.get(getClass)

  override def selectWriteSet(
      preExecutionOutput: PreExecutionOutput[ReadSet, WriteSet]
  )(implicit loggingContext: LoggingContext): WriteSet = {
    val recordTime = now()
    val minRecordTime = preExecutionOutput.minRecordTime.getOrElse(Instant.MIN)
    val maxRecordTime = preExecutionOutput.maxRecordTime.getOrElse(Instant.MAX)
    val withinTimeBounds =
      !recordTime.isBefore(minRecordTime) &&
        !recordTime.isAfter(maxRecordTime)
    if (withinTimeBounds) {
      preExecutionOutput.successWriteSet
    } else {
      val rejectionReason = Rejection.RecordTimeOutOfRange(minRecordTime, maxRecordTime)
      logger.trace(s"Transaction rejected at post-execution, ${rejectionReason.description}")
      preExecutionOutput.outOfTimeBoundsWriteSet
    }
  }

}
