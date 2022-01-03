// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import com.codahale.metrics.Counter
import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.committer.Committer.buildLogEntryWithOptionalRecordTime
import com.daml.ledger.participant.state.kvutils.committer.{StepResult, StepStop}
import com.daml.ledger.participant.state.kvutils.store.events.{
  DamlSubmitterInfo,
  DamlTransactionRejectionEntry,
}
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics

private[transaction] class Rejections(metrics: Metrics) {

  final private val logger = ContextualizedLogger.get(getClass)

  def reject[A](
      transactionEntry: DamlTransactionEntrySummary,
      rejection: Rejection,
      recordTime: Option[Timestamp],
  )(implicit loggingContext: LoggingContext): StepResult[A] =
    reject(
      Conversions.encodeTransactionRejectionEntry(transactionEntry.submitterInfo, rejection),
      rejection.description,
      recordTime,
    )

  def reject[A](
      rejectionEntry: DamlTransactionRejectionEntry.Builder,
      rejectionDescription: String,
      recordTime: Option[Timestamp],
  )(implicit loggingContext: LoggingContext): StepResult[A] = {
    Metrics.rejections(rejectionEntry.getReasonCase.getNumber).inc()
    logger.trace(s"Transaction rejected, $rejectionDescription.")
    StepStop(
      buildLogEntryWithOptionalRecordTime(
        recordTime,
        _.setTransactionRejectionEntry(rejectionEntry),
      )
    )
  }

  def preExecutionOutOfTimeBoundsRejectionEntry(
      submitterInfo: DamlSubmitterInfo,
      minimumRecordTime: Timestamp,
      maximumRecordTime: Timestamp,
  ): DamlTransactionRejectionEntry =
    Conversions
      .encodeTransactionRejectionEntry(
        submitterInfo,
        Rejection.RecordTimeOutOfRange(minimumRecordTime, maximumRecordTime),
      )
      .build

  private object Metrics {
    val rejections: Map[Int, Counter] =
      DamlTransactionRejectionEntry.ReasonCase.values
        .map(reasonCase =>
          reasonCase.getNumber -> metrics.daml.kvutils.committer.transaction
            .rejection(reasonCase.name())
        )
        .toMap
  }
}
