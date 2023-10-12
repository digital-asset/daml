// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.CommonData
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext

object TimeValidator {

  def checkTimestamps(
      commonData: CommonData,
      sequencerTimestamp: CantonTimestamp,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
      amSubmitter: Boolean,
      logger: TracedLogger,
  )(implicit tc: TraceContext): Either[TimeCheckFailure, Unit] = {

    val CommonData(_transactionId, ledgerTime, submissionTime, _confirmationPolicy) = commonData

    def log(msg: String): Unit = {
      lazy val logMsg = s"Time validation has failed: ${msg}"
      if (amSubmitter) logger.warn(logMsg)
      else logger.info(logMsg)
    }

    // check that the ledger time is valid
    if (
      ledgerTime < sequencerTimestamp - ledgerTimeRecordTimeTolerance ||
      ledgerTime > sequencerTimestamp + ledgerTimeRecordTimeTolerance
    ) {
      log(
        s"The delta of the ledger time $ledgerTime and the record time $sequencerTimestamp exceeds the max " +
          s"of $ledgerTimeRecordTimeTolerance"
      )
      Left(
        LedgerTimeRecordTimeDeltaTooLargeError(
          ledgerTime,
          sequencerTimestamp,
          ledgerTimeRecordTimeTolerance,
        )
      )
    }
    // check that the submission time is valid
    else if (
      submissionTime < sequencerTimestamp - ledgerTimeRecordTimeTolerance ||
      submissionTime > sequencerTimestamp + ledgerTimeRecordTimeTolerance
    ) {
      log(
        s"The delta of the submission time $submissionTime and the record time $sequencerTimestamp exceeds the max " +
          s"of $ledgerTimeRecordTimeTolerance"
      )
      Left(
        SubmissionTimeRecordTimeDeltaTooLargeError(
          submissionTime,
          sequencerTimestamp,
          ledgerTimeRecordTimeTolerance,
        )
      )

    } else Right(())

  }

  sealed trait TimeCheckFailure extends Product with Serializable

  final case class LedgerTimeRecordTimeDeltaTooLargeError(
      ledgerTime: CantonTimestamp,
      recordTime: CantonTimestamp,
      maxDelta: NonNegativeFiniteDuration,
  ) extends TimeCheckFailure

  final case class SubmissionTimeRecordTimeDeltaTooLargeError(
      submissionTime: CantonTimestamp,
      recordTime: CantonTimestamp,
      maxDelta: NonNegativeFiniteDuration,
  ) extends TimeCheckFailure

}
