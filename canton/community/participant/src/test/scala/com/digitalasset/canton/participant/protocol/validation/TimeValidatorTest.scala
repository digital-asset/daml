// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.CommonData
import com.digitalasset.canton.participant.protocol.validation.TimeValidator.{
  LedgerTimeRecordTimeDeltaTooLargeError,
  SubmissionTimeRecordTimeDeltaTooLargeError,
}
import com.digitalasset.canton.protocol.{
  ConfirmationPolicy,
  ExampleTransactionFactory,
  TransactionId,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class TimeValidatorTest extends AnyWordSpec with BaseTest {
  val sequencerTimestamp: CantonTimestamp = CantonTimestamp.ofEpochSecond(0)
  val ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfSeconds(10)
  val transactionId: TransactionId = ExampleTransactionFactory.transactionId(0)
  val confirmationPolicy: ConfirmationPolicy = mock[ConfirmationPolicy]

  private def checkTimestamps(
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      sequencerTimestamp: CantonTimestamp,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
  ) = {
    TimeValidator.checkTimestamps(
      CommonData(transactionId, ledgerTime, submissionTime, confirmationPolicy),
      sequencerTimestamp,
      ledgerTimeRecordTimeTolerance,
      amSubmitter = false,
      logger,
    )
  }

  "ledger time" when {

    "valid" should {
      "yield a Right" in {
        val sequencerTime = CantonTimestamp.Epoch
        val submissionTime = CantonTimestamp.Epoch

        val ledgerTimeEarliest = sequencerTime - ledgerTimeRecordTimeTolerance
        val ledgerTimeLatest = sequencerTime + ledgerTimeRecordTimeTolerance

        val earliestRes =
          checkTimestamps(
            ledgerTimeEarliest,
            submissionTime,
            sequencerTime,
            ledgerTimeRecordTimeTolerance,
          )
        val latestRes = checkTimestamps(
          ledgerTimeLatest,
          submissionTime,
          sequencerTime,
          ledgerTimeRecordTimeTolerance,
        )

        earliestRes shouldBe Right(())
        latestRes shouldBe Right(())
      }
    }

    "too far from sequencer timestamp" should {
      "yield an error" in {
        val submissionTime: CantonTimestamp = CantonTimestamp.Epoch.minusMillis(9)
        val ledgerTime = CantonTimestamp.Epoch

        val futureSeqTimestamp =
          ledgerTime + ledgerTimeRecordTimeTolerance + NonNegativeFiniteDuration.tryOfSeconds(10)

        val pastSeqTimestamp =
          ledgerTime - ledgerTimeRecordTimeTolerance - NonNegativeFiniteDuration.tryOfMillis(1)

        val tooLate =
          checkTimestamps(
            ledgerTime,
            submissionTime,
            sequencerTimestamp = futureSeqTimestamp,
            ledgerTimeRecordTimeTolerance,
          )
        val tooEarly =
          checkTimestamps(
            ledgerTime,
            submissionTime,
            pastSeqTimestamp,
            ledgerTimeRecordTimeTolerance,
          )

        tooLate shouldBe Left(
          LedgerTimeRecordTimeDeltaTooLargeError(
            ledgerTime,
            futureSeqTimestamp,
            ledgerTimeRecordTimeTolerance,
          )
        )
        tooEarly shouldBe Left(
          LedgerTimeRecordTimeDeltaTooLargeError(
            ledgerTime,
            pastSeqTimestamp,
            ledgerTimeRecordTimeTolerance,
          )
        )
      }
    }
  }

  "submission time " when {
    val ledgerTime = CantonTimestamp.Epoch
    val sequencerTime = CantonTimestamp.Epoch

    "valid" should {
      "yield a Right" in {
        val submissionTimeEarliest = sequencerTime - ledgerTimeRecordTimeTolerance
        val submissionTimeLatest = sequencerTime + ledgerTimeRecordTimeTolerance

        val earliestRes =
          checkTimestamps(
            ledgerTime,
            submissionTimeEarliest,
            sequencerTime,
            ledgerTimeRecordTimeTolerance,
          )
        val latestRes = checkTimestamps(
          ledgerTime,
          submissionTimeLatest,
          sequencerTime,
          ledgerTimeRecordTimeTolerance,
        )

        earliestRes shouldBe Right(())
        latestRes shouldBe Right(())
      }
    }

    "too far from sequencer timestamp" should {
      val submissionTimeBeforeSeq: CantonTimestamp = CantonTimestamp.Epoch.minusMillis(9)
      val futureSeqTimestamp =
        (submissionTimeBeforeSeq + ledgerTimeRecordTimeTolerance).add(Duration.ofMillis(1))

      val submissionTimeAfterSeq = CantonTimestamp.ofEpochSecond(1)
      val pastSeqTimestamp =
        (submissionTimeAfterSeq - ledgerTimeRecordTimeTolerance).minus(Duration.ofMillis(1))

      "yield an error" in {
        val tooLate =
          checkTimestamps(
            ledgerTime,
            submissionTimeBeforeSeq,
            futureSeqTimestamp,
            ledgerTimeRecordTimeTolerance,
          )

        val tooEarly =
          checkTimestamps(
            ledgerTime,
            submissionTimeAfterSeq,
            pastSeqTimestamp,
            ledgerTimeRecordTimeTolerance,
          )

        tooLate shouldBe Left(
          SubmissionTimeRecordTimeDeltaTooLargeError(
            submissionTimeBeforeSeq,
            futureSeqTimestamp,
            ledgerTimeRecordTimeTolerance,
          )
        )
        tooEarly shouldBe Left(
          SubmissionTimeRecordTimeDeltaTooLargeError(
            submissionTimeAfterSeq,
            pastSeqTimestamp,
            ledgerTimeRecordTimeTolerance,
          )
        )
      }
    }
  }

}
