// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.either.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.CommonData
import com.digitalasset.canton.participant.protocol.validation.TimeValidator.{
  LedgerTimeRecordTimeDeltaTooLargeError,
  SubmissionTimeRecordTimeDeltaTooLargeError,
}
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, TransactionId}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class TimeValidatorTest extends AnyWordSpec with BaseTest {
  private val ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfSeconds(10)
  private val submissionTimeRecordTimeTolerance: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfSeconds(60)
  private val transactionId: TransactionId = ExampleTransactionFactory.transactionId(0)

  private def checkTimestamps(
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      sequencerTimestamp: CantonTimestamp,
  ) =
    TimeValidator.checkTimestamps(
      CommonData(transactionId, ledgerTime, submissionTime),
      sequencerTimestamp,
      ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
      submissionTimeRecordTimeTolerance = submissionTimeRecordTimeTolerance,
      amSubmitter = false,
      logger,
    )

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
          )
        val latestRes = checkTimestamps(
          ledgerTimeLatest,
          submissionTime,
          sequencerTime,
        )

        earliestRes shouldBe Either.unit
        latestRes shouldBe Either.unit
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
          )
        val tooEarly =
          checkTimestamps(
            ledgerTime,
            submissionTime,
            pastSeqTimestamp,
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
        val submissionTimeEarliest = sequencerTime - submissionTimeRecordTimeTolerance
        val submissionTimeLatest = sequencerTime + submissionTimeRecordTimeTolerance

        val earliestRes =
          checkTimestamps(
            ledgerTime,
            submissionTimeEarliest,
            sequencerTime,
          )
        val latestRes = checkTimestamps(
          ledgerTime,
          submissionTimeLatest,
          sequencerTime,
        )

        earliestRes shouldBe Either.unit
        latestRes shouldBe Either.unit
      }
    }

    "too far from sequencer timestamp" should {
      val submissionTimeBeforeSeq: CantonTimestamp = CantonTimestamp.Epoch.minusMillis(9)
      val futureSeqTimestamp =
        (submissionTimeBeforeSeq + submissionTimeRecordTimeTolerance).add(Duration.ofMillis(1))

      val submissionTimeAfterSeq = CantonTimestamp.ofEpochSecond(1)
      val pastSeqTimestamp =
        (submissionTimeAfterSeq - submissionTimeRecordTimeTolerance).minus(Duration.ofMillis(1))

      "yield an error" in {
        val tooLate =
          checkTimestamps(
            ledgerTime = futureSeqTimestamp, // Set the ledger time to the seq time to make it valid
            submissionTimeBeforeSeq,
            futureSeqTimestamp,
          )

        val tooEarly =
          checkTimestamps(
            ledgerTime = pastSeqTimestamp, // Set the ledger time to the seq time to make it valid
            submissionTimeAfterSeq,
            pastSeqTimestamp,
          )

        tooLate shouldBe Left(
          SubmissionTimeRecordTimeDeltaTooLargeError(
            submissionTimeBeforeSeq,
            futureSeqTimestamp,
            submissionTimeRecordTimeTolerance,
          )
        )
        tooEarly shouldBe Left(
          SubmissionTimeRecordTimeDeltaTooLargeError(
            submissionTimeAfterSeq,
            pastSeqTimestamp,
            submissionTimeRecordTimeTolerance,
          )
        )
      }
    }
  }

}
