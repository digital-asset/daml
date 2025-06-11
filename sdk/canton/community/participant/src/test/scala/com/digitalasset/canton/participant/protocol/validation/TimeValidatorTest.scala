// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.either.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.CommonData
import com.digitalasset.canton.participant.protocol.validation.TimeValidator.{
  LedgerTimeRecordTimeDeltaTooLargeError,
  PreparationTimeRecordTimeDeltaTooLargeError,
}
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, TransactionId}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class TimeValidatorTest extends AnyWordSpec with BaseTest {
  private val ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfSeconds(10)
  private val preparationTimeRecordTimeTolerance: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfSeconds(60)
  private val transactionId: TransactionId = ExampleTransactionFactory.transactionId(0)

  private def checkTimestamps(
      ledgerTime: CantonTimestamp,
      preparationTime: CantonTimestamp,
      sequencerTimestamp: CantonTimestamp,
  ) =
    TimeValidator.checkTimestamps(
      CommonData(transactionId, ledgerTime, preparationTime),
      sequencerTimestamp,
      ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
      preparationTimeRecordTimeTolerance = preparationTimeRecordTimeTolerance,
      amSubmitter = false,
      logger,
    )

  "ledger time" when {

    "valid" should {
      "yield a Right" in {
        val sequencerTime = CantonTimestamp.Epoch
        val preparationTime = CantonTimestamp.Epoch

        val ledgerTimeEarliest = sequencerTime - ledgerTimeRecordTimeTolerance
        val ledgerTimeLatest = sequencerTime + ledgerTimeRecordTimeTolerance

        val earliestRes =
          checkTimestamps(
            ledgerTimeEarliest,
            preparationTime,
            sequencerTime,
          )
        val latestRes = checkTimestamps(
          ledgerTimeLatest,
          preparationTime,
          sequencerTime,
        )

        earliestRes shouldBe Either.unit
        latestRes shouldBe Either.unit
      }
    }

    "too far from sequencer timestamp" should {
      "yield an error" in {
        val preparationTime: CantonTimestamp = CantonTimestamp.Epoch.minusMillis(9)
        val ledgerTime = CantonTimestamp.Epoch

        val futureSeqTimestamp =
          ledgerTime + ledgerTimeRecordTimeTolerance + NonNegativeFiniteDuration.tryOfSeconds(10)

        val pastSeqTimestamp =
          ledgerTime - ledgerTimeRecordTimeTolerance - NonNegativeFiniteDuration.tryOfMillis(1)

        val tooLate =
          checkTimestamps(
            ledgerTime,
            preparationTime,
            sequencerTimestamp = futureSeqTimestamp,
          )
        val tooEarly =
          checkTimestamps(
            ledgerTime,
            preparationTime,
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

  "preparation time " when {
    val ledgerTime = CantonTimestamp.Epoch
    val sequencerTime = CantonTimestamp.Epoch

    "valid" should {
      "yield a Right" in {
        val preparationTimeEarliest = sequencerTime - preparationTimeRecordTimeTolerance
        val preparationTimeLatest = sequencerTime + preparationTimeRecordTimeTolerance

        val earliestRes =
          checkTimestamps(
            ledgerTime,
            preparationTimeEarliest,
            sequencerTime,
          )
        val latestRes = checkTimestamps(
          ledgerTime,
          preparationTimeLatest,
          sequencerTime,
        )

        earliestRes shouldBe Either.unit
        latestRes shouldBe Either.unit
      }
    }

    "too far from sequencer timestamp" should {
      val preparationTimeBeforeSeq: CantonTimestamp = CantonTimestamp.Epoch.minusMillis(9)
      val futureSeqTimestamp =
        (preparationTimeBeforeSeq + preparationTimeRecordTimeTolerance).add(Duration.ofMillis(1))

      val preparationTimeAfterSeq = CantonTimestamp.ofEpochSecond(1)
      val pastSeqTimestamp =
        (preparationTimeAfterSeq - preparationTimeRecordTimeTolerance).minus(Duration.ofMillis(1))

      "yield an error" in {
        val tooLate =
          checkTimestamps(
            ledgerTime = futureSeqTimestamp, // Set the ledger time to the seq time to make it valid
            preparationTimeBeforeSeq,
            futureSeqTimestamp,
          )

        val tooEarly =
          checkTimestamps(
            ledgerTime = pastSeqTimestamp, // Set the ledger time to the seq time to make it valid
            preparationTimeAfterSeq,
            pastSeqTimestamp,
          )

        tooLate shouldBe Left(
          PreparationTimeRecordTimeDeltaTooLargeError(
            preparationTimeBeforeSeq,
            futureSeqTimestamp,
            preparationTimeRecordTimeTolerance,
          )
        )
        tooEarly shouldBe Left(
          PreparationTimeRecordTimeDeltaTooLargeError(
            preparationTimeAfterSeq,
            pastSeqTimestamp,
            preparationTimeRecordTimeTolerance,
          )
        )
      }
    }
  }

}
