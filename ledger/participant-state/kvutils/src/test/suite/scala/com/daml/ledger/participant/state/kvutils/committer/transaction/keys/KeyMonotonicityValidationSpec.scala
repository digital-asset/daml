// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.keys

import java.time.{Instant, ZoneOffset, ZonedDateTime}

import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.committer.StepContinue
import com.daml.ledger.participant.state.kvutils.committer.transaction.{
  DamlTransactionEntrySummary,
  TransactionCommitter,
}
import com.daml.ledger.participant.state.v1.{RejectionReasonV0}
import com.daml.logging.LoggingContext
import com.google.protobuf.ByteString
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class KeyMonotonicityValidationSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val testKey = DamlStateKey.newBuilder().build()
  private val testSubmissionSeed = ByteString.copyFromUtf8("a" * 32)
  private val ledgerEffectiveTime =
    ZonedDateTime.of(2021, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC).toInstant
  private val testTransactionEntry = DamlTransactionEntrySummary(
    DamlTransactionEntry.newBuilder
      .setSubmissionSeed(testSubmissionSeed)
      .setLedgerEffectiveTime(Conversions.buildTimestamp(ledgerEffectiveTime))
      .build
  )

  "checkContractKeysCausalMonotonicity" should {
    "create StepContinue in case of correct keys" in {
      KeyMonotonicityValidation.checkContractKeysCausalMonotonicity(
        mock[TransactionCommitter],
        None,
        Set(testKey),
        Map(testKey -> aStateValueActiveAt(ledgerEffectiveTime.minusSeconds(1))),
        testTransactionEntry,
      ) shouldBe StepContinue(testTransactionEntry)
    }

    "reject transaction in case of incorrect keys" in {
      val mockTransactionCommitter = mock[TransactionCommitter]

      KeyMonotonicityValidation
        .checkContractKeysCausalMonotonicity(
          mockTransactionCommitter,
          None,
          Set(testKey),
          Map(testKey -> aStateValueActiveAt(ledgerEffectiveTime.plusSeconds(1))),
          testTransactionEntry,
        )

      verify(mockTransactionCommitter).buildRejectionLogEntry(
        eqTo(testTransactionEntry),
        any[RejectionReasonV0.InvalidLedgerTime],
      )(any[LoggingContext])
      verify(mockTransactionCommitter).reject(
        eqTo(None),
        any[DamlTransactionRejectionEntry.Builder],
      )
      succeed
    }
  }

  private def aStateValueActiveAt(activeAt: Instant) =
    DamlStateValue.newBuilder
      .setContractKeyState(
        DamlContractKeyState.newBuilder.setActiveAt(Conversions.buildTimestamp(activeAt))
      )
      .build
}
