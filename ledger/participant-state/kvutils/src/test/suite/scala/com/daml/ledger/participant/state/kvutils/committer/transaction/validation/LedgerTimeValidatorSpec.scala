// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.TestHelpers.{
  createCommitContext,
  createEmptyTransactionEntry,
  theDefaultConfig,
}
import com.daml.ledger.participant.state.kvutils.committer.transaction.{
  DamlTransactionEntrySummary,
  Rejections,
}
import com.daml.ledger.participant.state.kvutils.committer.{StepContinue, StepStop}
import com.daml.ledger.participant.state.kvutils.store.DamlStateValue
import com.daml.ledger.participant.state.kvutils.store.events.{
  DamlConfigurationEntry,
  DamlTransactionEntry,
}
import com.daml.ledger.participant.state.kvutils.{Conversions, Err}
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LedgerTimeValidatorSpec extends AnyWordSpec with Matchers {

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val metrics = new Metrics(new MetricRegistry)
  private val rejections = new Rejections(metrics)
  private val ledgerTimeValidationStep =
    new LedgerTimeValidator(theDefaultConfig).createValidationStep(rejections)
  private val aDamlTransactionEntry: DamlTransactionEntry = createEmptyTransactionEntry(
    List("aSubmitter")
  )
  private val aTransactionEntrySummary = DamlTransactionEntrySummary(aDamlTransactionEntry)
  private val emptyConfigurationStateValue: DamlStateValue =
    defaultConfigurationStateValueBuilder().build

  "LedgerTimeValidator" can {
    "when the record time is not available" should {
      "compute and correctly set out-of-time-bounds log entry with min/max record time available in the committer context" in {
        val context = createCommitContext(recordTime = None)
        context.minimumRecordTime = Some(Timestamp.now())
        context.maximumRecordTime = Some(Timestamp.now())
        ledgerTimeValidationStep.apply(
          context,
          aTransactionEntrySummary,
        )

        context.outOfTimeBoundsLogEntry should not be empty
        context.outOfTimeBoundsLogEntry.foreach { actualOutOfTimeBoundsLogEntry =>
          actualOutOfTimeBoundsLogEntry.hasTransactionRejectionEntry shouldBe true
          actualOutOfTimeBoundsLogEntry.getTransactionRejectionEntry.hasRecordTimeOutOfRange shouldBe true
        }
      }

      "fail if minimum record time is not set" in {
        val context = createCommitContext(recordTime = None)
        context.maximumRecordTime = Some(Timestamp.now())
        an[Err.InternalError] shouldBe thrownBy(
          ledgerTimeValidationStep.apply(
            context,
            aTransactionEntrySummary,
          )
        )
      }

      "fail if maximum record time is not set" in {
        val context = createCommitContext(recordTime = None)
        context.minimumRecordTime = Some(Timestamp.now())
        an[Err.InternalError] shouldBe thrownBy(
          ledgerTimeValidationStep.apply(
            context,
            aTransactionEntrySummary,
          )
        )
      }
    }

    "produce rejection log entry if record time is outside of ledger effective time bounds" in {
      val recordTime = Timestamp.now()
      val recordTimeInstant = recordTime.toInstant
      val lowerBound =
        recordTimeInstant
          .minus(theDefaultConfig.timeModel.minSkew)
          .minusMillis(1)
      val upperBound =
        recordTimeInstant.plus(theDefaultConfig.timeModel.maxSkew).plusMillis(1)
      val inputWithDeclaredConfig =
        Map(Conversions.configurationStateKey -> Some(emptyConfigurationStateValue))

      for (ledgerEffectiveTime <- Iterable(lowerBound, upperBound)) {
        val context =
          createCommitContext(recordTime = Some(recordTime), inputs = inputWithDeclaredConfig)
        val transactionEntrySummary = DamlTransactionEntrySummary(
          aDamlTransactionEntry.toBuilder
            .setLedgerEffectiveTime(
              com.google.protobuf.Timestamp.newBuilder
                .setSeconds(ledgerEffectiveTime.getEpochSecond)
                .setNanos(ledgerEffectiveTime.getNano)
            )
            .build
        )
        val actual = ledgerTimeValidationStep.apply(context, transactionEntrySummary)

        actual match {
          case StepContinue(_) => fail()
          case StepStop(actualLogEntry) =>
            actualLogEntry.hasTransactionRejectionEntry shouldBe true
        }
      }
    }

  }

  private def defaultConfigurationStateValueBuilder(): DamlStateValue.Builder =
    DamlStateValue.newBuilder
      .setConfigurationEntry(
        DamlConfigurationEntry.newBuilder
          .setConfiguration(Configuration.encode(theDefaultConfig))
      )
}
