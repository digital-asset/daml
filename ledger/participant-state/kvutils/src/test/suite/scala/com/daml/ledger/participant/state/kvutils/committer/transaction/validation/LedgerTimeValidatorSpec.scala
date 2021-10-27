// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import java.time.Instant

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.Conversions.configurationStateKey
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
import com.daml.ledger.participant.state.kvutils.store.events.DamlConfigurationEntry
import com.daml.ledger.participant.state.kvutils.store.{DamlCommandDedupValue, DamlStateValue}
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

  private val aDamlTransactionEntry = createEmptyTransactionEntry(List("aSubmitter"))
  private val aTransactionEntrySummary = DamlTransactionEntrySummary(aDamlTransactionEntry)
  private val aSubmissionTime = createProtobufTimestamp(seconds = 1)
  private val aLedgerEffectiveTime = createProtobufTimestamp(seconds = 2)
  private val aDamlTransactionEntryWithSubmissionAndLedgerEffectiveTimes =
    aDamlTransactionEntry.toBuilder
      .setSubmissionTime(aSubmissionTime)
      .setLedgerEffectiveTime(aLedgerEffectiveTime)
      .build()
  private val aDamlTransactionEntrySummaryWithSubmissionAndLedgerEffectiveTimes =
    DamlTransactionEntrySummary(aDamlTransactionEntryWithSubmissionAndLedgerEffectiveTimes)
  private val emptyConfigurationStateValue =
    defaultConfigurationStateValueBuilder().build
  private val aDedupKey = Conversions
    .commandDedupKey(aTransactionEntrySummary.submitterInfo)
  private val inputWithTimeModelAndEmptyCommandDeduplication =
    Map(Conversions.configurationStateKey -> Some(emptyConfigurationStateValue), aDedupKey -> None)
  private val aDeduplicateUntil = createProtobufTimestamp(seconds = 3)
  private val aDedupValue = DamlStateValue.newBuilder
    .setCommandDedup(DamlCommandDedupValue.newBuilder.setDeduplicatedUntil(aDeduplicateUntil))
    .build()
  private val inputWithTimeModelAndCommandDeduplication =
    Map(
      Conversions.configurationStateKey -> Some(emptyConfigurationStateValue),
      aDedupKey -> Some(aDedupValue),
    )

  "LedgerTimeValidator" can {
    "when the record time is not available" should {
      "continue" in {
        val result = ledgerTimeValidationStep.apply(
          contextWithTimeModelAndEmptyCommandDeduplication(),
          aTransactionEntrySummary,
        )

        result match {
          case StepContinue(_) => succeed
          case StepStop(_) => fail()
        }
      }

      "compute and correctly set the min/max ledger time and out-of-time-bounds log entry without deduplicateUntil" in {
        val context = contextWithTimeModelAndEmptyCommandDeduplication()

        ledgerTimeValidationStep.apply(
          context,
          aDamlTransactionEntrySummaryWithSubmissionAndLedgerEffectiveTimes,
        )

        context.minimumRecordTime shouldEqual Some(
          Timestamp.assertFromInstant(
            Instant.ofEpochSecond(2).minus(theDefaultConfig.timeModel.minSkew)
          )
        )
        context.maximumRecordTime shouldEqual Some(
          Timestamp.assertFromInstant(
            Instant.ofEpochSecond(1).plus(theDefaultConfig.timeModel.maxSkew)
          )
        )
        context.deduplicateUntil shouldBe empty
        context.outOfTimeBoundsLogEntry should not be empty
        context.outOfTimeBoundsLogEntry.foreach { actualOutOfTimeBoundsLogEntry =>
          actualOutOfTimeBoundsLogEntry.hasTransactionRejectionEntry shouldBe true
          actualOutOfTimeBoundsLogEntry.getTransactionRejectionEntry.hasRecordTimeOutOfRange shouldBe true
        }
      }

      "compute and correctly set the min/max ledger time and out-of-time-bounds log entry with deduplicateUntil" in {
        val context = contextWithTimeModelAndCommandDeduplication()

        ledgerTimeValidationStep.apply(
          context,
          aDamlTransactionEntrySummaryWithSubmissionAndLedgerEffectiveTimes,
        )

        context.minimumRecordTime shouldEqual Some(
          Timestamp.assertFromInstant(Instant.ofEpochSecond(3).plus(Timestamp.Resolution))
        )
        context.maximumRecordTime shouldEqual Some(
          Timestamp.assertFromInstant(
            Instant.ofEpochSecond(1).plus(theDefaultConfig.timeModel.maxSkew)
          )
        )
        context.deduplicateUntil shouldEqual Some(
          Timestamp.assertFromInstant(Instant.ofEpochSecond(aDeduplicateUntil.getSeconds))
        )
        context.outOfTimeBoundsLogEntry should not be empty
        context.outOfTimeBoundsLogEntry.foreach { actualOutOfTimeBoundsLogEntry =>
          actualOutOfTimeBoundsLogEntry.hasTransactionRejectionEntry shouldBe true
          actualOutOfTimeBoundsLogEntry.getTransactionRejectionEntry.hasRecordTimeOutOfRange shouldBe true
        }
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

    "mark config key as accessed in context" in {
      val commitContext =
        createCommitContext(recordTime = None, inputWithTimeModelAndCommandDeduplication)

      ledgerTimeValidationStep.apply(commitContext, aTransactionEntrySummary)

      commitContext.getAccessedInputKeys should contain(configurationStateKey)
    }
  }

  private def createProtobufTimestamp(seconds: Long) =
    Conversions.buildTimestamp(Timestamp.assertFromInstant(Instant.ofEpochSecond(seconds)))

  private def contextWithTimeModelAndEmptyCommandDeduplication() =
    createCommitContext(recordTime = None, inputs = inputWithTimeModelAndEmptyCommandDeduplication)

  private def contextWithTimeModelAndCommandDeduplication() =
    createCommitContext(recordTime = None, inputs = inputWithTimeModelAndCommandDeduplication)

  private def defaultConfigurationStateValueBuilder(): DamlStateValue.Builder =
    DamlStateValue.newBuilder
      .setConfigurationEntry(
        DamlConfigurationEntry.newBuilder
          .setConfiguration(Configuration.encode(theDefaultConfig))
      )
}
