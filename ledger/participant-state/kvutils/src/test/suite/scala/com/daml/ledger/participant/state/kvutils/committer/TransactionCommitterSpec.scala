// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import java.time.Instant

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.TestHelpers._
import com.daml.ledger.participant.state.kvutils.committer.TransactionCommitter.DamlTransactionEntrySummary
import com.daml.ledger.participant.state.v1.Configuration
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class TransactionCommitterSpec extends WordSpec with Matchers with MockitoSugar {
  private val metrics = new Metrics(new MetricRegistry)
  private val aDamlTransactionEntry = DamlTransactionEntry.newBuilder
    .setSubmitterInfo(
      DamlSubmitterInfo.newBuilder
        .setCommandId("commandId")
        .setSubmitter("aSubmitter"))
    .setSubmissionSeed(ByteString.copyFromUtf8("a" * 32))
    .build
  private val aTransactionEntrySummary = DamlTransactionEntrySummary(aDamlTransactionEntry)
  private val aRecordTime = Timestamp(100)
  private val instance = createTransactionCommitter() // Stateless, can be shared between tests
  private val dedupKey = Conversions
    .commandDedupKey(aTransactionEntrySummary.submitterInfo)
  private val configurationStateValue = defaultConfigurationStateValueBuilder().build
  private val inputWithTimeModelAndEmptyCommandDeduplication =
    Map(Conversions.configurationStateKey -> Some(configurationStateValue), dedupKey -> None)
  private val contextWithTimeModelAndEmptyCommandDeduplication = new FakeCommitContext(
    recordTime = None,
    inputs = inputWithTimeModelAndEmptyCommandDeduplication)
  private val aSubmissionTime = createProtobufTimestamp(seconds = 1)
  private val aLedgerEffectiveTime = createProtobufTimestamp(seconds = 2)
  private val aDamlTransactionEntryWithSubmissionAndLedgerEffectiveTimes =
    aDamlTransactionEntry.toBuilder
      .setSubmissionTime(aSubmissionTime)
      .setLedgerEffectiveTime(aLedgerEffectiveTime)
      .build()
  private val aDamlTransactionEntrySummaryWithSubmissionAndLedgerEffectiveTimes =
    DamlTransactionEntrySummary(aDamlTransactionEntryWithSubmissionAndLedgerEffectiveTimes)
  private val aDeduplicateUntil = createProtobufTimestamp(seconds = 3)
  private val dedupValue = DamlStateValue.newBuilder
    .setCommandDedup(DamlCommandDedupValue.newBuilder.setDeduplicatedUntil(aDeduplicateUntil))
    .build()
  private val inputWithTimeModelAndCommandDeduplication =
    Map(
      Conversions.configurationStateKey -> Some(configurationStateValue),
      dedupKey -> Some(dedupValue))
  private val contextWithTimeModelAndCommandDeduplication =
    new FakeCommitContext(recordTime = None, inputs = inputWithTimeModelAndCommandDeduplication)

  "deduplicateCommand" should {
    "continue if record time is not available" in {
      val context = new FakeCommitContext(recordTime = None)

      val actual = instance.deduplicateCommand(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail
      }
    }

    "continue if record time is available but no deduplication entry could be found" in {
      val inputs = Map(dedupKey -> None)
      val context =
        new FakeCommitContext(recordTime = Some(aRecordTime), inputs = inputs)

      val actual = instance.deduplicateCommand(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail
      }
    }

    "continue if record time is after deduplication time in case a deduplication entry is found" in {
      val dedupValue = newDedupValue(aRecordTime)
      val inputs = Map(dedupKey -> Some(dedupValue))
      val context =
        new FakeCommitContext(recordTime = Some(aRecordTime.addMicros(1)), inputs = inputs)

      val actual = instance.deduplicateCommand(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail
      }
    }

    "produce rejection log entry in case record time is on or before deduplication time" in {
      for ((recordTime, deduplicationTime) <- Iterable(
          (aRecordTime, aRecordTime),
          (aRecordTime, aRecordTime.addMicros(1)))) {
        val dedupValue = newDedupValue(deduplicationTime)
        val inputs = Map(dedupKey -> Some(dedupValue))
        val context =
          new FakeCommitContext(recordTime = Some(recordTime), inputs = inputs)

        val actual = instance.deduplicateCommand(context, aTransactionEntrySummary)

        actual match {
          case StepContinue(_) => fail
          case StepStop(actualLogEntry) =>
            actualLogEntry.hasTransactionRejectionEntry shouldBe true
        }
      }
    }
  }

  "validateLedgerTime" can {
    "when the record time is not available" should {
      "continue" in {
        val result = instance.validateLedgerTime(
          contextWithTimeModelAndEmptyCommandDeduplication,
          aTransactionEntrySummary)

        result match {
          case StepContinue(_) => succeed
          case StepStop(_) => fail
        }
      }

      "compute and correctly set the min/max ledger time without deduplicateUntil" in {
        instance.validateLedgerTime(
          contextWithTimeModelAndEmptyCommandDeduplication,
          aDamlTransactionEntrySummaryWithSubmissionAndLedgerEffectiveTimes)
        contextWithTimeModelAndEmptyCommandDeduplication.minimumRecordTime shouldEqual Some(
          Instant.ofEpochSecond(-28))
        contextWithTimeModelAndEmptyCommandDeduplication.maximumRecordTime shouldEqual Some(
          Instant.ofEpochSecond(31))
        contextWithTimeModelAndEmptyCommandDeduplication.deduplicateUntil shouldBe empty
      }

      "compute and correctly set the min/max ledger time with deduplicateUntil" in {
        instance.validateLedgerTime(
          contextWithTimeModelAndCommandDeduplication,
          aDamlTransactionEntrySummaryWithSubmissionAndLedgerEffectiveTimes)
        contextWithTimeModelAndCommandDeduplication.minimumRecordTime shouldEqual Some(
          Instant.ofEpochSecond(3).plus(Timestamp.Resolution))
        contextWithTimeModelAndCommandDeduplication.maximumRecordTime shouldEqual Some(
          Instant.ofEpochSecond(31))
        contextWithTimeModelAndCommandDeduplication.deduplicateUntil shouldEqual Some(
          Instant.ofEpochSecond(aDeduplicateUntil.getSeconds)
        )
      }
    }

    "produce rejection log entry if record time is outside of ledger effective time bounds" in {
      val recordTime = Timestamp.now()
      val recordTimeInstant = recordTime.toInstant
      val lowerBound =
        recordTimeInstant.minus(theDefaultConfig.timeModel.minSkew).minusMillis(1)
      val upperBound =
        recordTimeInstant.plus(theDefaultConfig.timeModel.maxSkew).plusMillis(1)
      val inputWithDeclaredConfig =
        Map(Conversions.configurationStateKey -> Some(configurationStateValue))

      for (ledgerEffectiveTime <- Iterable(lowerBound, upperBound)) {
        val context =
          new FakeCommitContext(recordTime = Some(recordTime), inputs = inputWithDeclaredConfig)
        val transactionEntrySummary = DamlTransactionEntrySummary(
          aDamlTransactionEntry.toBuilder
            .setLedgerEffectiveTime(
              com.google.protobuf.Timestamp.newBuilder
                .setSeconds(ledgerEffectiveTime.getEpochSecond)
                .setNanos(ledgerEffectiveTime.getNano))
            .build)
        val actual = instance.validateLedgerTime(context, transactionEntrySummary)

        actual match {
          case StepContinue(_) => fail
          case StepStop(actualLogEntry) =>
            actualLogEntry.hasTransactionRejectionEntry shouldBe true
        }
      }
    }
  }

  "buildLogEntry" should {
    "set record time in log entry when it is available" in {
      val context = new FakeCommitContext(recordTime = Some(theRecordTime))

      val actual = instance.buildLogEntry(aTransactionEntrySummary, context)

      actual.hasRecordTime shouldBe true
      actual.getRecordTime shouldBe buildTimestamp(theRecordTime)
      actual.hasTransactionEntry shouldBe true
      actual.getTransactionEntry shouldBe aTransactionEntrySummary.submission
    }

    "skip setting record time in log entry when it is not available" in {
      val context = new FakeCommitContext(recordTime = None)

      val actual = instance.buildLogEntry(aTransactionEntrySummary, context)

      actual.hasRecordTime shouldBe false
      actual.hasTransactionEntry shouldBe true
      actual.getTransactionEntry shouldBe aTransactionEntrySummary.submission
    }

    "produce an out-of-time-bounds rejection log entry in case pre-execution is enabled" in {
      val context = new FakeCommitContext(recordTime = None)

      val _ = instance.buildLogEntry(aTransactionEntrySummary, context)

      context.preExecute shouldBe true
      context.outOfTimeBoundsLogEntry should not be empty
      context.outOfTimeBoundsLogEntry.foreach { actual =>
        actual.hasRecordTime shouldBe false
        actual.hasTransactionRejectionEntry shouldBe true
        actual.getTransactionRejectionEntry.getSubmitterInfo shouldBe aTransactionEntrySummary.submitterInfo
      }
    }

    "not set an out-of-time-bounds rejection log entry in case pre-execution is disabled" in {
      val context = new FakeCommitContext(recordTime = Some(aRecordTime))

      val _ = instance.buildLogEntry(aTransactionEntrySummary, context)

      context.preExecute shouldBe false
      context.outOfTimeBoundsLogEntry shouldBe empty
    }
  }

  private def createTransactionCommitter(): TransactionCommitter =
    new TransactionCommitter(theDefaultConfig, mock[Engine], metrics, inStaticTimeMode = false)

  private def newDedupValue(deduplicationTime: Timestamp): DamlStateValue =
    DamlStateValue.newBuilder
      .setCommandDedup(
        DamlCommandDedupValue.newBuilder.setDeduplicatedUntil(buildTimestamp(deduplicationTime)))
      .build

  private def createProtobufTimestamp(seconds: Long) =
    Conversions.buildTimestamp(Timestamp.assertFromInstant(Instant.ofEpochSecond(seconds)))

  private def defaultConfigurationStateValueBuilder(): DamlStateValue.Builder =
    DamlStateValue.newBuilder
      .setConfigurationEntry(
        DamlConfigurationEntry.newBuilder
          .setConfiguration(Configuration.encode(theDefaultConfig))
      )
}
