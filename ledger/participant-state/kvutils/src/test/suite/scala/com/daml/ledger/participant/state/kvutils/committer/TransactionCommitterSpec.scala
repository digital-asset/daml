// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

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
  private val dedupKey = Conversions
    .commandDedupKey(aTransactionEntrySummary.submitterInfo)

  "deduplicateCommand" should {
    "continue if record time is not available" in {
      val instance = createTransactionCommitter()
      val context = new FakeCommitContext(recordTime = None)

      val actual = instance.deduplicateCommand(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail
      }
    }

    "continue if record time is available but no deduplication entry could be found" in {
      val instance = createTransactionCommitter()
      val inputs = Map(dedupKey -> (None -> ByteString.EMPTY))
      val context =
        new FakeCommitContext(recordTime = Some(aRecordTime), inputsWithFingerprints = inputs)

      val actual = instance.deduplicateCommand(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail
      }
    }

    "continue if record time is after deduplication time in case a deduplication entry is found" in {
      val instance = createTransactionCommitter()
      val dedupValue = newDedupValue(aRecordTime)
      val inputs =
        Map(dedupKey -> (Some(dedupValue) -> dedupValue.toByteString))
      val context =
        new FakeCommitContext(
          recordTime = Some(aRecordTime.addMicros(1)),
          inputsWithFingerprints = inputs)

      val actual = instance.deduplicateCommand(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail
      }
    }

    "produce rejection log entry in case record time is on or before deduplication time" in {
      val instance = createTransactionCommitter()
      for ((recordTime, deduplicationTime) <- Iterable(
          (aRecordTime, aRecordTime),
          (aRecordTime, aRecordTime.addMicros(1)))) {
        val dedupValue = newDedupValue(deduplicationTime)
        val inputs =
          Map(dedupKey -> (Some(dedupValue) -> dedupValue.toByteString))
        val context =
          new FakeCommitContext(recordTime = Some(recordTime), inputsWithFingerprints = inputs)

        val actual = instance.deduplicateCommand(context, aTransactionEntrySummary)

        actual match {
          case StepContinue(_) => fail
          case StepStop(actualLogEntry) =>
            actualLogEntry.hasTransactionRejectionEntry shouldBe true
        }
      }
    }
  }

  "validateLedgerTime" should {
    "continue without accessing ledger configuration if record time is not available" in {
      val instance = createTransactionCommitter()
      val context = new FakeCommitContext(recordTime = None)
      val actual = instance.validateLedgerTime(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail
      }
    }

    "produce rejection log entry if record time is outside of ledger effective time bounds" in {
      val instance = createTransactionCommitter()
      val recordTime = Timestamp.now()
      val recordTimeInstant = recordTime.toInstant
      val lowerBound =
        recordTimeInstant.minus(theDefaultConfig.timeModel.minSkew).minusMillis(1)
      val upperBound =
        recordTimeInstant.plus(theDefaultConfig.timeModel.maxSkew).plusMillis(1)
      val configurationStateValue = DamlStateValue.newBuilder
        .setConfigurationEntry(
          DamlConfigurationEntry.newBuilder
            .setConfiguration(Configuration.encode(theDefaultConfig))
        )
        .build
      val inputWithDeclaredConfig =
        Map(
          Conversions.configurationStateKey -> (Some(configurationStateValue) -> configurationStateValue.toByteString))

      for (ledgerEffectiveTime <- Iterable(lowerBound, upperBound)) {
        val context =
          new FakeCommitContext(
            recordTime = Some(recordTime),
            inputsWithFingerprints = inputWithDeclaredConfig)
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

  private def createTransactionCommitter(): TransactionCommitter =
    new TransactionCommitter(theDefaultConfig, mock[Engine], metrics, inStaticTimeMode = false)

  private def newDedupValue(deduplicationTime: Timestamp): DamlStateValue =
    DamlStateValue.newBuilder
      .setCommandDedup(
        DamlCommandDedupValue.newBuilder.setDeduplicatedUntil(buildTimestamp(deduplicationTime)))
      .build
}
