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
import com.daml.ledger.participant.state.v1.{Configuration, RejectionReason}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.{Engine, ReplayMismatch}
import com.daml.lf.transaction.{
  NodeId,
  RecordedNodeMissing,
  ReplayNodeMismatch,
  ReplayedNodeMissing,
  Transaction
}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.Inspectors.forEvery

class TransactionCommitterSpec extends WordSpec with Matchers with MockitoSugar {
  private val metrics = new Metrics(new MetricRegistry)
  private val aDamlTransactionEntry = DamlTransactionEntry.newBuilder
    .setTransaction(Conversions.encodeTransaction(TransactionBuilder.Empty))
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
          contextWithTimeModelAndEmptyCommandDeduplication(),
          aTransactionEntrySummary)

        result match {
          case StepContinue(_) => succeed
          case StepStop(_) => fail
        }
      }

      "compute and correctly set the min/max ledger time and out-of-time-bounds log entry without deduplicateUntil" in {
        val context = contextWithTimeModelAndEmptyCommandDeduplication()
        instance.validateLedgerTime(
          context,
          aDamlTransactionEntrySummaryWithSubmissionAndLedgerEffectiveTimes)
        context.minimumRecordTime shouldEqual Some(Instant.ofEpochSecond(-28))
        context.maximumRecordTime shouldEqual Some(Instant.ofEpochSecond(31))
        context.deduplicateUntil shouldBe empty
        context.outOfTimeBoundsLogEntry should not be empty
        context.outOfTimeBoundsLogEntry.foreach { actualOutOfTimeBoundsLogEntry =>
          actualOutOfTimeBoundsLogEntry.hasTransactionRejectionEntry shouldBe true
          actualOutOfTimeBoundsLogEntry.getTransactionRejectionEntry.hasInvalidLedgerTime shouldBe true
        }
      }

      "compute and correctly set the min/max ledger time and out-of-time-bounds log entry with deduplicateUntil" in {
        val context = contextWithTimeModelAndCommandDeduplication()
        instance.validateLedgerTime(
          context,
          aDamlTransactionEntrySummaryWithSubmissionAndLedgerEffectiveTimes)
        context.minimumRecordTime shouldEqual Some(
          Instant.ofEpochSecond(3).plus(Timestamp.Resolution))
        context.maximumRecordTime shouldEqual Some(Instant.ofEpochSecond(31))
        context.deduplicateUntil shouldEqual Some(
          Instant.ofEpochSecond(aDeduplicateUntil.getSeconds)
        )
        context.outOfTimeBoundsLogEntry should not be empty
        context.outOfTimeBoundsLogEntry.foreach { actualOutOfTimeBoundsLogEntry =>
          actualOutOfTimeBoundsLogEntry.hasTransactionRejectionEntry shouldBe true
          actualOutOfTimeBoundsLogEntry.getTransactionRejectionEntry.hasInvalidLedgerTime shouldBe true
        }
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

  "blind" should {
    "always set blindingInfo" in {
      val context = new FakeCommitContext(recordTime = None)

      val actual = instance.blind(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(_) => fail
        case StepStop(logEntry) =>
          logEntry.hasTransactionEntry shouldBe true
          logEntry.getTransactionEntry.hasBlindingInfo shouldBe true
      }
    }
  }

  "rejectionReasonForValidationError" should {
    "report key lookup errors as Inconsistent unless the contracts are created in the same transaction" in {

      val maintainer = "maintainer"
      val key = "key"
      val dummyValue = TransactionBuilder.record("field" -> "value")

      def create(contractId: String, key: String = key): TransactionBuilder.Create =
        TransactionBuilder.create(
          id = contractId,
          template = "dummyPackage:DummyModule:DummyTemplate",
          argument = dummyValue,
          signatories = Seq(maintainer),
          observers = Seq.empty,
          key = Some(key)
        )

      def mkMismatch(
          recorded: (Transaction.Transaction, NodeId),
          replayed: (Transaction.Transaction, NodeId))
        : ReplayNodeMismatch[NodeId, Value.ContractId] =
        ReplayNodeMismatch(recorded._1, recorded._2, replayed._1, replayed._2)
      def mkRecordedMissing(
          recorded: Transaction.Transaction,
          replayed: (Transaction.Transaction, NodeId))
        : RecordedNodeMissing[NodeId, Value.ContractId] =
        RecordedNodeMissing(recorded, replayed._1, replayed._2)
      def mkReplayedMissing(
          recorded: (Transaction.Transaction, NodeId),
          replayed: Transaction.Transaction): ReplayedNodeMissing[NodeId, Value.ContractId] =
        ReplayedNodeMissing(recorded._1, recorded._2, replayed)

      val createInput = create("#inputContractId")
      val create1 = create("#someContractId")
      val create2 = create("#otherContractId")

      val exercise = TransactionBuilder.exercise(
        contract = createInput,
        choice = "DummyChoice",
        consuming = false,
        actingParties = Set(maintainer),
        argument = dummyValue,
        byKey = false
      )
      val otherKeyCreate = create("#contractWithOtherKey", "otherKey")

      val lookupNodes @ Seq(lookup1, lookup2, lookupNone, lookupOther @ _) =
        Seq(create1 -> true, create2 -> true, create1 -> false, otherKeyCreate -> true) map {
          case (create, found) => TransactionBuilder.lookupByKey(create, found)
        }
      val Seq(tx1, tx2, txNone, txOther) = lookupNodes map { node =>
        val builder = TransactionBuilder()
        val rootId = builder.add(exercise)
        val lookupId = builder.add(node, rootId)
        builder.build() -> lookupId
      }

      val inconsistentLookups = Seq(
        mkMismatch(tx1, tx2),
        mkMismatch(tx1, txNone),
        mkMismatch(txNone, tx2),
      )
      forEvery(inconsistentLookups) { mismatch =>
        val replayMismatch = ReplayMismatch(mismatch)
        instance.rejectionReasonForValidationError(replayMismatch) shouldBe
          RejectionReason.Inconsistent(replayMismatch.msg)
      }

      val Seq(txC1, txC2, txCNone) = Seq(lookup1, lookup2, lookupNone) map { node =>
        val builder = TransactionBuilder()
        val rootId = builder.add(exercise)
        builder.add(create1, rootId)
        val lookupId = builder.add(node, rootId)
        builder.build() -> lookupId
      }
      val Seq(tx1C, txNoneC) = Seq(lookup1, lookupNone) map { node =>
        val builder = TransactionBuilder()
        val rootId = builder.add(exercise)
        val lookupId = builder.add(node, rootId)
        builder.add(create1)
        builder.build() -> lookupId
      }

      // These lookups are disputed because the recorded transaction is key-inconsistent.
      val recordedKeyInconsistent = Seq(
        mkMismatch(txC2, txC1),
        mkMismatch(txCNone, txC1),
        mkMismatch(txC1, txCNone),
        mkMismatch(tx1C, txNoneC),
      )

      val txExerciseOnly = {
        val builder = TransactionBuilder()
        builder.add(exercise)
        builder.build()
      }
      val txCreate = {
        val builder = TransactionBuilder()
        val rootId = builder.add(exercise)
        val createId = builder.add(create1, rootId)
        builder.build() -> createId
      }
      val miscMismatches = Seq(
        mkMismatch(txOther, tx1), // wrong key
        mkMismatch(txCreate, tx1C), // wrong node type
        mkRecordedMissing(txExerciseOnly, tx2),
        mkReplayedMissing(tx1, txExerciseOnly),
      )
      val disputedLookups = recordedKeyInconsistent ++ miscMismatches
      forEvery(disputedLookups) { mismatch =>
        val replayMismatch = ReplayMismatch(mismatch)
        instance.rejectionReasonForValidationError(replayMismatch) shouldBe RejectionReason
          .Disputed(replayMismatch.msg)
      }
    }
  }

  private def createTransactionCommitter(): TransactionCommitter =
    new TransactionCommitter(theDefaultConfig, mock[Engine], metrics, inStaticTimeMode = false)

  private def contextWithTimeModelAndEmptyCommandDeduplication() =
    new FakeCommitContext(
      recordTime = None,
      inputs = inputWithTimeModelAndEmptyCommandDeduplication)

  private def contextWithTimeModelAndCommandDeduplication() =
    new FakeCommitContext(recordTime = None, inputs = inputWithTimeModelAndCommandDeduplication)

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
