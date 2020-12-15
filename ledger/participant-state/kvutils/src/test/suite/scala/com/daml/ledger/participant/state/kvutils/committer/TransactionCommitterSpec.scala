// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import java.time.Instant

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.Conversions.configurationStateKey
import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.Err.MissingInputState
import com.daml.ledger.participant.state.kvutils.TestHelpers._
import com.daml.ledger.participant.state.kvutils.committer.TransactionCommitter.DamlTransactionEntrySummary
import com.daml.ledger.participant.state.v1.{Configuration, RejectionReason}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.{Engine, ReplayMismatch}
import com.daml.lf.transaction
import com.daml.lf.transaction.{
  NodeId,
  RecordedNodeMissing,
  ReplayNodeMismatch,
  ReplayedNodeMissing,
  Transaction,
  TransactionOuterClass
}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.Inspectors.forEvery

import scala.collection.JavaConverters._

class TransactionCommitterSpec extends AnyWordSpec with Matchers with MockitoSugar {
  private[this] val txBuilder = TransactionBuilder()
  private val metrics = new Metrics(new MetricRegistry)
  private val aDamlTransactionEntry = createTransactionEntry(List("aSubmitter"))
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
  private val aRichTransactionTreeSummary = {
    val roots = Seq("Exercise-1", "Fetch-1", "LookupByKey-1", "Create-1")
    val nodes: Seq[TransactionOuterClass.Node] = Seq(
      createNode("Fetch-1")(_.setFetch(fetchNodeBuilder)),
      createNode("LookupByKey-1")(_.setLookupByKey(lookupByKeyNodeBuilder)),
      createNode("Create-1")(_.setCreate(createNodeBuilder)),
      createNode("LookupByKey-2")(_.setLookupByKey(lookupByKeyNodeBuilder)),
      createNode("Fetch-2")(_.setFetch(fetchNodeBuilder)),
      createNode("Create-2")(_.setCreate(createNodeBuilder)),
      createNode("Fetch-3")(_.setFetch(fetchNodeBuilder)),
      createNode("Create-3")(_.setCreate(createNodeBuilder)),
      createNode("LookupByKey-3")(_.setLookupByKey(lookupByKeyNodeBuilder)),
      createNode("Exercise-2")(
        _.setExercise(
          exerciseNodeBuilder.addAllChildren(
            Seq("Fetch-3", "Create-3", "LookupByKey-3").asJava
          ))),
      createNode("Exercise-1")(
        _.setExercise(
          exerciseNodeBuilder.addAllChildren(
            Seq("LookupByKey-2", "Fetch-2", "Create-2", "Exercise-2").asJava
          )))
    )
    val tx = TransactionOuterClass.Transaction
      .newBuilder()
      .addAllRoots(roots.asJava)
      .addAllNodes(nodes.asJava)
      .build()
    val outTx = aDamlTransactionEntry.toBuilder.setTransaction(tx).build()
    DamlTransactionEntrySummary(outTx)
  }

  private def createNode(nodeId: String)(
      nodeImpl: TransactionOuterClass.Node.Builder => TransactionOuterClass.Node.Builder) =
    nodeImpl(TransactionOuterClass.Node.newBuilder().setNodeId(nodeId)).build()

  private def fetchNodeBuilder = TransactionOuterClass.NodeFetch.newBuilder()

  private def exerciseNodeBuilder = TransactionOuterClass.NodeExercise.newBuilder()

  private def createNodeBuilder = TransactionOuterClass.NodeCreate.newBuilder()

  private def lookupByKeyNodeBuilder = TransactionOuterClass.NodeLookupByKey.newBuilder()

  "trimUnnecessaryNodes" should {
    "remove `Fetch` and `LookupByKey` nodes from transaction tree" in {
      val context = createCommitContext(recordTime = None)

      instance.trimUnnecessaryNodes(context, aRichTransactionTreeSummary) match {
        case StepContinue(logEntry) =>
          val transaction = logEntry.submission.getTransaction
          transaction.getRootsList.asScala should contain theSameElementsInOrderAs Seq(
            "Exercise-1",
            "Create-1")
          val nodes = transaction.getNodesList.asScala
          nodes.map(_.getNodeId) should contain theSameElementsInOrderAs Seq(
            "Create-1",
            "Create-2",
            "Create-3",
            "Exercise-2",
            "Exercise-1")
          nodes(3).getExercise.getChildrenList.asScala should contain theSameElementsInOrderAs Seq(
            "Create-3")
          nodes(4).getExercise.getChildrenList.asScala should contain theSameElementsInOrderAs Seq(
            "Create-2",
            "Exercise-2")
        case StepStop(_) => fail("should be StepContinue")
      }
    }
  }

  "deduplicateCommand" should {
    "continue if record time is not available" in {
      val context = createCommitContext(recordTime = None)

      val actual = instance.deduplicateCommand(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail
      }
    }

    "continue if record time is available but no deduplication entry could be found" in {
      val inputs = Map(dedupKey -> None)
      val context =
        createCommitContext(recordTime = Some(aRecordTime), inputs = inputs)

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
        createCommitContext(recordTime = Some(aRecordTime.addMicros(1)), inputs = inputs)

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
          createCommitContext(recordTime = Some(recordTime), inputs = inputs)

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
          createCommitContext(recordTime = Some(recordTime), inputs = inputWithDeclaredConfig)
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

    "mark config key as accessed in context" in {
      val commitContext =
        createCommitContext(recordTime = None, inputWithTimeModelAndCommandDeduplication)

      val _ = instance.validateLedgerTime(commitContext, aTransactionEntrySummary)

      commitContext.getAccessedInputKeys should contain(configurationStateKey)
    }
  }

  "buildLogEntry" should {
    "set record time in log entry when it is available" in {
      val context = createCommitContext(recordTime = Some(theRecordTime))

      val actual = instance.buildLogEntry(aTransactionEntrySummary, context)

      actual.hasRecordTime shouldBe true
      actual.getRecordTime shouldBe buildTimestamp(theRecordTime)
      actual.hasTransactionEntry shouldBe true
      actual.getTransactionEntry shouldBe aTransactionEntrySummary.submission
    }

    "skip setting record time in log entry when it is not available" in {
      val context = createCommitContext(recordTime = None)

      val actual = instance.buildLogEntry(aTransactionEntrySummary, context)

      actual.hasRecordTime shouldBe false
      actual.hasTransactionEntry shouldBe true
      actual.getTransactionEntry shouldBe aTransactionEntrySummary.submission
    }

    "produce an out-of-time-bounds rejection log entry in case pre-execution is enabled" in {
      val context = createCommitContext(recordTime = None)

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
      val context = createCommitContext(recordTime = Some(aRecordTime))

      val _ = instance.buildLogEntry(aTransactionEntrySummary, context)

      context.preExecute shouldBe false
      context.outOfTimeBoundsLogEntry shouldBe empty
    }
  }

  "blind" should {
    "always set blindingInfo" in {
      val context = createCommitContext(recordTime = None)

      val actual = instance.blind(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(partialResult) =>
          partialResult.submission.hasBlindingInfo shouldBe true
        case StepStop(_) => fail
      }
    }
  }

  "rejectionReasonForValidationError" when {
    val maintainer = "maintainer"
    val dummyValue = TransactionBuilder.record("field" -> "value")

    def create(contractId: String, key: String = "key"): TransactionBuilder.Create =
      txBuilder.create(
        id = contractId,
        template = "dummyPackage:DummyModule:DummyTemplate",
        argument = dummyValue,
        signatories = Seq(maintainer),
        observers = Seq.empty,
        key = Some(key)
      )

    def mkMismatch(
        recorded: (Transaction.Transaction, NodeId),
        replayed: (Transaction.Transaction, NodeId)): ReplayNodeMismatch[NodeId, Value.ContractId] =
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

    def checkRejectionReason(mkReason: String => RejectionReason)(
        mismatch: transaction.ReplayMismatch[NodeId, Value.ContractId]) = {
      val replayMismatch = ReplayMismatch(mismatch)
      instance.rejectionReasonForValidationError(replayMismatch) shouldBe mkReason(
        replayMismatch.msg)
    }

    val createInput = create("#inputContractId")
    val create1 = create("#someContractId")
    val create2 = create("#otherContractId")

    val exercise = txBuilder.exercise(
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
        case (create, found) => txBuilder.lookupByKey(create, found)
      }
    val Seq(tx1, tx2, txNone, txOther) = lookupNodes map { node =>
      val builder = TransactionBuilder()
      val rootId = builder.add(exercise)
      val lookupId = builder.add(node, rootId)
      builder.build() -> lookupId
    }

    "there is a mismatch in lookupByKey nodes" should {
      "report an inconsistency if the contracts are not created in the same transaction" in {
        val inconsistentLookups = Seq(
          mkMismatch(tx1, tx2),
          mkMismatch(tx1, txNone),
          mkMismatch(txNone, tx2),
        )
        forEvery(inconsistentLookups)(checkRejectionReason(RejectionReason.Inconsistent))
      }

      "report Disputed if one of contracts is created in the same transaction" in {
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
        val recordedKeyInconsistent = Seq(
          mkMismatch(txC2, txC1),
          mkMismatch(txCNone, txC1),
          mkMismatch(txC1, txCNone),
          mkMismatch(tx1C, txNoneC),
        )
        forEvery(recordedKeyInconsistent)(checkRejectionReason(RejectionReason.Disputed))
      }

      "report Disputed if the keys are different" in {
        checkRejectionReason(RejectionReason.Disputed)(mkMismatch(txOther, tx1))
      }
    }

    "the mismatch is not between two lookup nodes" should {
      "report Disputed" in {
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
          mkMismatch(txCreate, tx1),
          mkRecordedMissing(txExerciseOnly, tx2),
          mkReplayedMissing(tx1, txExerciseOnly),
        )
        forEvery(miscMismatches)(checkRejectionReason(RejectionReason.Disputed))
      }
    }
  }

  "authorizeSubmitters" should {
    "reject a submission when any of the submitters keys is not present in the input state" in {
      val context = createCommitContext(
        recordTime = None,
        inputs = createInputs(
          Alice -> Some(hostedParty(Alice)),
          Bob -> Some(hostedParty(Bob)),
        ),
        participantId = ParticipantId
      )
      val tx = DamlTransactionEntrySummary(createTransactionEntry(List(Alice, Bob, Emma)))

      assertThrows[MissingInputState](instance.authorizeSubmitters.apply(context, tx))
    }

    "reject a submission when any of the submitters is not known" in {
      val context = createCommitContext(
        recordTime = None,
        inputs = createInputs(
          Alice -> Some(hostedParty(Alice)),
          Bob -> None,
        ),
        participantId = ParticipantId
      )
      val tx = DamlTransactionEntrySummary(createTransactionEntry(List(Alice, Bob)))

      val result = instance.authorizeSubmitters.apply(context, tx)
      result shouldBe a[StepStop]
      val rejectionReason = result
        .asInstanceOf[StepStop]
        .logEntry
        .getTransactionRejectionEntry
        .getPartyNotKnownOnLedger
        .getDetails
      rejectionReason should fullyMatch regex """Submitting party .+ not known"""
    }

    "reject a submission when any of the submitters' participant id is incorrect" in {
      val context = createCommitContext(
        recordTime = None,
        inputs = createInputs(
          Alice -> Some(hostedParty(Alice)),
          Bob -> Some(notHostedParty(Bob)),
        ),
        participantId = ParticipantId
      )
      val tx = DamlTransactionEntrySummary(createTransactionEntry(List(Alice, Bob)))

      val result = instance.authorizeSubmitters.apply(context, tx)
      result shouldBe a[StepStop]
      val rejectionReason = result
        .asInstanceOf[StepStop]
        .logEntry
        .getTransactionRejectionEntry
        .getSubmitterCannotActViaParticipant
        .getDetails
      rejectionReason should fullyMatch regex s"""Party .+ not hosted by participant ${mkParticipantId(
        ParticipantId)}"""
    }

    "allow a submission when all of the submitters are hosted on the participant" in {
      val context = createCommitContext(
        recordTime = None,
        inputs = createInputs(
          Alice -> Some(hostedParty(Alice)),
          Bob -> Some(hostedParty(Bob)),
          Emma -> Some(hostedParty(Emma)),
        ),
        participantId = ParticipantId
      )
      val tx = DamlTransactionEntrySummary(createTransactionEntry(List(Alice, Bob, Emma)))

      instance.authorizeSubmitters.apply(context, tx) shouldBe a[StepContinue[_]]
    }

    lazy val Alice = "alice"
    lazy val Bob = "bob"
    lazy val Emma = "emma"
    lazy val ParticipantId = 0
    lazy val OtherParticipantId = 1
    def partyAllocation(party: String, participantId: Int): DamlPartyAllocation =
      DamlPartyAllocation
        .newBuilder()
        .setParticipantId(mkParticipantId(participantId))
        .setDisplayName(party)
        .build()

    def hostedParty(party: String): DamlPartyAllocation = partyAllocation(party, ParticipantId)
    def notHostedParty(party: String): DamlPartyAllocation =
      partyAllocation(party, OtherParticipantId)
    def createInputs(
        inputs: (String, Option[DamlPartyAllocation])*): Map[DamlStateKey, Option[DamlStateValue]] =
      inputs.map {
        case (party, partyAllocation) =>
          DamlStateKey.newBuilder().setParty(party).build() -> partyAllocation.map(
            DamlStateValue.newBuilder().setParty(_).build())
      }.toMap
  }

  private def createTransactionCommitter(): TransactionCommitter =
    new TransactionCommitter(theDefaultConfig, mock[Engine], metrics, inStaticTimeMode = false)

  private def contextWithTimeModelAndEmptyCommandDeduplication() =
    createCommitContext(recordTime = None, inputs = inputWithTimeModelAndEmptyCommandDeduplication)

  private def contextWithTimeModelAndCommandDeduplication() =
    createCommitContext(recordTime = None, inputs = inputWithTimeModelAndCommandDeduplication)

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

  private def createTransactionEntry(submitters: List[String]): DamlTransactionEntry = {
    DamlTransactionEntry.newBuilder
      .setTransaction(Conversions.encodeTransaction(TransactionBuilder.Empty))
      .setSubmitterInfo(
        DamlSubmitterInfo.newBuilder
          .setCommandId("commandId")
          .addAllSubmitters(submitters.asJava))
      .setSubmissionSeed(ByteString.copyFromUtf8("a" * 32))
      .build
  }
}
