// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import java.time.Instant
import java.util.UUID

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.Conversions.{buildTimestamp, configurationStateKey}
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.Err.MissingInputState
import com.daml.ledger.participant.state.kvutils.TestHelpers._
import com.daml.ledger.participant.state.kvutils.committer.transaction.keys.ContractKeysValidation
import com.daml.ledger.participant.state.kvutils.committer.{
  CommitContext,
  StepContinue,
  StepResult,
  StepStop,
}
import com.daml.ledger.participant.state.kvutils.{Conversions, committer}
import com.daml.ledger.participant.state.v1.{Configuration, RejectionReason, RejectionReasonV0}
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.{Engine, ReplayMismatch}
import com.daml.lf.transaction
import com.daml.lf.transaction._
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.test.TransactionBuilder.{Create, Exercise}
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.mockito.MockitoSugar
import org.scalatest.Inspectors.forEvery
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class TransactionCommitterSpec extends AnyWordSpec with Matchers with MockitoSugar {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val txBuilder = TransactionBuilder()
  private val metrics = new Metrics(new MetricRegistry)
  private val transactionCommitter =
    createTransactionCommitter() // Stateless, can be shared between tests
  private val aDamlTransactionEntry = createEmptyTransactionEntry(List("aSubmitter"))
  private val aTransactionEntrySummary = DamlTransactionEntrySummary(aDamlTransactionEntry)
  private val aRecordTime = Timestamp(100)
  private val aDedupKey = Conversions
    .commandDedupKey(aTransactionEntrySummary.submitterInfo)
  private val emptyConfigurationStateValue =
    defaultConfigurationStateValueBuilder().build
  private val inputWithTimeModelAndEmptyCommandDeduplication =
    Map(Conversions.configurationStateKey -> Some(emptyConfigurationStateValue), aDedupKey -> None)
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
  private val aDedupValue = DamlStateValue.newBuilder
    .setCommandDedup(DamlCommandDedupValue.newBuilder.setDeduplicatedUntil(aDeduplicateUntil))
    .build()
  private val inputWithTimeModelAndCommandDeduplication =
    Map(
      Conversions.configurationStateKey -> Some(emptyConfigurationStateValue),
      aDedupKey -> Some(aDedupValue),
    )
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
          )
        )
      ),
      createNode("Exercise-1")(
        _.setExercise(
          exerciseNodeBuilder.addAllChildren(
            Seq("LookupByKey-2", "Fetch-2", "Create-2", "Exercise-2").asJava
          )
        )
      ),
      createNode("Rollback-1")(
        _.setRollback(
          rollbackNodeBuilder.addAllChildren(Seq("RollbackChild-1", "RollbackChild-2").asJava)
        )
      ),
      createNode("RollbackChild-1")(_.setCreate(createNodeBuilder)),
      createNode("RollbackChild-2")(_.setFetch(fetchNodeBuilder)),
    )
    val tx = TransactionOuterClass.Transaction
      .newBuilder()
      .addAllRoots(roots.asJava)
      .addAllNodes(nodes.asJava)
      .build()
    val outTx = aDamlTransactionEntry.toBuilder.setTransaction(tx).build()
    DamlTransactionEntrySummary(outTx)
  }
  private val aDummyValue = TransactionBuilder.record("field" -> "value")
  private val aKey = "key"
  private val aKeyMaintainer = "maintainer"

  private def createNode(nodeId: String)(
      nodeImpl: TransactionOuterClass.Node.Builder => TransactionOuterClass.Node.Builder
  ) =
    nodeImpl(TransactionOuterClass.Node.newBuilder().setNodeId(nodeId)).build()

  private def fetchNodeBuilder = TransactionOuterClass.NodeFetch.newBuilder()

  private def exerciseNodeBuilder =
    TransactionOuterClass.NodeExercise.newBuilder()

  private def rollbackNodeBuilder =
    TransactionOuterClass.NodeRollback.newBuilder()

  private def createNodeBuilder = TransactionOuterClass.NodeCreate.newBuilder()

  private def lookupByKeyNodeBuilder =
    TransactionOuterClass.NodeLookupByKey.newBuilder()

  "trimUnnecessaryNodes" should {
    "remove `Fetch`, `LookupByKey`, and `Rollback` nodes from the transaction tree" in {
      val context = createCommitContext(recordTime = None)

      val actual = transactionCommitter.trimUnnecessaryNodes(
        context,
        aRichTransactionTreeSummary,
      )

      actual match {
        case StepContinue(logEntry) =>
          val transaction = logEntry.submission.getTransaction
          transaction.getRootsList.asScala should contain theSameElementsInOrderAs Seq(
            "Exercise-1",
            "Create-1",
          )
          val nodes = transaction.getNodesList.asScala
          nodes.map(_.getNodeId) should contain theSameElementsInOrderAs Seq(
            "Create-1",
            "Create-2",
            "Create-3",
            "Exercise-2",
            "Exercise-1",
          )
          nodes(3).getExercise.getChildrenList.asScala should contain theSameElementsInOrderAs Seq(
            "Create-3"
          )
          nodes(4).getExercise.getChildrenList.asScala should contain theSameElementsInOrderAs Seq(
            "Create-2",
            "Exercise-2",
          )
        case StepStop(_) => fail("should be StepContinue")
      }
    }
  }

  "deduplicateCommand" should {
    "continue if record time is not available" in {
      val context = createCommitContext(recordTime = None)

      val actual = transactionCommitter.deduplicateCommand(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail()
      }
    }

    "continue if record time is available but no deduplication entry could be found" in {
      val inputs = Map(aDedupKey -> None)
      val context =
        createCommitContext(recordTime = Some(aRecordTime), inputs = inputs)

      val actual = transactionCommitter.deduplicateCommand(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail()
      }
    }

    "continue if record time is after deduplication time in case a deduplication entry is found" in {
      val dedupValue = newDedupValue(aRecordTime)
      val inputs = Map(aDedupKey -> Some(dedupValue))
      val context =
        createCommitContext(recordTime = Some(aRecordTime.addMicros(1)), inputs = inputs)

      val actual = transactionCommitter.deduplicateCommand(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(_) => succeed
        case StepStop(_) => fail()
      }
    }

    "produce rejection log entry in case record time is on or before deduplication time" in {
      for (
        (recordTime, deduplicationTime) <- Iterable(
          (aRecordTime, aRecordTime),
          (aRecordTime, aRecordTime.addMicros(1)),
        )
      ) {
        val dedupValue = newDedupValue(deduplicationTime)
        val inputs = Map(aDedupKey -> Some(dedupValue))
        val context =
          createCommitContext(recordTime = Some(recordTime), inputs = inputs)

        val actual = transactionCommitter.deduplicateCommand(context, aTransactionEntrySummary)

        actual match {
          case StepContinue(_) => fail()
          case StepStop(actualLogEntry) =>
            actualLogEntry.hasTransactionRejectionEntry shouldBe true
        }
      }
    }
  }

  "validateLedgerTime" can {
    "when the record time is not available" should {
      "continue" in {
        val result = transactionCommitter.validateLedgerTime(
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

        transactionCommitter.validateLedgerTime(
          context,
          aDamlTransactionEntrySummaryWithSubmissionAndLedgerEffectiveTimes,
        )

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

        transactionCommitter.validateLedgerTime(
          context,
          aDamlTransactionEntrySummaryWithSubmissionAndLedgerEffectiveTimes,
        )

        context.minimumRecordTime shouldEqual Some(
          Instant.ofEpochSecond(3).plus(Timestamp.Resolution)
        )
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
        val actual = transactionCommitter.validateLedgerTime(context, transactionEntrySummary)

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

      transactionCommitter.validateLedgerTime(
        commitContext,
        aTransactionEntrySummary,
      )

      commitContext.getAccessedInputKeys should contain(configurationStateKey)
    }
  }

  "buildLogEntry" should {
    "set record time in log entry when it is available" in {
      val context = createCommitContext(recordTime = Some(theRecordTime))

      val actual = transactionCommitter.buildLogEntry(aTransactionEntrySummary, context)

      actual.hasRecordTime shouldBe true
      actual.getRecordTime shouldBe buildTimestamp(theRecordTime)
      actual.hasTransactionEntry shouldBe true
      actual.getTransactionEntry shouldBe aTransactionEntrySummary.submission
    }

    "skip setting record time in log entry when it is not available" in {
      val context = createCommitContext(recordTime = None)

      val actual =
        transactionCommitter.buildLogEntry(aTransactionEntrySummary, context)

      actual.hasRecordTime shouldBe false
      actual.hasTransactionEntry shouldBe true
      actual.getTransactionEntry shouldBe aTransactionEntrySummary.submission
    }

    "produce an out-of-time-bounds rejection log entry in case pre-execution is enabled" in {
      val context = createCommitContext(recordTime = None)

      transactionCommitter.buildLogEntry(aTransactionEntrySummary, context)

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

      transactionCommitter.buildLogEntry(aTransactionEntrySummary, context)

      context.preExecute shouldBe false
      context.outOfTimeBoundsLogEntry shouldBe empty
    }
  }

  "blind" should {
    "always set blindingInfo" in {
      val context = createCommitContext(recordTime = None)

      val actual = transactionCommitter.blind(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(partialResult) =>
          partialResult.submission.hasBlindingInfo shouldBe true
        case StepStop(_) => fail()
      }
    }
  }

  "rejectionReasonForValidationError" when {
    def mkMismatch(
        recorded: (Transaction.Transaction, NodeId),
        replayed: (Transaction.Transaction, NodeId),
    ): ReplayNodeMismatch[NodeId, Value.ContractId] =
      ReplayNodeMismatch(recorded._1, recorded._2, replayed._1, replayed._2)
    def mkRecordedMissing(
        recorded: Transaction.Transaction,
        replayed: (Transaction.Transaction, NodeId),
    ): RecordedNodeMissing[NodeId, Value.ContractId] =
      RecordedNodeMissing(recorded, replayed._1, replayed._2)
    def mkReplayedMissing(
        recorded: (Transaction.Transaction, NodeId),
        replayed: Transaction.Transaction,
    ): ReplayedNodeMissing[NodeId, Value.ContractId] =
      ReplayedNodeMissing(recorded._1, recorded._2, replayed)

    def checkRejectionReason(
        mkReason: String => RejectionReason
    )(mismatch: transaction.ReplayMismatch[NodeId, Value.ContractId]) = {
      val replayMismatch = ReplayMismatch(mismatch)
      transactionCommitter.rejectionReasonForValidationError(replayMismatch) shouldBe mkReason(
        replayMismatch.msg
      )
    }

    val createInput = create("#inputContractId")
    val create1 = create("#someContractId")
    val create2 = create("#otherContractId")

    val exercise = txBuilder.exercise(
      contract = createInput,
      choice = "DummyChoice",
      consuming = false,
      actingParties = Set(aKeyMaintainer),
      argument = aDummyValue,
      byKey = false,
    )
    val otherKeyCreate = create(
      contractId = "#contractWithOtherKey",
      signatories = Seq(aKeyMaintainer),
      keyAndMaintainer = Some("otherKey" -> aKeyMaintainer),
    )

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
        forEvery(inconsistentLookups)(checkRejectionReason(RejectionReasonV0.Inconsistent))
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
        forEvery(recordedKeyInconsistent)(checkRejectionReason(RejectionReasonV0.Disputed))
      }

      "report Disputed if the keys are different" in {
        checkRejectionReason(RejectionReasonV0.Disputed)(mkMismatch(txOther, tx1))
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
        forEvery(miscMismatches)(checkRejectionReason(RejectionReasonV0.Disputed))
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
        participantId = ParticipantId,
      )
      val tx = DamlTransactionEntrySummary(createEmptyTransactionEntry(List(Alice, Bob, Emma)))

      a[MissingInputState] should be thrownBy transactionCommitter.authorizeSubmitters(
        context,
        tx,
      )
    }

    "reject a submission when any of the submitters is not known" in {
      val context = createCommitContext(
        recordTime = None,
        inputs = createInputs(
          Alice -> Some(hostedParty(Alice)),
          Bob -> None,
        ),
        participantId = ParticipantId,
      )
      val tx = DamlTransactionEntrySummary(createEmptyTransactionEntry(List(Alice, Bob)))

      val result = transactionCommitter.authorizeSubmitters(context, tx)
      result shouldBe a[StepStop]

      val rejectionReason =
        getTransactionRejectionReason(result).getPartyNotKnownOnLedger.getDetails
      rejectionReason should fullyMatch regex """Submitting party .+ not known"""
    }

    "reject a submission when any of the submitters' participant id is incorrect" in {
      val context = createCommitContext(
        recordTime = None,
        inputs = createInputs(
          Alice -> Some(hostedParty(Alice)),
          Bob -> Some(notHostedParty(Bob)),
        ),
        participantId = ParticipantId,
      )
      val tx = DamlTransactionEntrySummary(createEmptyTransactionEntry(List(Alice, Bob)))

      val result = transactionCommitter.authorizeSubmitters(context, tx)
      result shouldBe a[StepStop]

      val rejectionReason =
        getTransactionRejectionReason(result).getSubmitterCannotActViaParticipant.getDetails
      rejectionReason should fullyMatch regex s"""Party .+ not hosted by participant ${mkParticipantId(
        ParticipantId
      )}"""
    }

    "allow a submission when all of the submitters are hosted on the participant" in {
      val context = createCommitContext(
        recordTime = None,
        inputs = createInputs(
          Alice -> Some(hostedParty(Alice)),
          Bob -> Some(hostedParty(Bob)),
          Emma -> Some(hostedParty(Emma)),
        ),
        participantId = ParticipantId,
      )
      val tx = DamlTransactionEntrySummary(createEmptyTransactionEntry(List(Alice, Bob, Emma)))

      val result = transactionCommitter.authorizeSubmitters(context, tx)
      result shouldBe a[StepContinue[_]]
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

    def hostedParty(party: String): DamlPartyAllocation =
      partyAllocation(party, ParticipantId)
    def notHostedParty(party: String): DamlPartyAllocation =
      partyAllocation(party, OtherParticipantId)
    def createInputs(
        inputs: (String, Option[DamlPartyAllocation])*
    ): Map[DamlStateKey, Option[DamlStateValue]] =
      inputs.map { case (party, partyAllocation) =>
        DamlStateKey.newBuilder().setParty(party).build() -> partyAllocation
          .map(
            DamlStateValue.newBuilder().setParty(_).build()
          )
      }.toMap
  }

  "validateContractKeys" should {
    def newCreateNodeWithFixedKey(contractId: String): Create =
      create(contractId, signatories = Seq("Alice"), keyAndMaintainer = Some(aKey -> "Alice"))

    def freshContractId: String =
      s"testContractId-${UUID.randomUUID().toString.take(10)}"

    def newLookupByKeySubmittedTransaction(
        found: Boolean,
        inRollback: Boolean,
    ): SubmittedTransaction = {
      val lookup =
        txBuilder.lookupByKey(newCreateNodeWithFixedKey(contractId = s"#$freshContractId"), found)
      val builder = TransactionBuilder()
      if (inRollback) {
        val rollback = builder.add(txBuilder.rollback())
        builder.add(lookup, rollback)
      } else {
        builder.add(lookup)
      }
      builder.buildSubmitted()
    }

    val conflictingKey = {
      val aCreateNode = newCreateNodeWithFixedKey("#dummy")
      Conversions.encodeContractKey(aCreateNode.templateId, aCreateNode.key.get.key)
    }

    "return Inconsistent when a contract key resolves to a different contract ID than submitted by a participant" in {

      val cases =
        Seq(
          ("existing global key was not found", false, Some(s"#$freshContractId")),
          (
            "existing global key was mapped to the wrong contract id",
            true,
            Some(s"#$freshContractId"),
          ),
          ("no global key exists but lookup succeeded", true, None),
        )
          .flatMap { case (name, found, contractIdAtCommitter) =>
            Seq(false, true).map(inRollback =>
              (name, newLookupByKeySubmittedTransaction(found, inRollback), contractIdAtCommitter)
            )
          }

      val casesTable = Table(
        ("name", "transaction", "contractIdAtCommitter"),
        cases: _*
      )

      forAll(casesTable) {
        (_, transaction: SubmittedTransaction, contractIdAtCommitter: Option[String]) =>
          val context = commitContextWithContractStateKeys(
            conflictingKey -> contractIdAtCommitter
          )
          val result = validate(context, transaction)
          result shouldBe a[StepStop]

          val rejectionReason =
            getTransactionRejectionReason(result).getInconsistent.getDetails
          rejectionReason should startWith("InconsistentKeys")
      }
    }

    "return DuplicateKeys when two local contracts conflict" in {
      val builder = TransactionBuilder()
      builder.add(newCreateNodeWithFixedKey(s"#$freshContractId"))
      builder.add(newCreateNodeWithFixedKey(s"#$freshContractId"))
      val transaction = builder.buildSubmitted()
      val context = commitContextWithContractStateKeys(conflictingKey -> None)

      val result = validate(context, transaction)
      result shouldBe a[StepStop]

      val rejectionReason =
        getTransactionRejectionReason(result).getInconsistent.getDetails
      rejectionReason should startWith("DuplicateKeys")
    }

    "return DuplicateKeys when a create in a rollback conflicts with a global key" in {
      val builder = TransactionBuilder()
      val rollback = builder.add(builder.rollback())
      builder.add(newCreateNodeWithFixedKey(s"#$freshContractId"), rollback)
      val transaction = builder.buildSubmitted()
      val context = commitContextWithContractStateKeys(conflictingKey -> Some(s"#$freshContractId"))

      val result = validate(context, transaction)
      result shouldBe a[StepStop]

      val rejectionReason =
        getTransactionRejectionReason(result).getInconsistent.getDetails
      rejectionReason should startWith("DuplicateKeys")
    }

    "not return DuplicateKeys between local contracts if first create is rolled back" in {
      val builder = TransactionBuilder()
      val rollback = builder.add(builder.rollback())
      builder.add(newCreateNodeWithFixedKey(s"#$freshContractId"), rollback)
      builder.add(newCreateNodeWithFixedKey(s"#$freshContractId"))

      val transaction = builder.buildSubmitted()

      val context = commitContextWithContractStateKeys(conflictingKey -> None)
      val result = validate(context, transaction)
      result shouldBe a[StepContinue[_]]
    }

    "return DuplicateKeys between local contracts even if second create is rolled back" in {
      val builder = TransactionBuilder()
      builder.add(newCreateNodeWithFixedKey(s"#$freshContractId"))
      val rollback = builder.add(builder.rollback())
      builder.add(newCreateNodeWithFixedKey(s"#$freshContractId"), rollback)
      val transaction = builder.buildSubmitted()
      val context = commitContextWithContractStateKeys(conflictingKey -> None)

      val result = validate(context, transaction)
      result shouldBe a[StepStop]

      val rejectionReason =
        getTransactionRejectionReason(result).getInconsistent.getDetails
      rejectionReason should startWith("DuplicateKeys")
    }

    "return DuplicateKeys between local contracts even if the first one was archived in a rollback" in {
      val builder = TransactionBuilder()
      val create = newCreateNodeWithFixedKey(s"#$freshContractId")
      builder.add(create)
      val rollback = builder.add(builder.rollback())
      builder.add(archive(create, Set("Alice")), rollback)
      builder.add(newCreateNodeWithFixedKey(s"#$freshContractId"))
      val transaction = builder.buildSubmitted()
      val context = commitContextWithContractStateKeys(conflictingKey -> None)

      val result = validate(context, transaction)
      result shouldBe a[StepStop]

      val rejectionReason =
        getTransactionRejectionReason(result).getInconsistent.getDetails
      rejectionReason should startWith("DuplicateKeys")
    }

    "return InconsistentKeys on conflict local and global contracts even if global was archived in a rollback" in {
      val builder = TransactionBuilder()
      val globalCid = s"#freshContractId"
      val rollback = builder.add(builder.rollback())
      builder.add(archive(globalCid, Set("Alice")), rollback)
      builder.add(newCreateNodeWithFixedKey(s"#$freshContractId"))
      val transaction = builder.buildSubmitted()
      val context = commitContextWithContractStateKeys(conflictingKey -> Some(globalCid))

      val result = validate(context, transaction)
      result shouldBe a[StepStop]

      val rejectionReason =
        getTransactionRejectionReason(result).getInconsistent.getDetails
      rejectionReason should startWith("InconsistentKeys")
    }

    def validate(
        ctx: CommitContext,
        transaction: SubmittedTransaction,
    )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] =
      ContractKeysValidation.validateKeys(transactionCommitter)(
        ctx,
        DamlTransactionEntrySummary(createTransactionEntry(List("Alice"), transaction)),
      )

    def contractKeyState(contractId: String): DamlContractKeyState =
      DamlContractKeyState
        .newBuilder()
        .setContractId(contractId)
        .build()

    def contractStateKey(contractKey: DamlContractKey): DamlStateKey =
      DamlStateKey
        .newBuilder()
        .setContractKey(contractKey)
        .build()

    def contractKeyStateValue(contractId: String): DamlStateValue =
      DamlStateValue
        .newBuilder()
        .setContractKeyState(contractKeyState(contractId))
        .build()

    def commitContextWithContractStateKeys(
        contractKeyIdPairs: (DamlContractKey, Option[String])*
    ): CommitContext =
      createCommitContext(
        recordTime = None,
        inputs = contractKeyIdPairs.map { case (key, id) =>
          contractStateKey(key) -> id.map(contractKeyStateValue)
        }.toMap,
      )
  }

  private def getTransactionRejectionReason(
      result: StepResult[DamlTransactionEntrySummary]
  ): DamlTransactionRejectionEntry =
    result
      .asInstanceOf[StepStop]
      .logEntry
      .getTransactionRejectionEntry

  private def createTransactionCommitter(): committer.transaction.TransactionCommitter =
    new committer.transaction.TransactionCommitter(
      theDefaultConfig,
      mock[Engine],
      metrics,
      inStaticTimeMode = false,
    )

  private def contextWithTimeModelAndEmptyCommandDeduplication() =
    createCommitContext(recordTime = None, inputs = inputWithTimeModelAndEmptyCommandDeduplication)

  private def contextWithTimeModelAndCommandDeduplication() =
    createCommitContext(recordTime = None, inputs = inputWithTimeModelAndCommandDeduplication)

  private def newDedupValue(deduplicationTime: Timestamp): DamlStateValue =
    DamlStateValue.newBuilder
      .setCommandDedup(
        DamlCommandDedupValue.newBuilder.setDeduplicatedUntil(buildTimestamp(deduplicationTime))
      )
      .build

  private def createProtobufTimestamp(seconds: Long) =
    Conversions.buildTimestamp(Timestamp.assertFromInstant(Instant.ofEpochSecond(seconds)))

  private def defaultConfigurationStateValueBuilder(): DamlStateValue.Builder =
    DamlStateValue.newBuilder
      .setConfigurationEntry(
        DamlConfigurationEntry.newBuilder
          .setConfiguration(Configuration.encode(theDefaultConfig))
      )

  private def createEmptyTransactionEntry(submitters: List[String]): DamlTransactionEntry =
    createTransactionEntry(submitters, TransactionBuilder.EmptySubmitted)

  private def createTransactionEntry(submitters: List[String], tx: SubmittedTransaction) =
    DamlTransactionEntry.newBuilder
      .setTransaction(Conversions.encodeTransaction(tx))
      .setSubmitterInfo(
        DamlSubmitterInfo.newBuilder
          .setCommandId("commandId")
          .addAllSubmitters(submitters.asJava)
      )
      .setSubmissionSeed(ByteString.copyFromUtf8("a" * 32))
      .build

  private def tuple(values: String*): TransactionBuilder.Value =
    TransactionBuilder.record(values.zipWithIndex.map { case (v, i) =>
      s"_$i" -> v
    }: _*)

  private def create(
      contractId: String,
      signatories: Seq[String] = Seq(aKeyMaintainer),
      argument: TransactionBuilder.Value = aDummyValue,
      keyAndMaintainer: Option[(String, String)] = Some(aKey -> aKeyMaintainer),
  ): TransactionBuilder.Create =
    txBuilder.create(
      id = contractId,
      template = "dummyPackage:DummyModule:DummyTemplate",
      argument = argument,
      signatories = signatories,
      observers = Seq.empty,
      key = keyAndMaintainer.map { case (key, maintainer) =>
        tuple(maintainer, key)
      },
    )

  def archive(create: Create, actingParties: Set[String]): Exercise =
    txBuilder.exercise(
      create,
      choice = "Archive",
      consuming = true,
      actingParties = actingParties,
      argument = Value.ValueRecord(None, ImmArray.empty),
      result = Some(Value.ValueUnit),
    )

  def archive(contractId: String, actingParties: Set[String]): Exercise =
    archive(create(contractId), actingParties)
}
