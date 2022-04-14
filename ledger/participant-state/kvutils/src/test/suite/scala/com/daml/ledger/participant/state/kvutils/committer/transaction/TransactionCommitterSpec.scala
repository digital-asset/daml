// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.ledger.participant.state.kvutils.Err.MissingInputState
import com.daml.ledger.participant.state.kvutils.TestHelpers._
import com.daml.ledger.participant.state.kvutils.committer.{StepContinue, StepStop}
import com.daml.ledger.participant.state.kvutils.store.events.{
  DamlConfigurationEntry,
  DamlTransactionRejectionEntry,
}
import com.daml.ledger.participant.state.kvutils.store.{
  DamlCommandDedupValue,
  DamlLogEntry,
  DamlPartyAllocation,
  DamlStateKey,
  DamlStateValue,
  PreExecutionDeduplicationBounds,
}
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.ledger.participant.state.kvutils.{Conversions, Err, KeyValueCommitting, committer}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.Engine
import com.daml.lf.kv.contracts.ContractConversions
import com.daml.lf.transaction._
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value.{ContractId, ValueRecord, ValueText}
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.google.protobuf.{ByteString, Duration}
import org.mockito.MockitoSugar
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class TransactionCommitterSpec
    extends AnyWordSpec
    with Matchers
    with MockitoSugar
    with OptionValues {
  import TransactionBuilder.Implicits._
  import TransactionCommitterSpec._

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val txBuilder = TransactionBuilder()
  private val metrics = new Metrics(new MetricRegistry)
  private val transactionCommitter =
    createTransactionCommitter() // Stateless, can be shared between tests

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

      getTransactionRejectionReason(result).getReasonCase should be(
        DamlTransactionRejectionEntry.ReasonCase.SUBMITTING_PARTY_NOT_KNOWN_ON_LEDGER
      )
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
  }

  "trimUnnecessaryNodes" should {
    "remove `Fetch`, `LookupByKey`, and `Rollback` nodes from the transaction tree" in {
      val context = createCommitContext(recordTime = None)

      val actual = transactionCommitter.trimUnnecessaryNodes(
        context,
        aRichTransactionTreeSummary,
      )

      actual match {
        case StepContinue(logEntry) =>
          val transaction =
            TransactionOuterClass.Transaction.parseFrom(logEntry.submission.getRawTransaction)
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

    "fail on a non-parsable transaction" in {
      val context = createCommitContext(recordTime = None)
      val brokenEntry =
        aDamlTransactionEntry.toBuilder.setRawTransaction(ByteString.copyFromUtf8("wrong")).build()

      an[Err.DecodeError] should be thrownBy transactionCommitter.trimUnnecessaryNodes(
        context,
        DamlTransactionEntrySummary(brokenEntry),
      )
    }

    "fail on a transaction with invalid roots" in {
      val context = createCommitContext(recordTime = None)
      val brokenEntry = aDamlTransactionEntry.toBuilder
        .setRawTransaction(
          aRichNodeTreeTransaction.toBuilder.addRoots("non-existent").build().toByteString
        )
        .build()
      an[Err.InternalError] should be thrownBy transactionCommitter.trimUnnecessaryNodes(
        context,
        DamlTransactionEntrySummary(brokenEntry),
      )
    }
  }

  "buildLogEntry" should {
    "set record time in log entry when it is available" in {
      val context = createCommitContext(recordTime = Some(theRecordTime))

      val actual = TransactionCommitter.buildLogEntry(aTransactionEntrySummary, context)

      actual.hasRecordTime shouldBe true
      actual.getRecordTime shouldBe buildTimestamp(theRecordTime)
      actual.hasTransactionEntry shouldBe true
      actual.getTransactionEntry shouldBe aTransactionEntrySummary.submission
    }

    "skip setting record time in log entry when it is not available" in {
      val context = createCommitContext(recordTime = None)

      val actual =
        TransactionCommitter.buildLogEntry(aTransactionEntrySummary, context)

      actual.hasRecordTime shouldBe false
      actual.hasTransactionEntry shouldBe true
      actual.getTransactionEntry shouldBe aTransactionEntrySummary.submission
    }

    "produce an out-of-time-bounds rejection log entry in case pre-execution is enabled" in {
      val context = createCommitContext(recordTime = None)

      TransactionCommitter.buildLogEntry(aTransactionEntrySummary, context)

      context.preExecute shouldBe true
      context.outOfTimeBoundsLogEntry should not be empty
      context.outOfTimeBoundsLogEntry.foreach { actual =>
        actual.hasRecordTime shouldBe false
        actual.hasTransactionRejectionEntry shouldBe true
        val entry = actual.getTransactionRejectionEntry
        entry.getSubmitterInfo shouldBe aTransactionEntrySummary.submitterInfo
        entry.getDefiniteAnswer shouldBe false
      }
    }

    "not set an out-of-time-bounds rejection log entry in case pre-execution is disabled" in {
      val context = createCommitContext(recordTime = Some(aRecordTime))

      TransactionCommitter.buildLogEntry(aTransactionEntrySummary, context)

      context.preExecute shouldBe false
      context.outOfTimeBoundsLogEntry shouldBe empty
    }
  }

  "blind" should {
    "always set blindingInfo" in {
      val context = createCommitContext(recordTime = None)
      context.set(Conversions.configurationStateKey, aDamlConfigurationStateValue)

      val builder = TransactionBuilder()
      val cid = builder.newCid

      val (expectedRawContractInstance, txEntry) = txEntryWithDivulgedContract(builder, cid)
      val txEntryBuilder = txEntry.toBuilder
      // deduplication duration is mandatory as we set the context dedup entry during blinding
      txEntryBuilder.getSubmitterInfoBuilder.setDeduplicationDuration(
        Duration.newBuilder().setSeconds(5)
      )
      val actual =
        transactionCommitter.blind(
          context,
          DamlTransactionEntrySummary(txEntryBuilder.build()),
        )

      actual match {
        case StepContinue(partialResult) =>
          val blindingInfo = partialResult.submission.getBlindingInfo

          val actualDivulgencesList =
            blindingInfo.getDivulgencesList.asScala
              .map(entry =>
                (
                  entry.getContractId,
                  entry.getDivulgedToLocalPartiesList.asScala.toSet,
                  entry.getRawContractInstance,
                )
              )

          actualDivulgencesList should contain theSameElementsAs {
            Vector((cid.coid, Set("ChoiceObserver"), expectedRawContractInstance.byteString))
          }

          val actualDisclosureList =
            blindingInfo.getDisclosuresList.asScala
              .map(entry => entry.getNodeId -> entry.getDisclosedToLocalPartiesList.asScala.toSet)

          actualDisclosureList should contain theSameElementsAs Vector(
            "0" -> Set("Alice"),
            "1" -> Set("Actor", "Alice", "ChoiceObserver"),
          )

        case StepStop(_) => fail()
      }
    }
  }

  "out of time bounds entry" should {

    "be set" when {

      "a submitting party is not known" in {
        val context = createCommitContext(
          recordTime = None,
          inputs = createInputs(
            Alice -> Some(hostedParty(Alice)),
            Bob -> None,
          ) + (Conversions.configurationStateKey -> None),
          participantId = ParticipantId,
        )
        val transactionEntry = createEmptyTransactionEntry(List(Alice, Bob))
        val result = transactionCommitter.preExecute(
          DamlSubmission.newBuilder().setTransactionEntry(transactionEntry).build(),
          context,
        )
        resultIsRejectedWithPayload(
          result,
          DamlTransactionRejectionEntry.ReasonCase.SUBMITTING_PARTY_NOT_KNOWN_ON_LEDGER,
        )
      }

      "the command is a duplicate" in {
        val transactionEntry = createEmptyTransactionEntry(List(Alice))
        val configurationInput = Conversions.configurationStateKey -> None
        val commandDeduplicationInput = Conversions.commandDedupKey(
          transactionEntry.getSubmitterInfo
        ) -> Some(
          DamlStateValue.newBuilder
            .setCommandDedup(
              DamlCommandDedupValue.newBuilder
                .setRecordTimeBounds(
                  PreExecutionDeduplicationBounds
                    .newBuilder()
                    .setMaxRecordTime(transactionEntry.getSubmissionTime)
                    .setMinRecordTime(transactionEntry.getSubmissionTime)
                )
            )
            .build
        )
        val context = createCommitContext(
          recordTime = None,
          inputs = createInputs(
            Alice -> Some(hostedParty(Alice))
          ) + configurationInput + commandDeduplicationInput,
          participantId = ParticipantId,
        )
        val result = transactionCommitter.preExecute(
          DamlSubmission.newBuilder().setTransactionEntry(transactionEntry).build(),
          context,
        )
        resultIsRejectedWithPayload(
          result,
          DamlTransactionRejectionEntry.ReasonCase.DUPLICATE_COMMAND,
        )
      }
    }

    def resultIsRejectedWithPayload(
        result: KeyValueCommitting.PreExecutionResult,
        transactionRejectionReason: DamlTransactionRejectionEntry.ReasonCase,
    ) = {
      result.outOfTimeBoundsLogEntry.getPayloadCase shouldBe DamlLogEntry.PayloadCase.OUT_OF_TIME_BOUNDS_ENTRY
      result.outOfTimeBoundsLogEntry.getOutOfTimeBoundsEntry.getEntry.getPayloadCase shouldBe DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
      result.outOfTimeBoundsLogEntry.getOutOfTimeBoundsEntry.getEntry.getTransactionRejectionEntry.getReasonCase shouldBe DamlTransactionRejectionEntry.ReasonCase.RECORD_TIME_OUT_OF_RANGE
      result.successfulLogEntry.getPayloadCase shouldBe DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
      result.successfulLogEntry.getTransactionRejectionEntry.getReasonCase shouldBe transactionRejectionReason
    }
  }

  private def createTransactionCommitter(
      defaultConfig: Configuration = theDefaultConfig
  ): committer.transaction.TransactionCommitter =
    new committer.transaction.TransactionCommitter(
      defaultConfig,
      mock[Engine],
      metrics,
    )

  private def create(
      contractId: ContractId,
      signatories: Set[Ref.Party] = Set(aKeyMaintainer),
      argument: Value = aDummyValue,
      keyAndMaintainer: Option[(String, String)] = Some(aKey -> aKeyMaintainer),
  ): Node.Create =
    txBuilder.create(
      id = contractId,
      templateId = "DummyModule:DummyTemplate",
      argument = argument,
      signatories = signatories,
      observers = Set.empty,
      key = keyAndMaintainer.map { case (key, maintainer) => lfTuple(maintainer, key) },
    )

  def archive(create: Node.Create, actingParties: Set[String]): Node.Exercise =
    txBuilder.exercise(
      create,
      choice = "Archive",
      consuming = true,
      actingParties = actingParties,
      argument = Value.ValueRecord(None, ImmArray.Empty),
      result = Some(Value.ValueUnit),
    )

  def archive(contractId: String, actingParties: Set[String]): Node.Exercise =
    archive(create(contractId), actingParties)
}

object TransactionCommitterSpec {

  import TransactionBuilder.Implicits._

  private val Alice = "alice"
  private val Bob = "bob"
  private val Emma = "emma"
  private val ParticipantId = 0
  private val OtherParticipantId = 1
  private val aDamlTransactionEntry = createEmptyTransactionEntry(List("aSubmitter"))
  private val aTransactionEntrySummary = DamlTransactionEntrySummary(aDamlTransactionEntry)
  private val aRecordTime = Timestamp(100)
  private val aDummyValue = TransactionBuilder.record("field" -> "value")
  private val aKey = "key"
  private val aKeyMaintainer = "maintainer"
  private val aDamlConfigurationStateValue = DamlStateValue.newBuilder
    .setConfigurationEntry(
      DamlConfigurationEntry.newBuilder
        .setConfiguration(Configuration.encode(theDefaultConfig))
    )
    .build
  private val aRichNodeTreeTransaction = {
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
    TransactionOuterClass.Transaction
      .newBuilder()
      .addAllRoots(roots.asJava)
      .addAllNodes(nodes.asJava)
      .build()
  }
  private val aRichTransactionTreeSummary = DamlTransactionEntrySummary(
    aDamlTransactionEntry.toBuilder.setRawTransaction(aRichNodeTreeTransaction.toByteString).build()
  )

  private def txEntryWithDivulgedContract(
      builder: TransactionBuilder,
      divulgedContractId: Value.ContractId,
  ) = {
    val templateId = "DummyModule:DummyTemplate"
    val textArgument = ValueText("DummyText")

    val createNode = builder.create(
      id = divulgedContractId,
      templateId = templateId,
      argument = textArgument,
      signatories = Set("Alice"),
      observers = Set.empty,
      key = None,
    )
    val exerciseNode = builder.exercise(
      contract = createNode,
      choice = "C",
      consuming = false,
      actingParties = Set("Actor"),
      argument = ValueRecord(None, ImmArray.Empty),
      choiceObservers = Set("ChoiceObserver"),
    )

    builder.add(createNode)
    builder.add(exerciseNode)

    val expectedRawContractInstance = ContractConversions
      .encodeContractInstance(
        Value.VersionedContractInstance(
          version = TransactionVersion.StableVersions.max,
          template = templateId,
          arg = textArgument,
          agreementText = "",
        )
      )
      .getOrElse(throw Err.EncodeError("ContractInstance", "Should not happen"))

    expectedRawContractInstance -> createTransactionEntry(
      List("aSubmitter"),
      SubmittedTransaction(builder.build()),
    )
  }

  private def createInputs(
      inputs: (String, Option[DamlPartyAllocation])*
  ): Map[DamlStateKey, Option[DamlStateValue]] =
    inputs.map { case (party, partyAllocation) =>
      DamlStateKey.newBuilder().setParty(party).build() -> partyAllocation
        .map(
          DamlStateValue.newBuilder().setParty(_).build()
        )
    }.toMap

  private def hostedParty(party: String): DamlPartyAllocation =
    partyAllocation(party, ParticipantId)
  private def notHostedParty(party: String): DamlPartyAllocation =
    partyAllocation(party, OtherParticipantId)
  private def partyAllocation(party: String, participantId: Int): DamlPartyAllocation =
    DamlPartyAllocation
      .newBuilder()
      .setParticipantId(mkParticipantId(participantId))
      .setDisplayName(party)
      .build()

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

}
