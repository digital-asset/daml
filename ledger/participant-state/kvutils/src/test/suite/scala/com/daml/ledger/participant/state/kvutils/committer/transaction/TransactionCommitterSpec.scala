// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.TestHelpers._
import com.daml.ledger.participant.state.kvutils.committer.{StepContinue, StepStop}
import com.daml.ledger.participant.state.kvutils.{Conversions, committer}
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.Engine
import com.daml.lf.transaction._
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.test.TransactionBuilder.{Create, Exercise}
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
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
        actual.getTransactionRejectionEntry.getSubmitterInfo shouldBe aTransactionEntrySummary.submitterInfo
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

      val actual = transactionCommitter.blind(context, aTransactionEntrySummary)

      actual match {
        case StepContinue(partialResult) =>
          partialResult.submission.hasBlindingInfo shouldBe true
        case StepStop(_) => fail()
      }
    }
  }

  private def createTransactionCommitter(): committer.transaction.TransactionCommitter =
    new committer.transaction.TransactionCommitter(
      theDefaultConfig,
      mock[Engine],
      metrics,
      inStaticTimeMode = false,
    )

  private def newDedupValue(deduplicationTime: Timestamp): DamlStateValue =
    DamlStateValue.newBuilder
      .setCommandDedup(
        DamlCommandDedupValue.newBuilder.setDeduplicatedUntil(buildTimestamp(deduplicationTime))
      )
      .build

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
      key = keyAndMaintainer.map { case (key, maintainer) => lfTuple(maintainer, key) },
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
