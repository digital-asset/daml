// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import cats.data.EitherT
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.{RepairUpdate, Update}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.platform.indexer.IndexerState.RepairInProgress
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.PekkoUtil.{FutureQueue, RecoveringFutureQueue}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import org.apache.pekko.Done
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{Future, Promise}

class IndexerStateSpec extends AnyFlatSpec with BaseTest with HasExecutionContext {

  behavior of "IndexerState"

  it should "successfully initiate indexer and shut down" in {
    val initialIndexer = new TestRecoveringIndexer
    val indexerState = new IndexerState(
      recoveringIndexerFactory = seqFactory(initialIndexer),
      repairIndexerFactory = asyncSeqFactory(),
      loggerFactory = loggerFactory,
    )

    // initial indexer is up and running
    Threading.sleep(20)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    initialIndexer.donePromise.isCompleted shouldBe false
    // shutting down
    val indexerStateTerminated = indexerState.shutdown()
    // first the ongoing indexing should be shut
    initialIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    indexerStateTerminated.isCompleted shouldBe false
    // and as the ongoing indexing is shut
    initialIndexer.donePromise.trySuccess(Done)
    // indexer state should be terminated
    indexerStateTerminated.futureValue
  }

  it should "orchestrate repair operation correctly during the happy path" in {
    val initialIndexer = new TestRecoveringIndexer
    val afterRepairIndexer = new TestRecoveringIndexer
    val repairIndexer = new TestRepairIndexer
    val repairIndexerFactoryCalled = Promise[Unit]()
    val repairIndexerCreated = Promise[FutureQueue[Update]]()

    val indexerState = new IndexerState(
      recoveringIndexerFactory = seqFactory(
        initialIndexer,
        afterRepairIndexer,
      ),
      repairIndexerFactory = asyncSeqFactory(
        repairIndexerFactoryCalled -> repairIndexerCreated.future
      ),
      loggerFactory = loggerFactory,
    )

    // initial indexer is up and running
    Threading.sleep(20)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    initialIndexer.donePromise.isCompleted shouldBe false
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        2L -> update,
      )
    )
    // starting repair operation
    val repairOperationStartedPromise = Promise[Unit]()
    val repairOperationFinishedPromise = Promise[Unit]()
    val repairOperationF = indexerState.withRepairIndexer { repairQueue =>
      repairQueue.offer(repairUpdate).futureValue shouldBe Done
      repairOperationStartedPromise.trySuccess(())
      EitherT.right[String](repairOperationFinishedPromise.future)
    }.value
    // first we wait for empty indexer queue
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    Threading.sleep(220)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    // and as the indexer queue is empty
    initialIndexer.uncommittedQueueSnapshotRef.set(Vector())
    // the initial Indexer is getting shut
    initialIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    initialIndexer.donePromise.isCompleted shouldBe false
    repairOperationStartedPromise.isCompleted shouldBe false
    repairIndexerFactoryCalled.isCompleted shouldBe false
    // then initial Indexer completes
    initialIndexer.donePromise.trySuccess(Done)
    // so repair Indexer is getting created
    repairIndexerFactoryCalled.future.futureValue
    Threading.sleep(20)
    repairOperationStartedPromise.isCompleted shouldBe false
    // and as repair indexer is created
    repairIndexerCreated.trySuccess(repairIndexer)
    // repair operation starts
    repairOperationStartedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // and as repair operation finishes
    repairOperationFinishedPromise.trySuccess(())
    // first the the CommitRepair is expected to be offered to the repairIndexer
    repairIndexer.repairReceivedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // then waiting for the message to be persisted
    repairIndexer.repairPersistedPromise.trySuccess(())
    // then the repair-indexer should be completing without shutdown signal
    repairIndexer.donePromise.trySuccess(Done)
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // and also the new indexer is completed it's first successfull initialization
    afterRepairIndexer.firstSuccessfulConsumerInitializationPromise.trySuccess(())
    // repair operation should be completed
    repairOperationF.futureValue
    // and as shutting down the indexer state
    val indexerStateTerminated = indexerState.shutdown()
    // first the ongoing indexing should be shut
    afterRepairIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    indexerStateTerminated.isCompleted shouldBe false
    // and as the ongoing indexing is shut
    afterRepairIndexer.donePromise.trySuccess(Done)
    // indexer state should be terminated
    indexerStateTerminated.futureValue
  }

  it should "fail repair operation if repair is in progress, but can proceed after repairDone" in {
    val initialIndexer = new TestRecoveringIndexer
    val afterRepairIndexer = new TestRecoveringIndexer
    val repairIndexer = new TestRepairIndexer
    val repairIndexerFactoryCalled = Promise[Unit]()
    val repairIndexerCreated = Promise[FutureQueue[Update]]()

    val indexerState = new IndexerState(
      recoveringIndexerFactory = seqFactory(
        initialIndexer,
        afterRepairIndexer,
      ),
      repairIndexerFactory = asyncSeqFactory(
        repairIndexerFactoryCalled -> repairIndexerCreated.future
      ),
      loggerFactory = loggerFactory,
    )

    // initial indexer is up and running
    Threading.sleep(20)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    initialIndexer.donePromise.isCompleted shouldBe false
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        2L -> update,
      )
    )
    // starting repair operation
    val repairOperationStartedPromise = Promise[Unit]()
    val repairOperationFinishedPromise = Promise[Unit]()
    val repairOperationF = indexerState.withRepairIndexer { _ =>
      repairOperationStartedPromise.trySuccess(())
      EitherT.right[String](repairOperationFinishedPromise.future)
    }.value
    // subsequent repair operation should fail immediately, signalling back repairDone
    val repairDone = indexerState.withRepairIndexer(_ => fail()).value.failed.futureValue match {
      case repairInProgress: RepairInProgress => repairInProgress.repairDone
      case unexpected => fail(s"RepairInProgress expected, but was $unexpected")
    }
    // first we wait for empty indexer queue
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    Threading.sleep(220)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    // and as the indexer queue is empty
    initialIndexer.uncommittedQueueSnapshotRef.set(Vector())
    // the initial Indexer is getting shut
    initialIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    initialIndexer.donePromise.isCompleted shouldBe false
    repairOperationStartedPromise.isCompleted shouldBe false
    repairIndexerFactoryCalled.isCompleted shouldBe false
    // then initial Indexer completes
    initialIndexer.donePromise.trySuccess(Done)
    // so repair Indexer is getting created
    repairIndexerFactoryCalled.future.futureValue
    Threading.sleep(20)
    repairOperationStartedPromise.isCompleted shouldBe false
    // and as repair indexer is created
    repairIndexerCreated.trySuccess(repairIndexer)
    // repair operation starts
    repairOperationStartedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // and as repair operation finishes
    repairOperationFinishedPromise.trySuccess(())
    // first the the CommitRepair is expected to be offered to the repairIndexer
    repairIndexer.repairReceivedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // then waiting for the message to be persisted
    repairIndexer.repairPersistedPromise.trySuccess(())
    // then the repair-indexer should be completing without shutdown signal
    repairIndexer.donePromise.trySuccess(Done)
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    repairDone.isCompleted shouldBe false
    // and also the new indexer is completed it's first successfull initialization
    afterRepairIndexer.firstSuccessfulConsumerInitializationPromise.trySuccess(())
    // repair operation should be completed
    repairOperationF.futureValue
    // also repairDone should be completed
    repairDone.futureValue
    // and as the repairDone completes new repair operation should be able to initiate
    val repairOperationF2 = indexerState.withRepairIndexer(_ => fail()).value
    // and it can be observed the the indexer is going down
    afterRepairIndexer.shutdownPromise.future.futureValue
    // and as shutting down the IndexerState
    val indexerStateTerminatedF = indexerState.shutdown()
    // and failing the shutdown of the after-repair-indexer
    afterRepairIndexer.donePromise.tryFailure(new RuntimeException("failed"))
    // repair operation completes with failure
    repairOperationF2.failed.futureValue.getMessage shouldBe "failed"
    // and indexer state should be terminated
    indexerStateTerminatedF.futureValue
  }

  it should "fail repair operation during shutdown" in {
    val initialIndexer = new TestRecoveringIndexer
    val indexerState = new IndexerState(
      recoveringIndexerFactory = seqFactory(initialIndexer),
      repairIndexerFactory = asyncSeqFactory(),
      loggerFactory = loggerFactory,
    )

    // initial indexer is up and running
    Threading.sleep(20)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    initialIndexer.donePromise.isCompleted shouldBe false
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        2L -> update,
      )
    )
    // shutting down
    val indexerStateTerminated = indexerState.shutdown()
    // repair operation should fail immediately
    indexerState
      .withRepairIndexer(_ => fail())
      .value
      .failed
      .futureValue
      .getMessage shouldBe "Shutdown in progress"
    // first the ongoing indexing should be shut
    initialIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    indexerStateTerminated.isCompleted shouldBe false
    // and as the ongoing indexing is shut
    initialIndexer.donePromise.trySuccess(Done)
    // indexer state should be terminated
    indexerStateTerminated.futureValue
  }

  it should "fail repair operation if the repair indexer failing to come up (also switch back to normal indexing)" in {
    val initialIndexer = new TestRecoveringIndexer
    val afterRepairIndexer = new TestRecoveringIndexer
    val repairIndexerFactoryCalled = Promise[Unit]()
    val repairIndexerCreated = Promise[FutureQueue[Update]]()

    val indexerState = new IndexerState(
      recoveringIndexerFactory = seqFactory(
        initialIndexer,
        afterRepairIndexer,
      ),
      repairIndexerFactory = asyncSeqFactory(
        repairIndexerFactoryCalled -> repairIndexerCreated.future
      ),
      loggerFactory = loggerFactory,
    )

    // initial indexer is up and running
    Threading.sleep(20)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    initialIndexer.donePromise.isCompleted shouldBe false
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        2L -> update,
      )
    )
    // starting repair operation
    val repairOperationF = indexerState.withRepairIndexer(_ => fail()).value
    // first we wait for empty indexer queue
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    Threading.sleep(220)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    // and as the indexer queue is empty
    initialIndexer.uncommittedQueueSnapshotRef.set(Vector())
    // the initial Indexer is getting shut
    initialIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    initialIndexer.donePromise.isCompleted shouldBe false
    repairIndexerFactoryCalled.isCompleted shouldBe false
    // then initial Indexer completes
    initialIndexer.donePromise.trySuccess(Done)
    // so repair Indexer is getting created
    repairIndexerFactoryCalled.future.futureValue
    Threading.sleep(20)
    // and as repair indexer is created
    repairIndexerCreated.tryFailure(new RuntimeException("repair indexer failed to initialize"))
    // then the repair-indexer should be completing without shutdown signal
    Threading.sleep(20)
    repairOperationF.isCompleted shouldBe false
    // and also the new indexer is completed it's first successfull initialization
    afterRepairIndexer.firstSuccessfulConsumerInitializationPromise.trySuccess(())
    // repair operation should be completed with a failure
    repairOperationF.failed.futureValue.getMessage shouldBe "repair indexer failed to initialize"
    // and as shutting down the indexer state
    val indexerStateTerminated = indexerState.shutdown()
    // first the ongoing indexing should be shut
    afterRepairIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    indexerStateTerminated.isCompleted shouldBe false
    // and as the ongoing indexing is shut
    afterRepairIndexer.donePromise.trySuccess(Done)
    // indexer state should be terminated
    indexerStateTerminated.futureValue
  }

  private def testRepairFunctionFailure(
      repairFunctionOutcome: Future[Either[String, Unit]],
      repairOperationAssertion: Future[Either[String, Unit]] => Assertion,
  ): Unit = {
    val initialIndexer = new TestRecoveringIndexer
    val afterRepairIndexer = new TestRecoveringIndexer
    val repairIndexer = new TestRepairIndexer
    val repairIndexerFactoryCalled = Promise[Unit]()
    val repairIndexerCreated = Promise[FutureQueue[Update]]()

    val indexerState = new IndexerState(
      recoveringIndexerFactory = seqFactory(
        initialIndexer,
        afterRepairIndexer,
      ),
      repairIndexerFactory = asyncSeqFactory(
        repairIndexerFactoryCalled -> repairIndexerCreated.future
      ),
      loggerFactory = loggerFactory,
    )

    // initial indexer is up and running
    Threading.sleep(20)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    initialIndexer.donePromise.isCompleted shouldBe false
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        2L -> update,
      )
    )
    // starting repair operation
    val repairOperationStartedPromise = Promise[Unit]()
    val repairOperationFinishedPromise = Promise[Unit]()
    val repairOperationF = indexerState.withRepairIndexer { _ =>
      repairOperationStartedPromise.trySuccess(())
      EitherT(repairOperationFinishedPromise.future.flatMap(_ => repairFunctionOutcome))
    }.value
    // first we wait for empty indexer queue
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    Threading.sleep(220)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    // and as the indexer queue is empty
    initialIndexer.uncommittedQueueSnapshotRef.set(Vector())
    // the initial Indexer is getting shut
    initialIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    initialIndexer.donePromise.isCompleted shouldBe false
    repairOperationStartedPromise.isCompleted shouldBe false
    repairIndexerFactoryCalled.isCompleted shouldBe false
    // then initial Indexer completes
    initialIndexer.donePromise.trySuccess(Done)
    // so repair Indexer is getting created
    repairIndexerFactoryCalled.future.futureValue
    Threading.sleep(20)
    repairOperationStartedPromise.isCompleted shouldBe false
    // and as repair indexer is created
    repairIndexerCreated.trySuccess(repairIndexer)
    // repair operation starts
    repairOperationStartedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // and as repair operation finishes with failure
    repairOperationFinishedPromise.trySuccess(())
    // then the repair-indexer should receive a shutdown (and no RepairCommit)
    repairIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    repairOperationF.isCompleted shouldBe false
    repairIndexer.repairReceivedPromise.isCompleted shouldBe false
    // and  as the repairIndexer is shut
    repairIndexer.donePromise.trySuccess(Done)
    Threading.sleep(20)
    repairOperationF.isCompleted shouldBe false
    repairIndexer.repairReceivedPromise.isCompleted shouldBe false
    // and also the new indexer is completed it's first successfull initialization
    afterRepairIndexer.firstSuccessfulConsumerInitializationPromise.trySuccess(())
    // repair operation should be completed
    repairOperationAssertion(repairOperationF)
    repairIndexer.repairReceivedPromise.isCompleted shouldBe false
    // and as shutting down the indexer state
    val indexerStateTerminated = indexerState.shutdown()
    // first the ongoing indexing should be shut
    afterRepairIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    indexerStateTerminated.isCompleted shouldBe false
    // and as the ongoing indexing is shut
    afterRepairIndexer.donePromise.trySuccess(Done)
    // indexer state should be terminated
    indexerStateTerminated.futureValue
  }

  it should "fail repair operation if the provided repair function fails with an Either.Left (also should tear down repair indexer, and switch back to normal indexing)" in
    testRepairFunctionFailure(
      repairFunctionOutcome = Future.successful(Left("failed")),
      repairOperationAssertion = _.futureValue shouldBe Left("failed"),
    )

  it should "fail repair operation if the provided repair function fails with an Future.failed (also should tear down repair indexer, and switch back to normal indexing)" in
    testRepairFunctionFailure(
      repairFunctionOutcome = Future.failed(new RuntimeException("failed with exception")),
      repairOperationAssertion = _.failed.futureValue.getMessage shouldBe "failed with exception",
    )

  it should "fail repair operation if the provided repair function fails with an exception before Future (also should tear down repair indexer, and switch back to normal indexing)" in {
    val initialIndexer = new TestRecoveringIndexer
    val afterRepairIndexer = new TestRecoveringIndexer
    val repairIndexer = new TestRepairIndexer
    val repairIndexerFactoryCalled = Promise[Unit]()
    val repairIndexerCreated = Promise[FutureQueue[Update]]()

    val indexerState = new IndexerState(
      recoveringIndexerFactory = seqFactory(
        initialIndexer,
        afterRepairIndexer,
      ),
      repairIndexerFactory = asyncSeqFactory(
        repairIndexerFactoryCalled -> repairIndexerCreated.future
      ),
      loggerFactory = loggerFactory,
    )

    // initial indexer is up and running
    Threading.sleep(20)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    initialIndexer.donePromise.isCompleted shouldBe false
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        2L -> update,
      )
    )
    // starting repair operation
    val repairOperationStartedPromise = Promise[Unit]()
    val repairOperationF = indexerState.withRepairIndexer { _ =>
      repairOperationStartedPromise.trySuccess(())
      throw new RuntimeException("failed with exception before Future")
    }.value
    // first we wait for empty indexer queue
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    Threading.sleep(220)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    // and as the indexer queue is empty
    initialIndexer.uncommittedQueueSnapshotRef.set(Vector())
    // the initial Indexer is getting shut
    initialIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    initialIndexer.donePromise.isCompleted shouldBe false
    repairOperationStartedPromise.isCompleted shouldBe false
    repairIndexerFactoryCalled.isCompleted shouldBe false
    // then initial Indexer completes
    initialIndexer.donePromise.trySuccess(Done)
    // so repair Indexer is getting created
    repairIndexerFactoryCalled.future.futureValue
    Threading.sleep(20)
    repairOperationStartedPromise.isCompleted shouldBe false
    // and as repair indexer is created
    repairIndexerCreated.trySuccess(repairIndexer)
    // repair operation starts
    repairOperationStartedPromise.future.futureValue
    Threading.sleep(20)
    repairOperationF.isCompleted shouldBe false
    // then the repair-indexer should receive a shutdown (and no RepairCommit)
    repairIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    repairOperationF.isCompleted shouldBe false
    repairIndexer.repairReceivedPromise.isCompleted shouldBe false
    // and  as the repairIndexer is shut
    repairIndexer.donePromise.trySuccess(Done)
    Threading.sleep(20)
    repairOperationF.isCompleted shouldBe false
    repairIndexer.repairReceivedPromise.isCompleted shouldBe false
    // and also the new indexer is completed it's first successfull initialization
    afterRepairIndexer.firstSuccessfulConsumerInitializationPromise.trySuccess(())
    // repair operation should be completed
    repairOperationF.failed.futureValue.getMessage shouldBe "failed with exception before Future"
    repairIndexer.repairReceivedPromise.isCompleted shouldBe false
    // and as shutting down the indexer state
    val indexerStateTerminated = indexerState.shutdown()
    // first the ongoing indexing should be shut
    afterRepairIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    indexerStateTerminated.isCompleted shouldBe false
    // and as the ongoing indexing is shut
    afterRepairIndexer.donePromise.trySuccess(Done)
    // indexer state should be terminated
    indexerStateTerminated.futureValue
  }

  it should "fail repair operation if the provided repair function succeeds, but commit fails to persist (also should tear down repair indexer, and switch back to normal indexing)" in {
    val initialIndexer = new TestRecoveringIndexer
    val afterRepairIndexer = new TestRecoveringIndexer
    val repairIndexer = new TestRepairIndexer
    val repairIndexerFactoryCalled = Promise[Unit]()
    val repairIndexerCreated = Promise[FutureQueue[Update]]()

    val indexerState = new IndexerState(
      recoveringIndexerFactory = seqFactory(
        initialIndexer,
        afterRepairIndexer,
      ),
      repairIndexerFactory = asyncSeqFactory(
        repairIndexerFactoryCalled -> repairIndexerCreated.future
      ),
      loggerFactory = loggerFactory,
    )

    // initial indexer is up and running
    Threading.sleep(20)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    initialIndexer.donePromise.isCompleted shouldBe false
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        2L -> update,
      )
    )
    // starting repair operation
    val repairOperationStartedPromise = Promise[Unit]()
    val repairOperationFinishedPromise = Promise[Unit]()
    val repairOperationF = indexerState.withRepairIndexer { _ =>
      repairOperationStartedPromise.trySuccess(())
      EitherT.right[String](repairOperationFinishedPromise.future)
    }.value
    // first we wait for empty indexer queue
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    Threading.sleep(220)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    // and as the indexer queue is empty
    initialIndexer.uncommittedQueueSnapshotRef.set(Vector())
    // the initial Indexer is getting shut
    initialIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    initialIndexer.donePromise.isCompleted shouldBe false
    repairOperationStartedPromise.isCompleted shouldBe false
    repairIndexerFactoryCalled.isCompleted shouldBe false
    // then initial Indexer completes
    initialIndexer.donePromise.trySuccess(Done)
    // so repair Indexer is getting created
    repairIndexerFactoryCalled.future.futureValue
    Threading.sleep(20)
    repairOperationStartedPromise.isCompleted shouldBe false
    // and as repair indexer is created
    repairIndexerCreated.trySuccess(repairIndexer)
    // repair operation starts
    repairOperationStartedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // and as repair operation finishes
    repairOperationFinishedPromise.trySuccess(())
    // first the the CommitRepair is expected to be offered to the repairIndexer
    repairIndexer.repairReceivedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false

    loggerFactory.assertLogs(
      {
        // then waiting for the message to be persisted, but that fails
        repairIndexer.repairPersistedPromise.tryFailure(
          new RuntimeException("commit failed to process")
        )
        // then the repair-indexer should receive the shutdown signal
        repairIndexer.shutdownPromise.future.futureValue
        Threading.sleep(20)
        repairOperationF.isCompleted shouldBe false
        // and as the repair-indexer shuts down
        repairIndexer.donePromise.trySuccess(Done)
        Threading.sleep(20)
        repairOperationF.isCompleted shouldBe false
        // and also the new indexer is completed it's first successfull initialization
        afterRepairIndexer.firstSuccessfulConsumerInitializationPromise.trySuccess(())
        // repair operation should be completed with a failure
        repairOperationF.failed.futureValue.getMessage shouldBe "Committing repair changes failed"
        repairOperationF.failed.futureValue.getCause.getMessage shouldBe "commit failed to process"
      },
      _.warningMessage should include(
        "Committing repair changes failed, resuming normal indexing..."
      ),
    )

    // and as shutting down the indexer state
    val indexerStateTerminated = indexerState.shutdown()
    // first the ongoing indexing should be shut
    afterRepairIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    indexerStateTerminated.isCompleted shouldBe false
    // and as the ongoing indexing is shut
    afterRepairIndexer.donePromise.trySuccess(Done)
    // indexer state should be terminated
    indexerStateTerminated.futureValue
  }

  it should "not fail an otherwise successfull repair operation after successful persisting the RepairCommit message, if the repair indexer shutdown is not successful" in {
    val initialIndexer = new TestRecoveringIndexer
    val afterRepairIndexer = new TestRecoveringIndexer
    val repairIndexer = new TestRepairIndexer
    val repairIndexerFactoryCalled = Promise[Unit]()
    val repairIndexerCreated = Promise[FutureQueue[Update]]()

    val indexerState = new IndexerState(
      recoveringIndexerFactory = seqFactory(
        initialIndexer,
        afterRepairIndexer,
      ),
      repairIndexerFactory = asyncSeqFactory(
        repairIndexerFactoryCalled -> repairIndexerCreated.future
      ),
      loggerFactory = loggerFactory,
    )

    // initial indexer is up and running
    Threading.sleep(20)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    initialIndexer.donePromise.isCompleted shouldBe false
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        2L -> update,
      )
    )
    // starting repair operation
    val repairOperationStartedPromise = Promise[Unit]()
    val repairOperationFinishedPromise = Promise[Unit]()
    val repairOperationF = indexerState.withRepairIndexer { _ =>
      repairOperationStartedPromise.trySuccess(())
      EitherT.right[String](repairOperationFinishedPromise.future)
    }.value
    // first we wait for empty indexer queue
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    Threading.sleep(220)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    // and as the indexer queue is empty
    initialIndexer.uncommittedQueueSnapshotRef.set(Vector())
    // the initial Indexer is getting shut
    initialIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    initialIndexer.donePromise.isCompleted shouldBe false
    repairOperationStartedPromise.isCompleted shouldBe false
    repairIndexerFactoryCalled.isCompleted shouldBe false
    // then initial Indexer completes
    initialIndexer.donePromise.trySuccess(Done)
    // so repair Indexer is getting created
    repairIndexerFactoryCalled.future.futureValue
    Threading.sleep(20)
    repairOperationStartedPromise.isCompleted shouldBe false
    // and as repair indexer is created
    repairIndexerCreated.trySuccess(repairIndexer)
    // repair operation starts
    repairOperationStartedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // and as repair operation finishes
    repairOperationFinishedPromise.trySuccess(())
    // first the the CommitRepair is expected to be offered to the repairIndexer
    repairIndexer.repairReceivedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // then waiting for the message to be persisted
    repairIndexer.repairPersistedPromise.trySuccess(())

    loggerFactory.assertLogs(
      {
        // then the repair-indexer should be completing without shutdown signal
        repairIndexer.donePromise.tryFailure(new RuntimeException("shutdown failure"))
        Threading.sleep(20)
        repairIndexer.shutdownPromise.isCompleted shouldBe false
        repairOperationF.isCompleted shouldBe false
        // and also the new indexer is completed it's first successfull initialization
        afterRepairIndexer.firstSuccessfulConsumerInitializationPromise.trySuccess(())
        // repair operation should be completed
        repairOperationF.futureValue
      },
      _.warningMessage should include("Repair Indexer finished with error"),
    )

    // and as shutting down the indexer state
    val indexerStateTerminated = indexerState.shutdown()
    // first the ongoing indexing should be shut
    afterRepairIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    indexerStateTerminated.isCompleted shouldBe false
    // and as the ongoing indexing is shut
    afterRepairIndexer.donePromise.trySuccess(Done)
    // indexer state should be terminated
    indexerStateTerminated.futureValue
  }

  it should "not even initiate the repair indexer if getting shutdown before initialization started" in {
    val initialIndexer = new TestRecoveringIndexer
    val repairIndexerFactoryCalled = Promise[Unit]()
    val repairIndexerCreated = Promise[FutureQueue[Update]]()

    val indexerState = new IndexerState(
      recoveringIndexerFactory = seqFactory(initialIndexer),
      repairIndexerFactory = asyncSeqFactory(
        repairIndexerFactoryCalled -> repairIndexerCreated.future
      ),
      loggerFactory = loggerFactory,
    )

    // initial indexer is up and running
    Threading.sleep(20)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    initialIndexer.donePromise.isCompleted shouldBe false
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        2L -> update,
      )
    )
    // starting repair operation
    val repairOperationStartedPromise = Promise[Unit]()
    val repairOperationF = indexerState.withRepairIndexer { _ =>
      repairOperationStartedPromise.trySuccess(())
      fail()
    }.value
    // first we wait for empty indexer queue
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    Threading.sleep(220)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    // and as the indexer queue is empty
    initialIndexer.uncommittedQueueSnapshotRef.set(Vector())
    // the initial Indexer is getting shut
    initialIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    initialIndexer.donePromise.isCompleted shouldBe false
    repairOperationStartedPromise.isCompleted shouldBe false
    repairIndexerFactoryCalled.isCompleted shouldBe false
    // inbetween shutting down the indexer state
    val indexerStateTerminated = indexerState.shutdown()
    // and then initial Indexer completes
    initialIndexer.donePromise.trySuccess(Done)
    // then repair operation should be terminating too
    repairOperationF.failed.futureValue.getMessage shouldBe "Shutdown in progress"
    // indexer state should be terminated
    indexerStateTerminated.futureValue
    // and repair indexer never started to initialize
    repairIndexerFactoryCalled.isCompleted shouldBe false
  }

  it should "not even start the repair operation if getting shut down during repair indexer initialization" in {
    val initialIndexer = new TestRecoveringIndexer
    val repairIndexer = new TestRepairIndexer
    val repairIndexerFactoryCalled = Promise[Unit]()
    val repairIndexerCreated = Promise[FutureQueue[Update]]()

    val indexerState = new IndexerState(
      recoveringIndexerFactory = seqFactory(initialIndexer),
      repairIndexerFactory = asyncSeqFactory(
        repairIndexerFactoryCalled -> repairIndexerCreated.future
      ),
      loggerFactory = loggerFactory,
    )

    // initial indexer is up and running
    Threading.sleep(20)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    initialIndexer.donePromise.isCompleted shouldBe false
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        2L -> update,
      )
    )
    // starting repair operation
    val repairOperationStartedPromise = Promise[Unit]()
    val repairOperationF = indexerState.withRepairIndexer { _ =>
      repairOperationStartedPromise.trySuccess(())
      fail()
    }.value
    // first we wait for empty indexer queue
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    Threading.sleep(220)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    // and as the indexer queue is empty
    initialIndexer.uncommittedQueueSnapshotRef.set(Vector())
    // the initial Indexer is getting shut
    initialIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    initialIndexer.donePromise.isCompleted shouldBe false
    repairOperationStartedPromise.isCompleted shouldBe false
    repairIndexerFactoryCalled.isCompleted shouldBe false
    // then initial Indexer completes
    initialIndexer.donePromise.trySuccess(Done)
    // so repair Indexer is getting created
    repairIndexerFactoryCalled.future.futureValue
    Threading.sleep(20)
    repairOperationStartedPromise.isCompleted shouldBe false
    // and as shutting down the indexer state
    val indexerStateTerminated = indexerState.shutdown()
    // and as repair indexer is created
    repairIndexerCreated.trySuccess(repairIndexer)
    Threading.sleep(20)
    // repair operation does not start
    repairOperationStartedPromise.isCompleted shouldBe false
    // repair operation should be completed with a failure
    repairOperationF.failed.futureValue.getMessage shouldBe "Shutdown in progress"
    // then repair-indexer should receive a shutdown signal
    repairIndexer.shutdownPromise.future.futureValue
    // then the repair-indexer should be completing
    repairIndexer.donePromise.trySuccess(Done)
    // indexer state should be terminated
    indexerStateTerminated.futureValue
    // repair operation never started
    repairOperationStartedPromise.isCompleted shouldBe false
  }

  it should "not attempt to commit repair if getting shut down during repair operation, which then results in an success" in {
    val initialIndexer = new TestRecoveringIndexer
    val repairIndexer = new TestRepairIndexer
    val repairIndexerFactoryCalled = Promise[Unit]()
    val repairIndexerCreated = Promise[FutureQueue[Update]]()

    val indexerState = new IndexerState(
      recoveringIndexerFactory = seqFactory(initialIndexer),
      repairIndexerFactory = asyncSeqFactory(
        repairIndexerFactoryCalled -> repairIndexerCreated.future
      ),
      loggerFactory = loggerFactory,
    )

    // initial indexer is up and running
    Threading.sleep(20)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    initialIndexer.donePromise.isCompleted shouldBe false
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        2L -> update,
      )
    )
    // starting repair operation
    val repairOperationStartedPromise = Promise[Unit]()
    val repairOperationFinishedPromise = Promise[Unit]()
    val repairOperationF = indexerState.withRepairIndexer { _ =>
      repairOperationStartedPromise.trySuccess(())
      EitherT.right[String](repairOperationFinishedPromise.future)
    }.value
    // first we wait for empty indexer queue
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    Threading.sleep(220)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    // and as the indexer queue is empty
    initialIndexer.uncommittedQueueSnapshotRef.set(Vector())
    // the initial Indexer is getting shut
    initialIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    initialIndexer.donePromise.isCompleted shouldBe false
    repairOperationStartedPromise.isCompleted shouldBe false
    repairIndexerFactoryCalled.isCompleted shouldBe false
    // then initial Indexer completes
    initialIndexer.donePromise.trySuccess(Done)
    // so repair Indexer is getting created
    repairIndexerFactoryCalled.future.futureValue
    Threading.sleep(20)
    repairOperationStartedPromise.isCompleted shouldBe false
    // and as repair indexer is created
    repairIndexerCreated.trySuccess(repairIndexer)
    // repair operation starts
    repairOperationStartedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // and as shutting down the indexer state
    val indexerStateTerminated = indexerState.shutdown()
    // then the repair-indexer should receive a shutdown signal
    repairIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    repairOperationF.isCompleted shouldBe false
    // and as repair operation finishes with success
    repairOperationFinishedPromise.trySuccess(())
    // repair operation should be finished with an error already before the repairIndexer is shut
    repairOperationF.failed.futureValue.getMessage shouldBe "Shutdown in progress"
    Threading.sleep(20)
    // and as repairIndexer is shut
    repairIndexer.donePromise.trySuccess(Done)
    // eventually indexer state should be terminated
    indexerStateTerminated.futureValue
    Threading.sleep(20)
    // and committing the changes should never be triggered
    repairIndexer.repairReceivedPromise.isCompleted shouldBe false
  }

  it should "not attempt to resume indexing if getting shut down during repair operation, which then results in an error" in {
    val initialIndexer = new TestRecoveringIndexer
    val repairIndexer = new TestRepairIndexer
    val repairIndexerFactoryCalled = Promise[Unit]()
    val repairIndexerCreated = Promise[FutureQueue[Update]]()

    val indexerState = new IndexerState(
      recoveringIndexerFactory = seqFactory(initialIndexer),
      repairIndexerFactory = asyncSeqFactory(
        repairIndexerFactoryCalled -> repairIndexerCreated.future
      ),
      loggerFactory = loggerFactory,
    )

    // initial indexer is up and running
    Threading.sleep(20)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    initialIndexer.donePromise.isCompleted shouldBe false
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        2L -> update,
      )
    )
    // starting repair operation
    val repairOperationStartedPromise = Promise[Unit]()
    val repairOperationFinishedPromise = Promise[Unit]()
    val repairOperationF = indexerState.withRepairIndexer { _ =>
      repairOperationStartedPromise.trySuccess(())
      EitherT.right[String](repairOperationFinishedPromise.future)
    }.value
    // first we wait for empty indexer queue
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    Threading.sleep(220)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    // and as the indexer queue is empty
    initialIndexer.uncommittedQueueSnapshotRef.set(Vector())
    // the initial Indexer is getting shut
    initialIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    initialIndexer.donePromise.isCompleted shouldBe false
    repairOperationStartedPromise.isCompleted shouldBe false
    repairIndexerFactoryCalled.isCompleted shouldBe false
    // then initial Indexer completes
    initialIndexer.donePromise.trySuccess(Done)
    // so repair Indexer is getting created
    repairIndexerFactoryCalled.future.futureValue
    Threading.sleep(20)
    repairOperationStartedPromise.isCompleted shouldBe false
    // and as repair indexer is created
    repairIndexerCreated.trySuccess(repairIndexer)
    // repair operation starts
    repairOperationStartedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // and as shutting down the indexer state
    val indexerStateTerminated = indexerState.shutdown()
    // then the repair-indexer should receive a shutdown signal
    repairIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    repairOperationF.isCompleted shouldBe false
    // and as repair operation finishes with an error
    repairOperationFinishedPromise.tryFailure(new RuntimeException("aborted"))
    // repair operation should be finished with an error already before the repairIndexer is shut
    repairOperationF.failed.futureValue.getMessage shouldBe "Shutdown in progress"
    Threading.sleep(20)
    // and as repairIndexer is shut
    repairIndexer.donePromise.trySuccess(Done)
    // eventually indexer state should be terminated
    indexerStateTerminated.futureValue
    // and committing the changes should never be triggered
    repairIndexer.repairReceivedPromise.isCompleted shouldBe false
  }

  it should "not fail an otherwise successfull repair operation after successful persisting the RepairCommit message, if getting shutdown during committing" in {
    val initialIndexer = new TestRecoveringIndexer
    val repairIndexer = new TestRepairIndexer
    val repairIndexerFactoryCalled = Promise[Unit]()
    val repairIndexerCreated = Promise[FutureQueue[Update]]()

    val indexerState = new IndexerState(
      recoveringIndexerFactory = seqFactory(initialIndexer),
      repairIndexerFactory = asyncSeqFactory(
        repairIndexerFactoryCalled -> repairIndexerCreated.future
      ),
      loggerFactory = loggerFactory,
    )

    // initial indexer is up and running
    Threading.sleep(20)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    initialIndexer.donePromise.isCompleted shouldBe false
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        2L -> update,
      )
    )
    // starting repair operation
    val repairOperationStartedPromise = Promise[Unit]()
    val repairOperationFinishedPromise = Promise[Unit]()
    val repairOperationF = indexerState.withRepairIndexer { _ =>
      repairOperationStartedPromise.trySuccess(())
      EitherT.right[String](repairOperationFinishedPromise.future)
    }.value
    // first we wait for empty indexer queue
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    Threading.sleep(220)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    // and as the indexer queue is empty
    initialIndexer.uncommittedQueueSnapshotRef.set(Vector())
    // the initial Indexer is getting shut
    initialIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    initialIndexer.donePromise.isCompleted shouldBe false
    repairOperationStartedPromise.isCompleted shouldBe false
    repairIndexerFactoryCalled.isCompleted shouldBe false
    // then initial Indexer completes
    initialIndexer.donePromise.trySuccess(Done)
    // so repair Indexer is getting created
    repairIndexerFactoryCalled.future.futureValue
    Threading.sleep(20)
    repairOperationStartedPromise.isCompleted shouldBe false
    // and as repair indexer is created
    repairIndexerCreated.trySuccess(repairIndexer)
    // repair operation starts
    repairOperationStartedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // and as repair operation finishes
    repairOperationFinishedPromise.trySuccess(())
    // first the the CommitRepair is expected to be offered to the repairIndexer
    repairIndexer.repairReceivedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // and then shutting down the indexer state
    val indexerStateTerminated = indexerState.shutdown()
    // repair indexer should receive the shutdown signal immediately
    repairIndexer.shutdownPromise.future.futureValue
    // then the message is persisted
    repairIndexer.repairPersistedPromise.trySuccess(())
    // then the repair-indexer should be completing
    repairIndexer.donePromise.trySuccess(Done)
    Threading.sleep(20)
    // repair operation should be completed successfully regardless
    repairOperationF.futureValue
    // indexer state should be terminated
    indexerStateTerminated.futureValue
  }

  it should "fail a repair operation, which failed at persisting the RepairCommit message, with the RepairCommit failed error, if getting shutdown during committing" in {
    val initialIndexer = new TestRecoveringIndexer
    val repairIndexer = new TestRepairIndexer
    val repairIndexerFactoryCalled = Promise[Unit]()
    val repairIndexerCreated = Promise[FutureQueue[Update]]()

    val indexerState = new IndexerState(
      recoveringIndexerFactory = seqFactory(initialIndexer),
      repairIndexerFactory = asyncSeqFactory(
        repairIndexerFactoryCalled -> repairIndexerCreated.future
      ),
      loggerFactory = loggerFactory,
    )

    // initial indexer is up and running
    Threading.sleep(20)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    initialIndexer.donePromise.isCompleted shouldBe false
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        2L -> update,
      )
    )
    // starting repair operation
    val repairOperationStartedPromise = Promise[Unit]()
    val repairOperationFinishedPromise = Promise[Unit]()
    val repairOperationF = indexerState.withRepairIndexer { _ =>
      repairOperationStartedPromise.trySuccess(())
      EitherT.right[String](repairOperationFinishedPromise.future)
    }.value
    // first we wait for empty indexer queue
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    Threading.sleep(220)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    // and as the indexer queue is empty
    initialIndexer.uncommittedQueueSnapshotRef.set(Vector())
    // the initial Indexer is getting shut
    initialIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    initialIndexer.donePromise.isCompleted shouldBe false
    repairOperationStartedPromise.isCompleted shouldBe false
    repairIndexerFactoryCalled.isCompleted shouldBe false
    // then initial Indexer completes
    initialIndexer.donePromise.trySuccess(Done)
    // so repair Indexer is getting created
    repairIndexerFactoryCalled.future.futureValue
    Threading.sleep(20)
    repairOperationStartedPromise.isCompleted shouldBe false
    // and as repair indexer is created
    repairIndexerCreated.trySuccess(repairIndexer)
    // repair operation starts
    repairOperationStartedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // and as repair operation finishes
    repairOperationFinishedPromise.trySuccess(())
    // first the the CommitRepair is expected to be offered to the repairIndexer
    repairIndexer.repairReceivedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // and then shutting down the indexer state
    val indexerStateTerminated = indexerState.shutdown()
    // repair indexer should receive the shutdown signal immediately
    repairIndexer.shutdownPromise.future.futureValue

    loggerFactory.assertLogs(
      {
        // then the commit message fails persisting
        repairIndexer.repairPersistedPromise.tryFailure(new RuntimeException("failed to persist"))
        // then the repair-indexer should be completing
        repairIndexer.donePromise.trySuccess(Done)
        // repair operation should be completed with the commit failure
        repairOperationF.failed.futureValue.getMessage shouldBe "Committing repair changes failed"
        repairOperationF.failed.futureValue.getCause.getMessage shouldBe "failed to persist"
      },
      _.warningMessage should include(
        "Committing repair changes failed, resuming normal indexing..."
      ),
    )

    // indexer state should be terminated
    indexerStateTerminated.futureValue
  }

  it should "not fail otherwise successful repair operation if shutdown precedes initialization of the next indexer, but report a warning" in {
    val initialIndexer = new TestRecoveringIndexer
    val afterRepairIndexer = new TestRecoveringIndexer
    val repairIndexer = new TestRepairIndexer
    val repairIndexerFactoryCalled = Promise[Unit]()
    val repairIndexerCreated = Promise[FutureQueue[Update]]()

    val indexerState = new IndexerState(
      recoveringIndexerFactory = seqFactory(
        initialIndexer,
        afterRepairIndexer,
      ),
      repairIndexerFactory = asyncSeqFactory(
        repairIndexerFactoryCalled -> repairIndexerCreated.future
      ),
      loggerFactory = loggerFactory,
    )

    // initial indexer is up and running
    Threading.sleep(20)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    initialIndexer.donePromise.isCompleted shouldBe false
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        2L -> update,
      )
    )
    // starting repair operation
    val repairOperationStartedPromise = Promise[Unit]()
    val repairOperationFinishedPromise = Promise[Unit]()
    val repairOperationF = indexerState.withRepairIndexer { _ =>
      repairOperationStartedPromise.trySuccess(())
      EitherT.right[String](repairOperationFinishedPromise.future)
    }.value
    // first we wait for empty indexer queue
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    Threading.sleep(220)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    // and as the indexer queue is empty
    initialIndexer.uncommittedQueueSnapshotRef.set(Vector())
    // the initial Indexer is getting shut
    initialIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    initialIndexer.donePromise.isCompleted shouldBe false
    repairOperationStartedPromise.isCompleted shouldBe false
    repairIndexerFactoryCalled.isCompleted shouldBe false
    // then initial Indexer completes
    initialIndexer.donePromise.trySuccess(Done)
    // so repair Indexer is getting created
    repairIndexerFactoryCalled.future.futureValue
    Threading.sleep(20)
    repairOperationStartedPromise.isCompleted shouldBe false
    // and as repair indexer is created
    repairIndexerCreated.trySuccess(repairIndexer)
    // repair operation starts
    repairOperationStartedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // and as repair operation finishes
    repairOperationFinishedPromise.trySuccess(())
    // first the the CommitRepair is expected to be offered to the repairIndexer
    repairIndexer.repairReceivedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false

    loggerFactory.assertEventuallyLogsSeq(
      SuppressionRule.forLogger[IndexerState] && SuppressionRule.Level(org.slf4j.event.Level.INFO)
    )(
      within = {
        // then waiting for the message to be persisted
        repairIndexer.repairPersistedPromise.trySuccess(())
        // then the repair-indexer should be completing without shutdown signal
        repairIndexer.donePromise.trySuccess(Done)
        Threading.sleep(20)
        repairIndexer.shutdownPromise.isCompleted shouldBe false
        repairOperationF.isCompleted shouldBe false
      },
      // waiting until the next indexer is started, so shutdown comes after the IndexerState transition
      assertion = _.find(_.infoMessage.contains("Switched to Normal Mode")).nonEmpty shouldBe true,
    )

    // as shutting down the indexer state
    val indexerStateTerminated = indexerState.shutdown()
    // first the ongoing indexing should be shut
    afterRepairIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    indexerStateTerminated.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // and as the ongoing indexing is shut
    afterRepairIndexer.donePromise.trySuccess(Done)
    // indexer state should be terminated
    indexerStateTerminated.futureValue
    Threading.sleep(20)
    repairOperationF.isCompleted shouldBe false
    // and also the new indexer signals first successful initialization failure
    // (this is the expected behavior from the RecoveringIndexer)
    afterRepairIndexer.firstSuccessfulConsumerInitializationPromise.trySuccess(())
    // repair operation should be completed
    repairOperationF.futureValue
  }

  it should "ensureNoProcessingForDomain works as expected" in {
    val domain1 = DomainId.tryFromString("x::domain1")
    val domain2 = DomainId.tryFromString("x::domain2")
    val domain3 = DomainId.tryFromString("x::domain3")

    val initialIndexer = new TestRecoveringIndexer
    val afterRepairIndexer = new TestRecoveringIndexer
    val repairIndexer = new TestRepairIndexer
    val repairIndexerFactoryCalled = Promise[Unit]()
    val repairIndexerCreated = Promise[FutureQueue[Update]]()

    val indexerState = new IndexerState(
      recoveringIndexerFactory = seqFactory(
        initialIndexer,
        afterRepairIndexer,
      ),
      repairIndexerFactory = asyncSeqFactory(
        repairIndexerFactoryCalled -> repairIndexerCreated.future
      ),
      loggerFactory = loggerFactory,
    )

    // initial indexer is up and running
    Threading.sleep(20)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    initialIndexer.donePromise.isCompleted shouldBe false
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        2L -> update,
        3L -> update.copy(domainId = domain1),
        4L -> update.copy(domainId = domain1),
        5L -> update.copy(domainId = domain2),
        6L -> update.copy(domainId = domain2),
        7L -> update.copy(domainId = domain3),
        8L -> update.copy(domainId = domain3),
      )
    )
    val ensureDomain1 = indexerState.ensureNoProcessingForDomain(domain1)
    val ensureDomain2 = indexerState.ensureNoProcessingForDomain(domain2)
    val ensureDomain3 = indexerState.ensureNoProcessingForDomain(domain3)
    Threading.sleep(20)
    ensureDomain1.isCompleted shouldBe false
    ensureDomain2.isCompleted shouldBe false
    ensureDomain3.isCompleted shouldBe false
    // remove the domain1
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        2L -> update,
        6L -> update.copy(domainId = domain2),
        7L -> update.copy(domainId = domain3),
        8L -> update.copy(domainId = domain3),
      )
    )
    Threading.sleep(110)
    ensureDomain1.futureValue
    ensureDomain2.isCompleted shouldBe false
    ensureDomain3.isCompleted shouldBe false
    // remove the domain2
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update,
        7L -> update.copy(domainId = domain3),
        8L -> update.copy(domainId = domain3),
      )
    )
    Threading.sleep(110)
    ensureDomain2.futureValue
    ensureDomain3.isCompleted shouldBe false
    // remove the domain3
    initialIndexer.uncommittedQueueSnapshotRef.set(
      Vector(
        1L -> update
      )
    )
    ensureDomain3.futureValue
    // starting repair operation
    val repairOperationStartedPromise = Promise[Unit]()
    val repairOperationFinishedPromise = Promise[Unit]()
    val repairOperationF = indexerState.withRepairIndexer { repairQueue =>
      repairQueue.offer(repairUpdate).futureValue shouldBe Done
      repairOperationStartedPromise.trySuccess(())
      EitherT.right[String](repairOperationFinishedPromise.future)
    }.value
    // first we wait for empty indexer queue
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    Threading.sleep(220)
    initialIndexer.shutdownPromise.isCompleted shouldBe false
    // as repair operation started the ensureNoProcessingForDomain should fail
    indexerState.ensureNoProcessingForDomain(domain1).failed.futureValue match {
      case _: RepairInProgress => ()
      case invalid => fail("Invalid error", invalid)
    }
    // and as the indexer queue is empty
    initialIndexer.uncommittedQueueSnapshotRef.set(Vector())
    // the initial Indexer is getting shut
    initialIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    initialIndexer.donePromise.isCompleted shouldBe false
    repairOperationStartedPromise.isCompleted shouldBe false
    repairIndexerFactoryCalled.isCompleted shouldBe false
    // then initial Indexer completes
    initialIndexer.donePromise.trySuccess(Done)
    // so repair Indexer is getting created
    repairIndexerFactoryCalled.future.futureValue
    Threading.sleep(20)
    repairOperationStartedPromise.isCompleted shouldBe false
    // and as repair indexer is created
    repairIndexerCreated.trySuccess(repairIndexer)
    // repair operation starts
    repairOperationStartedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // and as repair operation finishes
    repairOperationFinishedPromise.trySuccess(())
    // first the the CommitRepair is expected to be offered to the repairIndexer
    repairIndexer.repairReceivedPromise.future.futureValue
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // then waiting for the message to be persisted
    repairIndexer.repairPersistedPromise.trySuccess(())
    // then the repair-indexer should be completing without shutdown signal
    repairIndexer.donePromise.trySuccess(Done)
    Threading.sleep(20)
    repairIndexer.shutdownPromise.isCompleted shouldBe false
    repairOperationF.isCompleted shouldBe false
    // and also the new indexer is completed it's first successfull initialization
    afterRepairIndexer.firstSuccessfulConsumerInitializationPromise.trySuccess(())
    // repair operation should be completed
    repairOperationF.futureValue
    // and as shutting down the indexer state
    val indexerStateTerminated = indexerState.shutdown()
    // first the ongoing indexing should be shut
    afterRepairIndexer.shutdownPromise.future.futureValue
    Threading.sleep(20)
    indexerStateTerminated.isCompleted shouldBe false
    // as shutdown started the ensureNoProcessingForDomain should fail
    indexerState
      .ensureNoProcessingForDomain(domain1)
      .failed
      .futureValue
      .getMessage shouldBe "Shutdown in progress"
    // and as the ongoing indexing is shut
    afterRepairIndexer.donePromise.trySuccess(Done)
    // indexer state should be terminated
    indexerStateTerminated.futureValue
  }

  behavior of "IndexerQueueProxy"

  it should "fail shutdown and done methods, as not supposed to be used" in {
    val indexerQueueProxy = new IndexerQueueProxy(_ => Future.successful(Done))
    assertThrows[UnsupportedOperationException](indexerQueueProxy.shutdown())
    assertThrows[UnsupportedOperationException](indexerQueueProxy.done)
  }

  it should "allow offer for normal indexing" in {
    val indexerQueueProxy =
      new IndexerQueueProxy(stateF => stateF(IndexerState.Normal(new TestRecoveringIndexer, false)))
    indexerQueueProxy.offer(update).futureValue
  }

  it should "deny offer CommitRepair for normal indexing" in {
    val indexerQueueProxy =
      new IndexerQueueProxy(stateF => stateF(IndexerState.Normal(new TestRecoveringIndexer, false)))
    val commitRepair = Update.CommitRepair()
    indexerQueueProxy
      .offer(commitRepair)
      .failed
      .futureValue
      .getMessage shouldBe "CommitRepair should not be used"
    commitRepair.persisted.future.failed.futureValue.getMessage shouldBe "CommitRepair should not be used"
  }

  it should "propagate any exception to offer calls" in {
    val indexerQueueProxy = new IndexerQueueProxy(_ => throw new IllegalStateException("nah"))
    intercept[IllegalStateException](
      indexerQueueProxy.offer(update)
    ).getMessage shouldBe "nah"
  }

  it should "deny offer during repair indexing" in {
    val repairDone = Promise[Unit]()
    val indexerQueueProxy = new IndexerQueueProxy(stateF =>
      stateF(IndexerState.Repair(Future.never, repairDone.future, false))
    )
    val repairDoneReturned =
      indexerQueueProxy.offer(update).failed.futureValue match {
        case repairInProgress: RepairInProgress => repairInProgress.repairDone
        case _ => fail()
      }
    repairDoneReturned.isCompleted shouldBe false
    repairDone.trySuccess(())
    repairDoneReturned.futureValue
  }

  class TestRecoveringIndexer extends RecoveringFutureQueue[Update] {
    val donePromise = Promise[Done]()
    val shutdownPromise = Promise[Unit]()
    val firstSuccessfulConsumerInitializationPromise = Promise[Unit]()
    val uncommittedQueueSnapshotRef =
      new AtomicReference[Vector[(Long, Update)]](Vector.empty)

    override def firstSuccessfulConsumerInitialization: Future[Unit] =
      firstSuccessfulConsumerInitializationPromise.future

    override def uncommittedQueueSnapshot: Vector[(Long, Update)] =
      uncommittedQueueSnapshotRef.get()

    override def offer(elem: Update): Future[Done] = Future.successful(Done)

    override def shutdown(): Unit = shutdownPromise.trySuccess(())

    override def done: Future[Done] = donePromise.future
  }

  class TestRepairIndexer extends FutureQueue[Update] {
    val donePromise = Promise[Done]()
    val shutdownPromise = Promise[Unit]()
    val repairReceivedPromise = Promise[Unit]()
    val repairPersistedPromise = Promise[Unit]()

    override def offer(elem: Update): Future[Done] = elem match {
      case commitRepair: Update.CommitRepair =>
        repairReceivedPromise.trySuccess(())
        repairPersistedPromise.future.onComplete(commitRepair.persisted.tryComplete)
        Future.successful(Done)

      case _ =>
        Future.successful(Done)
    }

    override def shutdown(): Unit = shutdownPromise.trySuccess(())

    override def done: Future[Done] = donePromise.future
  }

  def seqFactory[T](ts: T*): () => T = {
    val atomicTQueue: AtomicReference[List[T]] = new AtomicReference[List[T]](ts.toList)
    () => atomicTQueue.getAndUpdate(_.tail).head
  }

  def asyncSeqFactory[T](promises: (Promise[Unit], Future[T])*): () => Future[T] = {
    val factory = seqFactory(promises*)
    () => {
      val (calledPromise, resultFuture) = factory()
      calledPromise.trySuccess(())
      resultFuture
    }
  }

  def update: Update.SequencerIndexMoved =
    Update.SequencerIndexMoved(
      domainId = DomainId.tryFromString("x::domain"),
      sequencerCounter = SequencerCounter(15L),
      recordTime = CantonTimestamp.now(),
      requestCounterO = None,
    )

  def repairUpdate: RepairUpdate = mock[RepairUpdate]

}
