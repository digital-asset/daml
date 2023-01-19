// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import PruningStateManager._

class PruningStateManager(
    maxBatchSize: Int,
    executionContext: ExecutionContext,
) {
  private val logger = ContextualizedLogger.get(getClass)
  @volatile private var pruningStateRef: PruningState = PruningStateAtStartup
  private implicit val ec: ExecutionContext = executionContext

  // TODO pruning: Allow the LAPI to query the pruning state/progress?
  def startAsyncPrune(
      latestPrunedUpToInclusive: Offset,
      upToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
  )(getOffsetAfter: (Offset, Int) => Future[Offset], prune: (Offset, Boolean) => Future[Unit])(
      implicit loggingContext: LoggingContext
  ): Either[String, Unit] =
    pruningStateRef.synchronized {
      Either.cond(
        pruningStateRef.canBeStarted, {
          val newPruningState = PruningStateInitialized()
          newPruningState.pruneInternally(
            latestPrunedUpToInclusive = latestPrunedUpToInclusive,
            pruneUpToInclusive = upToInclusive,
            pruneAllDivulgedContracts = pruneAllDivulgedContracts,
            maxBatchSize = maxBatchSize,
          )(
            getOffsetAfter = getOffsetAfter,
            prune = prune,
          )
          pruningStateRef = newPruningState
          ()
        },
        "Pruning in progress. TODO log as error code",
      )
    }

  def running(implicit loggingContext: LoggingContext): Future[Unit] = {
    val isRunning = !pruningStateRef.canBeStarted
    logger.info(s"Prune status queried: $pruningStateRef and running: $isRunning")
    pruningStateRef.running
  }

  def shutdown(): Future[Unit] =
    pruningStateRef.synchronized {
      val originalState = pruningStateRef
      pruningStateRef = PruningStateShutdown
      originalState.abort
    }

  def start(): Future[Unit] = pruningStateRef.synchronized {
    val oldState = pruningStateRef
    pruningStateRef = PruningStateAtStartup
    oldState.abort // Wait for old state to finish in case
  }
}

object PruningStateManager {
  def apply(maxBatchSize: Int): ResourceOwner[PruningStateManager] =
    for {
      executorService <- ResourceOwner.forExecutorService(() => Executors.newSingleThreadExecutor())
      // TODO pruning: instrument
      executionContext = ExecutionContext.fromExecutorService(executorService)
      pruningStateManager <- ResourceOwner.forReleasable(() =>
        new PruningStateManager(maxBatchSize, executionContext)
      )(_.shutdown())
    } yield pruningStateManager

  sealed trait PruningState extends Product with Serializable {
    def canBeStarted: Boolean

    def running: Future[Unit]

    def abort: Future[Unit]
  }

  final case object PruningStateAtStartup extends PruningState {
    override val canBeStarted: Boolean = true
    override val abort: Future[Unit] = Future.unit
    override val running: Future[Unit] = Future.unit
  }

  final case object PruningStateShutdown extends PruningState {
    override val canBeStarted: Boolean = false
    override val abort: Future[Unit] = Future.unit
    override val running: Future[Unit] = Future.unit
  }

  final case class PruningStateInitialized() extends PruningState {
    private val logger = ContextualizedLogger.get(getClass)
    @volatile private var aborted: Boolean = false
    @volatile private var runningF: Future[Unit] = Future.unit

    def pruneInternally(
        latestPrunedUpToInclusive: Offset,
        pruneUpToInclusive: Offset,
        pruneAllDivulgedContracts: Boolean, // TODO pruning: Consider in prunedUpToInclusive
        maxBatchSize: Int,
    )(getOffsetAfter: (Offset, Int) => Future[Offset], prune: (Offset, Boolean) => Future[Unit])(
        implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Unit = {
      def go(start: Offset): Future[Unit] =
        if (aborted || start >= pruneUpToInclusive)
          Future.unit
        else {
          getOffsetAfter(start, maxBatchSize)
            .flatMap { translatedOffset =>
              val offsetToPruneTo = Ordering[Offset].min(translatedOffset, pruneUpToInclusive)
              prune(offsetToPruneTo, pruneAllDivulgedContracts)
                .flatMap { _ =>
                  logger.warn(s"Pruned up to $translatedOffset")
                  go(start = translatedOffset)
                }
            }
        }

      logger.warn(
        s"Pruning the Index database incrementally (in batches of $maxBatchSize) from $latestPrunedUpToInclusive to $pruneUpToInclusive"
      )
      runningF = go(start = latestPrunedUpToInclusive)
    }

    override def canBeStarted: Boolean = runningF.isCompleted

    override def abort: Future[Unit] = {
      // TODO pruning: Allow the LAPI to cancel and issue a new pruning request?
      aborted = true
      runningF // TODO pruning: add timeout
    }

    override def running: Future[Unit] = runningF
  }
}
