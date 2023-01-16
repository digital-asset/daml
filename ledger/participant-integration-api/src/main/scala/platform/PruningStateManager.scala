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
  @volatile private var pruningStateRef: PruningState = PruningStateAtStartup
  private implicit val ec: ExecutionContext = executionContext

  // TODO pruning: Allow the LAPI to query the pruning state/progress?
  def pruneAsync(
      latestPrunedUpToInclusive: Offset,
      upToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
  )(getOffsetAfter: (Offset, Int) => Future[Offset], prune: (Offset, Boolean) => Future[Unit])(
      implicit loggingContext: LoggingContext
  ): Future[Unit] =
    pruningStateRef.synchronized {
      if (pruningStateRef.canBeStarted) {
        val newPruningState = PruningStateInitialized()
        val ret = newPruningState.pruneInternally(
          latestPrunedUpToInclusive = latestPrunedUpToInclusive,
          upToInclusive = upToInclusive,
          pruneAllDivulgedContracts = pruneAllDivulgedContracts,
          maxBatchSize = maxBatchSize,
        )(
          getOffsetAfter = getOffsetAfter,
          prune = prune,
        )
        pruningStateRef = newPruningState
        ret
      } else {
        Future.failed(new RuntimeException("Pruning in progress. TODO log as error code"))
      }
    }

  def shutdown(): Future[Unit] =
    pruningStateRef.synchronized {
      val originalState = pruningStateRef
      pruningStateRef = PruningStateShutdown
      originalState.abort
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

    def abort: Future[Unit]
  }

  final case object PruningStateAtStartup extends PruningState {
    override val canBeStarted: Boolean = true
    override val abort: Future[Unit] = Future.unit
  }

  final case object PruningStateShutdown extends PruningState {
    override val canBeStarted: Boolean = false
    override val abort: Future[Unit] = Future.unit
  }

  final case class PruningStateInitialized() extends PruningState {
    private val logger = ContextualizedLogger.get(getClass)
    @volatile private var aborted: Boolean = false
    @volatile private var runningF: Future[Unit] = Future.unit

    def pruneInternally(
        latestPrunedUpToInclusive: Offset,
        upToInclusive: Offset,
        pruneAllDivulgedContracts: Boolean, // TODO pruning: Consider in prunedUpToInclusive
        maxBatchSize: Int,
    )(getOffsetAfter: (Offset, Int) => Future[Offset], prune: (Offset, Boolean) => Future[Unit])(
        implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Future[Unit] = {
      def go(start: Offset): Future[Unit] =
        if (aborted || start >= upToInclusive)
          Future.unit
        else {
          getOffsetAfter(start, maxBatchSize)
            .flatMap { pruneUpToInclusive =>
              prune(pruneUpToInclusive, pruneAllDivulgedContracts)
                .flatMap { _ =>
                  logger.info(s"Pruned up to $pruneUpToInclusive")
                  if (aborted) go(pruneUpToInclusive) else Future.unit
                }
            }
        }

      logger.info(
        s"Pruning the Index database incrementally (in batches of $maxBatchSize) from $latestPrunedUpToInclusive to $upToInclusive"
      )
      runningF = go(latestPrunedUpToInclusive)
      Future.unit // Checks wrt the ledger end and latest pruning offset are async
    }

    override def canBeStarted: Boolean = runningF.isCompleted

    override def abort: Future[Unit] = {
      // TODO pruning: Allow the LAPI to cancel and issue a new pruning request?
      aborted = true
      runningF // TODO pruning: add timeout
    }
  }
}
