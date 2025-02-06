// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import cats.arrow.FunctionK
import cats.data.EitherT
import com.daml.timer.RetryStrategy
import com.daml.timer.RetryStrategy.UnhandledFailureException
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.Update.CommitRepair
import com.digitalasset.canton.ledger.participant.state.{
  ParticipantUpdate,
  RepairUpdate,
  SynchronizerUpdate,
  Update,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.util.PekkoUtil.{FutureQueue, RecoveringFutureQueue}
import com.digitalasset.canton.util.Thereafter.syntax.*
import org.apache.pekko.Done

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class IndexerState(
    recoveringIndexerFactory: () => RecoveringFutureQueue[Update],
    repairIndexerFactory: () => Future[FutureQueue[Update]],
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {
  import IndexerState.*

  private var state: State = Normal(recoveringIndexerFactory(), shutdownInitiated = false)
  private implicit val traceContext: TraceContext = TraceContext.empty

  // Requesting a Repair Indexer turns off normal indexing, therefore it needs to be ensured, before calling:
  //   - no synchronizers are connected
  //   - no party/package additions are happening
  // The life-cycle of the indexer with regards to Repair mode is as follows:
  //   1 - normal indexing is ongoing
  //   2 - repair indexing is requested
  //   3 - normal indexing waits for an empty indexing queue (no activity)
  //   4 - normal indexing stops
  //   5 - repair indexing is initiating
  //   6 - repair indexing is ready to be used (here provided repairOperation starts executing)
  //   7 - repair indexing is used: FutureQueue offer operations
  //   8 - repair usage finished (block finished executing)
  //   9 - repair is committing: the Ledger End will be persisted
  //  10 - repair indexing stops
  //  11 - normal indexing resumes operation (starts the recovering initialization loop)
  //  12 - normal indexer initialized the first time (important to ensure LAPI in memory state is intact by the time the repair command finishes)
  //  13 - the resulting Future finishes
  // The client needs to ensure that the provided repair indexer is not used after the resulting Future terminates, also: CommitRepair should not be used directly.
  def withRepairIndexer(
      repairOperation: FutureQueue[RepairUpdate] => EitherT[Future, String, Unit]
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = EitherT(
    withStateUnlessShutdown {
      case Repair(_, repairDone, _) =>
        Future.failed(new RepairInProgress(repairDone))

      case Normal(queue, _) =>
        logger.info("Switched to Repair Mode, waiting for inactive indexing...")
        val repairIndexerF = for {
          _ <- waitForEmptyIndexerQueue(queue)
          _ = logger.info("Shutting down Indexer...")
          _ = queue.shutdown()
          _ <- queue.done
          _ = logger.info("Initializing Repair Indexer...")
          repairIndexer <- withStateUnlessShutdown(_ => repairIndexerFactory())
          _ = logger.info("Repair Indexer ready")
        } yield new RepairQueueProxy(repairIndexer, () => onRepairFinished(), loggerFactory)
        val result = repairIndexerF.transformWith {
          case Failure(t) =>
            logger.info("Repair Indexer initialization failed, resuming normal indexing...", t)
            onRepairFinished().transform(_ => Failure(t))

          case Success(repairIndexer) =>
            logger.info("Repair Indexer initialized, executing repair operation...")
            executeRepairOperation(repairIndexer, repairOperation)
        }
        state = Repair(
          repairIndexerF,
          result.transform(_ => Success(())),
          shutdownInitiated = false,
        )
        result
    }
  )

  private def waitForEmptyIndexerQueue(queue: RecoveringFutureQueue[Update]): Future[Unit] =
    RetryStrategy
      .constant(Some(100), Duration.create(100, "millis")) { case t: Throwable =>
        t.getMessage.contains("Still indexing")
      }((_, _) =>
        withStateUnlessShutdown(_ =>
          if (queue.uncommittedQueueSnapshot.nonEmpty)
            Future.failed(new Exception(s"Still indexing"))
          else
            Future.unit
        )
      )
      .recoverWith { case UnhandledFailureException(_, _, ShutdownInProgress) =>
        Future.failed(ShutdownInProgress)
      }
      .recoverWith { err =>
        // shutting down indexer anyway, as the most probably cause here is that we are shutting down
        logger.info("Shutting down Indexer after waiting for empty indexer queue failed...")
        queue.shutdown()
        queue.done.transform(_ => Failure(err))
      }

  private def onRepairFinished(): Future[Unit] = withStateUnlessShutdown {
    case Normal(normalIndexer, _) =>
      logger.error(
        "Illegal state transition: before finished Repair Indexer, normal Indexer operation resumed"
      )
      normalIndexer.firstSuccessfulConsumerInitialization

    case Repair(_, _, _) =>
      logger.info("Switched to Normal Mode")
      val normalIndexer = recoveringIndexerFactory()
      state = Normal(normalIndexer, shutdownInitiated = false)
      normalIndexer.firstSuccessfulConsumerInitialization.thereafter {
        case Success(_) =>
          logger.info("Normal indexing successfully initialized")

        case Failure(t) =>
          logger.warn("Normal indexing failed to initialize successfully", t)
      }
  }

  private def executeRepairOperation(
      repairIndexer: RepairQueueProxy,
      repairOperation: PekkoUtil.FutureQueue[RepairUpdate] => EitherT[Future, String, Unit],
  ): Future[Either[String, Unit]] = withStateUnlessShutdown { _ =>
    def waitForRepairIndexerToTerminateAndThenReturnUnlessShutdown[T](result: Try[T]): Future[T] =
      withStateUnlessShutdown(_ => repairIndexer.done.transform(_ => result))

    def waitForRepairIndexerToTerminateUnlessShutdownAndThenReturn[T](result: Try[T]): Future[T] =
      withStateUnlessShutdown(_ => repairIndexer.done).transform(_ => result)

    def commitRepair(): Future[Right[Nothing, Unit]] = withStateUnlessShutdown(_ =>
      repairIndexer.commit().transformWith {
        case Failure(t) =>
          logger.warn(s"Committing repair changes failed, resuming normal indexing...", t)
          repairIndexer.shutdown()
          waitForRepairIndexerToTerminateUnlessShutdownAndThenReturn(
            Failure(new Exception("Committing repair changes failed", t))
          )

        case Success(_) =>
          logger.info(s"Committing repair changes succeeded, resuming normal indexing...")
          waitForRepairIndexerToTerminateUnlessShutdownAndThenReturn(Success(Right(())))
      }
    )

    Future.delegate(repairOperation(repairIndexer).value).transformWith {
      case Failure(t) =>
        logger.info("Repair operation failed with exception, resuming normal indexing...", t)
        repairIndexer.shutdown()
        waitForRepairIndexerToTerminateAndThenReturnUnlessShutdown(Failure(t))

      case Success(Left(failure)) =>
        logger.info(s"Repair operation failed with error ($failure), resuming normal indexing...")
        repairIndexer.shutdown()
        waitForRepairIndexerToTerminateAndThenReturnUnlessShutdown(Success(Left(failure)))

      case Success(Right(_)) =>
        logger.info(s"Repair operation succeeded, committing changes...")
        commitRepair()
    }
  }

  // Mapping all results to a clean shutdown, to allow further shutdown-steps to complete normally.
  private def handleShutdownDoneResult(doneResult: Try[Done]): Success[Unit] = {
    doneResult match {
      case Success(Done) =>
        logger.info("IndexerState stopped successfully")

      case Failure(t) =>
        // Logging at info level since either Repair-Index or Recovering-Indexer should emit warnings in case of shutdown related problems.
        logger.info("IndexerState stopped with a failure", t)
    }
    Success(())
  }

  def shutdown(): Future[Unit] = withState {
    case Normal(queue, shutdownInitiated) =>
      if (!shutdownInitiated) {
        queue.shutdown()
        state = Normal(
          queue,
          shutdownInitiated = true,
        )
      }
      queue.done.transform { doneResult =>
        queue.uncommittedQueueSnapshot
          .collect { case (_, participantUpdate: ParticipantUpdate) =>
            participantUpdate
          }
          .foreach(
            _.persisted
              .tryFailure(
                new IllegalStateException(
                  "Indexer is shutting down, this Update won't be persisted."
                )
              )
              .discard
          )
        handleShutdownDoneResult(doneResult)
      }

    case Repair(queueF, repairDone, shutdownInitiated) =>
      if (!shutdownInitiated) {
        queueF.onComplete(_.foreach(_.shutdown()))
        state = Repair(
          queueF,
          repairDone,
          shutdownInitiated = true,
        )
      }
      queueF.flatMap(_.done).transform(handleShutdownDoneResult)
  }

  def ensureNoProcessingForSynchronizer(synchronizerId: SynchronizerId): Future[Unit] =
    withStateUnlessShutdown {
      case Normal(recoveringQueue, _) =>
        RetryStrategy
          .constant(None, Duration.create(200, "millis")) { case t: Throwable =>
            t.getMessage.contains("Still uncommitted")
          }((_, _) =>
            withStateUnlessShutdown(_ =>
              if (
                recoveringQueue.uncommittedQueueSnapshot.iterator.map(_._2).exists {
                  case u: SynchronizerUpdate => u.synchronizerId == synchronizerId
                  case _: Update.CommitRepair => false
                  case _: Update.PartyAddedToParticipant => false
                }
              )
                Future.failed(
                  new Exception(
                    s"Still uncommitted activity for synchronizer $synchronizerId, waiting..."
                  )
                )
              else
                Future.unit
            )
          )
          .recoverWith { case UnhandledFailureException(_, _, ShutdownInProgress) =>
            Future.failed(ShutdownInProgress)
          }

      case Repair(_, repairDone, _) =>
        Future.failed(new RepairInProgress(repairDone))
    }

  private def withState[T](f: State => T): T =
    blocking(synchronized(f(state)))

  def withStateUnlessShutdown[T](f: State => Future[T]): Future[T] =
    withState(s =>
      if (s.shutdownInitiated) Future.failed(ShutdownInProgress)
      else f(s)
    )
}

object IndexerQueueProxy {
  import IndexerState.*

  def apply(
      withIndexerState: (IndexerState.State => Future[Unit]) => Future[Unit]
  )(implicit executionContext: ExecutionContext): Update => Future[Unit] = elem =>
    withIndexerState {
      case Normal(queue, _) =>
        elem match {
          case commitRepair: CommitRepair =>
            val failure = new IllegalStateException("CommitRepair should not be used")
            commitRepair.persisted.tryFailure(failure).discard
            Future.failed(failure)

          case _ => queue.offer(elem).map(_ => ())
        }

      case Repair(_, repairDone, _) =>
        Future.failed(new RepairInProgress(repairDone))
    }
}

class RepairQueueProxy(
    repairQueue: FutureQueue[Update],
    onRepairFinished: () => Future[Unit],
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends FutureQueue[RepairUpdate]
    with NamedLogging {
  private implicit val traceContext: TraceContext = TraceContext.empty

  override def offer(elem: RepairUpdate): Future[Done] =
    repairQueue.offer(elem)

  override def shutdown(): Unit = repairQueue.shutdown()

  override def done: Future[Done] =
    repairQueue.done.transformWith { repairDoneResult =>
      repairDoneResult match {
        case Failure(t) => logger.warn("Repair Indexer finished with error", t)
        case Success(_) => logger.info("Repair Indexer finished successfully")
      }
      onRepairFinished().transform(_ => repairDoneResult)
    }

  def commit(): Future[Unit] = {
    val commitRepair = CommitRepair()
    repairQueue
      .offer(commitRepair)
      .flatMap(_ => commitRepair.persisted.future)
  }
}

object IndexerState {
  sealed trait State {
    def shutdownInitiated: Boolean
  }

  final case class Normal(queue: RecoveringFutureQueue[Update], shutdownInitiated: Boolean)
      extends State

  final case class Repair(
      queue: Future[FutureQueue[RepairUpdate]],
      repairDone: Future[Unit],
      shutdownInitiated: Boolean,
  ) extends State

  // repairDone should never fail, and only complete if normal indexing is resumed
  class RepairInProgress(val repairDone: Future[Unit])
      extends RuntimeException("Repair in progress")

  object ShutdownInProgress extends RuntimeException("Shutdown in progress") with NoStackTrace {
    def transformToFUS[T](
        f: Future[T]
    )(implicit executionContext: ExecutionContext): FutureUnlessShutdown[T] =
      FutureUnlessShutdown
        .outcomeF(f)
        .recover { case ShutdownInProgress =>
          AbortedDueToShutdown
        }

    def functionK(implicit
        executionContext: ExecutionContext
    ): FunctionK[Future, FutureUnlessShutdown] =
      new FunctionK[Future, FutureUnlessShutdown] {
        override def apply[T](f: Future[T]): FutureUnlessShutdown[T] =
          transformToFUS(f)
      }

  }

}
