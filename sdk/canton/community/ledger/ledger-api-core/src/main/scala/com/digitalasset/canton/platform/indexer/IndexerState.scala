// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import cats.data.EitherT
import com.daml.timer.RetryStrategy
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.ledger.participant.state.Update.CommitRepair
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.util.PekkoUtil.{FutureQueue, RecoveringFutureQueue}
import org.apache.pekko.Done

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.{Failure, Success, Try}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class IndexerState(
    recoveringIndexerFactory: () => RecoveringFutureQueue[Traced[Update]],
    repairIndexerFactory: () => Future[FutureQueue[Traced[Update]]],
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {
  import IndexerState.*

  private var state: State = Normal(recoveringIndexerFactory(), shutdownInitiated = false)
  private implicit val traceContext: TraceContext = TraceContext.empty

  // Requesting a Repair Indexer turns off normal indexing, therefore it needs to be ensured, before calling:
  //   - no domains are connected
  //   - no party/package additions are happening
  // The life-cycle of the indexer with regards to Repair mode is as follows:
  //   1 - normal indexing is ongoing
  //   2 - repair indexing is requested
  //   3 - normal indexing waits for an empty indexing queue (no activity)
  //   4 - normal indexing stops
  //   5 - repair indexing is initiating
  //   6 - repair indexing is ready to be used (here provided repairOperation starts executing)
  //   7 - repair indexing is used: FutureQueue offer operations
  //   8 - repair usage finished (block finished execiting)
  //   9 - repair is committing: the Ledger End will be persisted
  //  10 - repair indexing stops
  //  11 - normal indexing resumes operation (starts the recovering initialization loop)
  //  12 - normal indexer initialized the first time (important to ensure LAPI in memory state is intact by the time the repair command finishes)
  //  13 - the resulting Future finishes
  // The client needs to ensure that the provided repair indexer is not used after the resulting Future terminates, also: CommitRepair should not be used directly.
  def withRepairIndexer(
      repairOperation: FutureQueue[Traced[Update]] => EitherT[Future, String, Unit]
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
          repairIndexer <- repairIndexerFactory()
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

  private def waitForEmptyIndexerQueue(queue: RecoveringFutureQueue[Traced[Update]]): Future[Unit] =
    RetryStrategy.constant(Some(100), Duration.create(100, "millis")) { case t: Throwable =>
      t.getMessage.contains("Still indexing")
    } { (_, _) =>
      if (queue.uncommittedQueueSnapshot.nonEmpty)
        Future.failed(new Exception(s"Still indexing"))
      else
        Future.unit
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
      normalIndexer.firstSuccessfulConsumerInitialization.andThen {
        case Success(_) =>
          logger.info("Normal indexing successfully initialized")

        case Failure(t) =>
          logger.warn("Normal indexing failed to initialize successfully", t)
      }
  }

  private def executeRepairOperation(
      repairIndexer: RepairQueueProxy,
      repairOperation: PekkoUtil.FutureQueue[Traced[Update]] => EitherT[Future, String, Unit],
  ): Future[Either[String, Unit]] = {
    def waitForRepairIndexerToTerminateAndThenReturn[T](result: Try[T]): Future[T] =
      repairIndexer.done.transform(_ => result)

    def commitRepair(): Future[Right[Nothing, Unit]] =
      repairIndexer.commit().transformWith {
        case Failure(t) =>
          logger.warn(s"Committing repair changes failed, resuming normal indexing...", t)
          repairIndexer.shutdown()
          waitForRepairIndexerToTerminateAndThenReturn(
            Failure(new Exception("Committing repair changes failed", t))
          )

        case Success(_) =>
          logger.info(s"Committing repair changes succeeded, resuming normal indexing...")
          waitForRepairIndexerToTerminateAndThenReturn(Success(Right(())))
      }

    repairOperation(repairIndexer).value.transformWith {
      case Failure(t) =>
        logger.info("Repair operation failed with exception, resuming normal indexing...", t)
        repairIndexer.shutdown()
        waitForRepairIndexerToTerminateAndThenReturn(Failure(t))

      case Success(Left(failure)) =>
        logger.info(s"Repair operation failed with error ($failure), resuming normal indexing...")
        repairIndexer.shutdown()
        waitForRepairIndexerToTerminateAndThenReturn(Success(Left(failure)))

      case Success(Right(_)) =>
        logger.info(s"Repair operation succeeded, committing changes...")
        commitRepair()
    }
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
      queue.done.map(_ =>
        queue.uncommittedQueueSnapshot.foreach(
          _._2.value.persisted
            .tryFailure(
              new IllegalStateException(
                "Indexer is shutting down, this Update won't be persisted."
              )
            )
            .discard
        )
      )

    case Repair(queueF, repairDone, shutdownInitiated) =>
      if (!shutdownInitiated) {
        queueF.onComplete(_.foreach(_.shutdown()))
        state = Repair(
          queueF,
          repairDone,
          shutdownInitiated = true,
        )
      }
      queueF.flatMap(_.done).map(_ => ())
  }

  def ensureNoProcessingForDomain(domainId: DomainId): Future[Unit] = withStateUnlessShutdown {
    case Normal(recoveringQueue, _) =>
      RetryStrategy.constant(None, Duration.create(200, "millis")) { case t: Throwable =>
        t.getMessage.contains("Still uncommitted")
      } { (_, _) =>
        if (
          recoveringQueue.uncommittedQueueSnapshot.iterator.map(_._2.value).exists {
            case u: Update.TransactionAccepted => u.domainId == domainId
            case u: Update.ReassignmentAccepted => u.domainId == domainId
            case u: Update.CommandRejected => u.domainId == domainId
            case u: Update.SequencerIndexMoved => u.domainId == domainId
            case _ => false
          }
        )
          Future.failed(
            new Exception(s"Still uncommitted activity for domain $domainId, waiting...")
          )
        else
          Future.unit
      }

    case Repair(_, repairDone, _) =>
      Future.failed(new RepairInProgress(repairDone))
  }

  private def withState[T](f: State => T): T =
    blocking(synchronized(f(state)))

  def withStateUnlessShutdown[T](f: State => Future[T]): Future[T] =
    withState(s =>
      if (s.shutdownInitiated) Future.failed(new IllegalStateException("Shutdown in progress"))
      else f(s)
    )
}

class IndexerQueueProxy(
    withIndexerStateUnlessShutdown: (IndexerState.State => Future[Done]) => Future[Done]
) extends FutureQueue[Traced[Update]] {
  import IndexerState.*

  override def offer(elem: Traced[Update]): Future[Done] = withIndexerStateUnlessShutdown {
    case Normal(queue, _) =>
      elem.value match {
        case _: CommitRepair =>
          val failure = new IllegalStateException("CommitRepair should not be used")
          elem.value.persisted.tryFailure(failure).discard
          Future.failed(failure)

        case _ => queue.offer(elem)
      }

    case Repair(_, repairDone, _) =>
      Future.failed(new RepairInProgress(repairDone))
  }

  override def shutdown(): Unit = throw new UnsupportedOperationException()

  override def done: Future[Done] = throw new UnsupportedOperationException()
}

class RepairQueueProxy(
    repairQueue: FutureQueue[Traced[Update]],
    onRepairFinished: () => Future[Unit],
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends FutureQueue[Traced[Update]]
    with NamedLogging {
  private implicit val traceContext: TraceContext = TraceContext.empty

  override def offer(elem: Traced[Update]): Future[Done] =
    elem.value match {
      case _: CommitRepair =>
        Future.failed(new IllegalStateException("CommitRepair should not be used"))
      case _ => repairQueue.offer(elem)
    }

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
    val commitRepair = Traced(CommitRepair())
    repairQueue
      .offer(commitRepair)
      .flatMap(_ => commitRepair.value.persisted.future)
  }
}

object IndexerState {
  sealed trait State {
    def shutdownInitiated: Boolean
  }

  final case class Normal(queue: RecoveringFutureQueue[Traced[Update]], shutdownInitiated: Boolean)
      extends State

  final case class Repair(
      queue: Future[FutureQueue[Traced[Update]]],
      repairDone: Future[Unit],
      shutdownInitiated: Boolean,
  ) extends State

  // repairDone should never fail, and only complete if normal indexing is resumed
  class RepairInProgress(val repairDone: Future[Unit])
      extends RuntimeException("Repair in progress")
}
