// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.{BoundedSourceQueue, Materializer, QueueOfferResult}
import akka.{Done, NotUsed}
import com.daml.metrics.api.MetricHandle.{Counter, Timer}
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.ledger.client.services.commands.CommandSubmission
import com.daml.ledger.client.services.commands.CommandTrackerFlow.Materialized
import com.daml.ledger.client.services.commands.tracker.CompletionResponse._
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.InstrumentedGraph
import com.daml.platform.apiserver.services.tracking.QueueBackedTracker._
import com.daml.ledger.errors.LedgerApiErrors
import com.daml.util.Ctx
import com.google.rpc.Status

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/** Tracks SubmitAndWaitRequests.
  *
  * @param queue The input queue to the tracking flow.
  */
private[services] final class QueueBackedTracker(
    queue: BoundedSourceQueue[QueueBackedTracker.QueueInput],
    done: Future[Done],
)(implicit loggingContext: LoggingContext)
    extends Tracker {

  override def track(
      submission: CommandSubmission
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[TrackedCompletionFailure, CompletionSuccess]] = {
    implicit val errorLogger: DamlContextualizedErrorLogger = new DamlContextualizedErrorLogger(
      logger,
      loggingContext,
      Some(submission.commands.submissionId),
    )
    logger.trace("Tracking command")
    val trackedPromise = Promise[Either[CompletionFailure, CompletionSuccess]]()
    Try(queue.offer(Ctx(trackedPromise, submission))) match {
      case Success(QueueOfferResult.Enqueued) =>
        trackedPromise.future.map(
          _.left.map(completionFailure => QueueCompletionFailure(completionFailure))
        )
      case Success(QueueOfferResult.Failure(throwable)) =>
        toQueueSubmitFailure(
          LedgerApiErrors.InternalError
            .Generic(
              s"Failed to enqueue: ${throwable.getClass.getSimpleName}: ${throwable.getMessage}",
              Some(throwable),
            )
            .asGrpcStatus
        )
      case Success(QueueOfferResult.Dropped) =>
        toQueueSubmitFailure(
          LedgerApiErrors.ParticipantBackpressure
            .Rejection("The submission ingress buffer is full")
            .asGrpcStatus
        )
      case Success(QueueOfferResult.QueueClosed) =>
        toQueueSubmitFailure(
          LedgerApiErrors.ServiceNotRunning
            .Reject("Command service queue")
            .asGrpcStatus
        )
      case Failure(throwable) =>
        toQueueSubmitFailure(
          LedgerApiErrors.InternalError
            .Generic(
              s"Unexpected `BoundedSourceQueue.offer` exception: ${throwable.getClass.getSimpleName}: ${throwable.getMessage}",
              Some(throwable),
            )
            .asGrpcStatus
        )
    }
  }

  private def toQueueSubmitFailure(
      e: Status
  ): Future[Left[QueueSubmitFailure, Nothing]] = {
    Future.successful(Left(QueueSubmitFailure(e)))
  }

  override def close(): Unit = {
    logger.debug("Shutting down tracking component.")
    queue.complete()
    Await.result(done, 30.seconds)
    ()
  }
}

private[services] object QueueBackedTracker {
  private val logger = ContextualizedLogger.get(this.getClass)

  def apply(
      tracker: Flow[
        Ctx[Promise[Either[CompletionFailure, CompletionSuccess]], CommandSubmission],
        Ctx[
          Promise[Either[CompletionFailure, CompletionSuccess]],
          Either[CompletionFailure, CompletionSuccess],
        ],
        Materialized[NotUsed, Promise[Either[CompletionFailure, CompletionSuccess]]],
      ],
      inputBufferSize: Int,
      capacityCounter: Counter,
      lengthCounter: Counter,
      delayTimer: Timer,
  )(implicit materializer: Materializer, loggingContext: LoggingContext): QueueBackedTracker = {
    val ((queue, mat), done) = InstrumentedGraph
      .queue[QueueInput](
        inputBufferSize,
        capacityCounter,
        lengthCounter,
        delayTimer,
      )
      .viaMat(tracker)(Keep.both)
      .toMat(Sink.foreach { case Ctx(promise, result, _) =>
        val didCompletePromise = promise.trySuccess(result)
        if (!didCompletePromise) {
          logger.trace(
            "Promise was already completed, could not propagate the completion for the command."
          )
        } else {
          logger.trace("Completed promise with the result of command.")
        }
        ()
      })(Keep.both)
      .run()

    done.onComplete { success =>
      val (promiseCancellationDescription, _) = success match {
        case Success(_) => "Unknown" -> null // in this case, there should be no promises cancelled
        case Failure(t: Exception) =>
          logger.error("Error in tracker", t)
          "Cancelled due to previous internal error" -> t
        case Failure(t: Throwable) =>
          logger.error("Error in tracker", t) -> t
          throw t // FIXME(mthvedt): we should shut down the server on internal errors.
      }

      mat.trackingMat.onComplete((aTry: Try[Map[_, Promise[_]]]) => {
        // no error expected here -- if there is one, we're at a total loss.
        // FIXME(mthvedt): we should shut down everything in this case.
        val promises: Iterable[Promise[_]] = aTry.get.values
        val errorLogger = new DamlContextualizedErrorLogger(logger, loggingContext, None)
        promises.foreach(p =>
          p.failure(
            LedgerApiErrors.InternalError
              .Generic(promiseCancellationDescription)(errorLogger)
              .asGrpcError
          )
        )
      })(ExecutionContext.parasitic)
    }(ExecutionContext.parasitic)

    new QueueBackedTracker(queue, done)
  }

  type QueueInput = Ctx[Promise[Either[CompletionFailure, CompletionSuccess]], CommandSubmission]
}
