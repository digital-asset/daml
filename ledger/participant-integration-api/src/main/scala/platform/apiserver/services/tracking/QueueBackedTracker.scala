// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import akka.stream.scaladsl.{Flow, Keep, Sink, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import akka.{Done, NotUsed}
import com.codahale.metrics.{Counter, Timer}
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.client.services.commands.CommandTrackerFlow.Materialized
import com.daml.ledger.client.services.commands.tracker.CompletionResponse._
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.InstrumentedSource
import com.daml.platform.apiserver.services.tracking.QueueBackedTracker._
import com.daml.platform.server.api.ApiException
import com.daml.util.Ctx
import io.grpc.{Status => GrpcStatus}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** Tracks SubmitAndWaitRequests.
  * @param queue The input queue to the tracking flow.
  */
private[services] final class QueueBackedTracker(
    queue: SourceQueueWithComplete[QueueBackedTracker.QueueInput],
    done: Future[Done],
)(implicit loggingContext: LoggingContext)
    extends Tracker {

  override def track(request: SubmitAndWaitRequest)(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[TrackedCompletionFailure, CompletionSuccess]] = {
    logger.trace("Tracking command")
    val trackedPromise = Promise[Either[CompletionFailure, CompletionSuccess]]()
    queue
      .offer(
        Ctx(
          trackedPromise,
          SubmitRequest(request.commands),
        )
      )
      .flatMap[Either[TrackedCompletionFailure, CompletionSuccess]] {
        case QueueOfferResult.Enqueued =>
          trackedPromise.future.map(
            _.left.map(completionFailure => QueueCompletionFailure(completionFailure))
          )
        case QueueOfferResult.Failure(t) =>
          failedQueueSubmission(
            GrpcStatus.ABORTED
              .withDescription(s"Failed to enqueue: ${t.getClass.getSimpleName}: ${t.getMessage}")
              .withCause(t)
          )
        case QueueOfferResult.Dropped =>
          failedQueueSubmission(
            GrpcStatus.RESOURCE_EXHAUSTED
              .withDescription("Ingress buffer is full")
          )
        case QueueOfferResult.QueueClosed =>
          failedQueueSubmission(GrpcStatus.ABORTED.withDescription("Queue closed"))
      }
      .recoverWith(transformQueueSubmissionExceptions)
  }

  private def failedQueueSubmission(status: GrpcStatus) =
    Future.successful(Left(QueueSubmitFailure(status)))

  private def transformQueueSubmissionExceptions
      : PartialFunction[Throwable, Future[Either[TrackedCompletionFailure, CompletionSuccess]]] = {
    case i: IllegalStateException
        if i.getMessage == "You have to wait for previous offer to be resolved to send another request" =>
      failedQueueSubmission(
        GrpcStatus.RESOURCE_EXHAUSTED
          .withDescription("Ingress buffer is full")
      )
    case t =>
      failedQueueSubmission(
        GrpcStatus.ABORTED
          .withDescription(s"Failure: ${t.getClass.getSimpleName}: ${t.getMessage}")
          .withCause(t)
      )
  }

  override def close(): Unit = {
    logger.debug("Shutting down tracking component.")
    queue.complete()
    Await.result(queue.watchCompletion(), 30.seconds)
    Await.result(done, 30.seconds)
    ()
  }
}

private[services] object QueueBackedTracker {

  private val logger = ContextualizedLogger.get(this.getClass)

  def apply(
      tracker: Flow[
        Ctx[Promise[Either[CompletionFailure, CompletionSuccess]], SubmitRequest],
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
    val ((queue, mat), done) = InstrumentedSource
      .queue[QueueInput](
        inputBufferSize,
        OverflowStrategy.dropNew,
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
      val (promiseCancellationDescription, error) = success match {
        case Success(_) => "Unknown" -> null // in this case, there should be no promises cancelled
        case Failure(t: Exception) =>
          logger.error("Error in tracker", t)
          "Cancelled due to previous internal error" -> t
        case Failure(t: Throwable) =>
          logger.error("Error in tracker", t) -> t
          throw t // FIXME(mthvedt): we should shut down the server on internal errors.
      }

      mat.trackingMat.onComplete(
        // no error expected here -- if there is one, we're at a total loss.
        // FIXME(mthvedt): we should shut down everything in this case.
        _.get.values
          .foreach(
            _.failure(
              new ApiException(
                GrpcStatus.INTERNAL.withDescription(promiseCancellationDescription).withCause(error)
              )
            )
          )
      )(DirectExecutionContext)
    }(DirectExecutionContext)

    new QueueBackedTracker(queue, done)
  }

  type QueueInput = Ctx[Promise[Either[CompletionFailure, CompletionSuccess]], SubmitRequest]
}
