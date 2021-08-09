// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import akka.stream.scaladsl.{Flow, Keep, Sink, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy}
import akka.{Done, NotUsed}
import com.codahale.metrics.{Counter, Timer}
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionFailure,
  CompletionSuccess,
}
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.client.services.commands.CommandTrackerFlow.Materialized
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.InstrumentedSource
import com.daml.platform.server.api.ApiException
import com.daml.util.Ctx
import io.grpc.{Status => GrpcStatus}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** Tracks SubmitAndWaitRequests.
  * @param queue The input queue to the tracking flow.
  */
private[services] final class TrackerImpl(
    queue: SourceQueueWithComplete[TrackerImpl.QueueInput],
    done: Future[Done],
)(implicit
    loggingContext: LoggingContext
) extends Tracker {

  import TrackerImpl.logger

  override def track(request: SubmitAndWaitRequest)(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[CompletionFailure, CompletionSuccess]] = {
    logger.trace("Tracking command")
    val promise = Promise[Either[CompletionFailure, CompletionSuccess]]()
    submitNewRequest(request, promise)
  }

  private def submitNewRequest(
      request: SubmitAndWaitRequest,
      promise: Promise[Either[CompletionFailure, CompletionSuccess]],
  )(implicit
      ec: ExecutionContext
  ): Future[Either[CompletionFailure, CompletionSuccess]] = {
    queue
      .offer(
        Ctx(
          promise,
          SubmitRequest(request.commands),
        )
      )
      .andThen {
        HandleOfferResult.completePromise(promise)
      }
    promise.future
  }

  override def close(): Unit = {
    logger.debug("Shutting down tracking component.")
    queue.complete()
    Await.result(queue.watchCompletion(), 30.seconds)
    Await.result(done, 30.seconds)
    ()
  }
}

private[services] object TrackerImpl {

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
  )(implicit materializer: Materializer, loggingContext: LoggingContext): TrackerImpl = {
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

    new TrackerImpl(queue, done)
  }

  type QueueInput = Ctx[Promise[Either[CompletionFailure, CompletionSuccess]], SubmitRequest]
}
