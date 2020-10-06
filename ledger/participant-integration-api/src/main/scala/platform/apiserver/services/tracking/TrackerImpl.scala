// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Keep, Sink, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy}
import com.codahale.metrics.Counter
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.services.commands.CommandTrackerFlow.Materialized
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.InstrumentedSource
import com.daml.platform.server.api.ApiException
import com.daml.util.Ctx
import com.google.rpc.code.Code
import com.google.rpc.status.Status
import io.grpc.{Status => GrpcStatus}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
  * Tracks SubmitAndWaitRequests.
  * @param queue The input queue to the tracking flow.
  */
private[services] final class TrackerImpl(queue: SourceQueueWithComplete[TrackerImpl.QueueInput])(
    implicit loggingContext: LoggingContext,
) extends Tracker {

  import TrackerImpl.logger

  override def track(request: SubmitAndWaitRequest)(
      implicit ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Completion] = {
    logger.trace("Tracking command")
    val promise = Promise[Completion]
    submitNewRequest(request, promise)
  }

  private def submitNewRequest(request: SubmitAndWaitRequest, promise: Promise[Completion])(
      implicit ec: ExecutionContext,
  ): Future[Completion] = {
    queue
      .offer(
        Ctx(
          promise,
          SubmitRequest(
            request.commands,
            request.traceContext
          )))
      .andThen {
        HandleOfferResult.completePromise(promise)
      }
    promise.future
  }

  override def close(): Unit = {
    logger.debug("Shutting down tracking component.")
    queue.complete()
    Await.result(queue.watchCompletion(), 30.seconds)
    ()
  }
}

private[services] object TrackerImpl {

  private val logger = ContextualizedLogger.get(this.getClass)

  def apply(
      tracker: Flow[
        Ctx[Promise[Completion], SubmitRequest],
        Ctx[Promise[Completion], Completion],
        Materialized[NotUsed, Promise[Completion]]],
      inputBufferSize: Int,
      capacityCounter: Counter,
      lengthCounter: Counter,
  )(implicit materializer: Materializer, loggingContext: LoggingContext): TrackerImpl = {
    val ((queue, mat), foreachMat) = InstrumentedSource
      .queue[QueueInput](inputBufferSize, OverflowStrategy.dropNew, capacityCounter, lengthCounter)
      .viaMat(tracker)(Keep.both)
      .toMat(Sink.foreach {
        case Ctx(promise, result) =>
          result match {
            case compl @ Completion(_, Some(Status(Code.OK.value, _, _)), _, _) =>
              logger.trace("Completing promise with success")
              promise.trySuccess(compl)
            case Completion(_, statusO, _, _) =>
              val status = statusO
                .map(
                  status =>
                    GrpcStatus
                      .fromCodeValue(status.code)
                      .withDescription(status.message))
                .getOrElse(GrpcStatus.INTERNAL
                  .withDescription("Missing status in completion response."))

              logger.trace(s"Completing promise with failure: $status")
              promise.tryFailure(status.asException())
          }
          ()
      })(Keep.both)
      .run()

    foreachMat.onComplete { success =>
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
          .foreach(_.failure(new ApiException(
            GrpcStatus.INTERNAL.withDescription(promiseCancellationDescription).withCause(error))))
      )(DirectExecutionContext)
    }(DirectExecutionContext)

    new TrackerImpl(queue)
  }

  type QueueInput = Ctx[Promise[Completion], SubmitRequest]
}
