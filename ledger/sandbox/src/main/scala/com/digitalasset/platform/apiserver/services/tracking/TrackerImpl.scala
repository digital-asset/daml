// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy}
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.services.commands.CommandTrackerFlow.Materialized
import com.daml.logging.ThreadLogger
import com.daml.platform.server.api.ApiException
import com.daml.util.Ctx
import com.google.rpc.code.Code
import com.google.rpc.status.Status
import io.grpc.{Status => GrpcStatus}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
  * Tracks SubmitAndWaitRequests.
  * @param queue The input queue to the tracking flow.
  */
final class TrackerImpl(queue: SourceQueueWithComplete[TrackerImpl.QueueInput]) extends Tracker {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def track(request: SubmitAndWaitRequest)(
      implicit ec: ExecutionContext): Future[Completion] = {
    ThreadLogger.traceThread("Tracker.track")
    logger.trace(
      s"tracking command for party: ${request.getCommands.party}, commandId: ${request.getCommands.commandId}")
    val promise = Promise[Completion]

    submitNewRequest(request, promise)
  }

  private def submitNewRequest(request: SubmitAndWaitRequest, promise: Promise[Completion])(
      implicit ec: ExecutionContext): Future[Completion] = {

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

object TrackerImpl {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  def apply(
      tracker: Flow[
        Ctx[Promise[Completion], SubmitRequest],
        Ctx[Promise[Completion], Completion],
        Materialized[NotUsed, Promise[Completion]]],
      inputBufferSize: Int,
  )(implicit materializer: Materializer): TrackerImpl = {
    val ((queue, mat), foreachMat) = Source
      .queue[QueueInput](inputBufferSize, OverflowStrategy.dropNew)
      .map(ThreadLogger.traceStreamElement("TrackerImpl.apply (stream before tracker)"))
      .viaMat(tracker)(Keep.both)
      .map(ThreadLogger.traceStreamElement("TrackerImpl.apply (stream after tracker)"))
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

              logger.trace("Completing promise with failure: {}", status)
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
