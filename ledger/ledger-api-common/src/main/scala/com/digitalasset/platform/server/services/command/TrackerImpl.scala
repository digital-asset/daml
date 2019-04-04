// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.command

import java.util
import java.util.Collections

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy}
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.client.services.commands.CommandTrackerFlow.Materialized
import com.digitalasset.platform.server.api.ApiException
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.util.Ctx
import com.google.protobuf.empty.Empty
import com.google.rpc.status.Status
import io.grpc.{Status => GrpcStatus}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
  * Tracks SubmitAndWaitRequests.
  * @param queue The input queue to the tracking flow.
  * @param historySize The number of command IDs to remember for deduplicating tracked futures and results.
  */
class TrackerImpl(queue: SourceQueueWithComplete[TrackerImpl.QueueInput], historySize: Int)
    extends Tracker {

  private val logger = LoggerFactory.getLogger(this.getClass)

  require(historySize > 0, " History size must be a positive integer.")

  private val knownResults: util.Map[String, Future[Empty]] =
    Collections.synchronizedMap(new SizeCappedMap(historySize / 2, historySize))

  override def track(request: SubmitAndWaitRequest)(
      implicit ec: ExecutionContext): Future[Empty] = {
    logger.trace(
      s"tracking command for party: ${request.getCommands.party}, commandId: ${request.getCommands.commandId}")
    val promise = Promise[Empty]

    val storedResult =
      knownResults.putIfAbsent(request.getCommands.commandId, promise.future)
    if (storedResult == null) {
      submitNewRequest(request, promise)
    } else {
      storedResult
    }
  }

  private def submitNewRequest(request: SubmitAndWaitRequest, promise: Promise[Empty])(
      implicit ec: ExecutionContext): Future[Empty] = {

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
        Ctx[Promise[Empty], SubmitRequest],
        Ctx[Promise[Empty], Completion],
        Materialized[NotUsed, Promise[Empty]]],
      inputBufferSize: Int,
      historySize: Int)(implicit materializer: Materializer): TrackerImpl = {
    val ((queue, mat), foreachMat) = Source
      .queue[QueueInput](inputBufferSize, OverflowStrategy.dropNew)
      .viaMat(tracker)(Keep.both)
      .toMat(Sink.foreach {
        case Ctx(promise, result) =>
          result match {
            case Completion(_, Some(Status(0, _, _)), _) =>
              logger.trace("Completing promise with success")
              promise.trySuccess(Empty())
            case Completion(_, statusO, _) =>
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

    new TrackerImpl(queue, historySize)
  }

  type QueueInput = Ctx[Promise[Empty], SubmitRequest]
}
