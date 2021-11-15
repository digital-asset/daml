// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import akka.stream.scaladsl.{Flow, Keep, Sink, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import akka.{Done, NotUsed}
import com.codahale.metrics.{Counter, Timer}
import com.daml.dec.DirectExecutionContext
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.ledger.client.services.commands.CommandSubmission
import com.daml.ledger.client.services.commands.CommandTrackerFlow.Materialized
import com.daml.ledger.client.services.commands.tracker.CompletionResponse._
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.InstrumentedSource
import com.daml.platform.apiserver.services.tracking.QueueBackedTracker._
import com.daml.platform.server.api.validation.ErrorFactories
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
    queue: SourceQueueWithComplete[QueueBackedTracker.QueueInput],
    done: Future[Done],
    errorFactories: ErrorFactories,
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
      None,
    )
    logger.trace("Tracking command")
    val trackedPromise = Promise[Either[CompletionFailure, CompletionSuccess]]()
    queue
      .offer(Ctx(trackedPromise, submission))
      .flatMap[Either[TrackedCompletionFailure, CompletionSuccess]] {
        case QueueOfferResult.Enqueued =>
          trackedPromise.future.map(
            _.left.map(completionFailure => QueueCompletionFailure(completionFailure))
          )
        case QueueOfferResult.Failure(t) =>
          toQueueSubmitFailure(errorFactories.TrackerErrors.QueueSubmitFailure.failedToEnqueue(t))
        case QueueOfferResult.Dropped =>
          toQueueSubmitFailure(errorFactories.TrackerErrors.QueueSubmitFailure.ingressBufferFull())
        case QueueOfferResult.QueueClosed =>
          toQueueSubmitFailure(errorFactories.TrackerErrors.QueueSubmitFailure.queueClosed())
      }
      .recoverWith {
        case i: IllegalStateException
            if i.getMessage == "You have to wait for previous offer to be resolved to send another request" =>
          toQueueSubmitFailure(errorFactories.TrackerErrors.QueueSubmitFailure.ingressBufferFull())
        case t =>
          toQueueSubmitFailure(errorFactories.TrackerErrors.QueueSubmitFailure.failed(t))
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
    Await.result(queue.watchCompletion(), 30.seconds)
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
      errorFactories: ErrorFactories,
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
            errorFactories.trackerFailure(msg = promiseCancellationDescription)(errorLogger)
          )
        )
      })(DirectExecutionContext)

    }(DirectExecutionContext)

    new QueueBackedTracker(queue = queue, done = done, errorFactories = errorFactories)
  }

  type QueueInput = Ctx[Promise[Either[CompletionFailure, CompletionSuccess]], CommandSubmission]
}
