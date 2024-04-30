// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.tracking

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.resources.ResourceOwner
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.error.{CommonErrors, LedgerApiErrors}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker.{
  SubmissionKey,
  Submitters,
}
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import io.opentelemetry.api.trace.Tracer

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

trait SubmissionTracker extends AutoCloseable {
  def track(
      submissionKey: SubmissionKey,
      timeout: NonNegativeFiniteDuration,
      submit: TraceContext => Future[Any],
  )(implicit
      errorLogger: ContextualizedErrorLogger,
      traceContext: TraceContext,
  ): Future[CompletionResponse]

  /** [[com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse.completion]] do not have `act_as` populated,
    * hence submitters are propagated separately.
    * TODO(#12658): Use only the completion response once completions.act_as is populated.
    */
  def onCompletion(completionResult: (CompletionStreamResponse, Submitters)): Unit
}

object SubmissionTracker {
  type Submitters = Set[String]

  def owner(
      maxCommandsInFlight: Int,
      metrics: Metrics,
      tracer: Tracer,
      loggerFactory: NamedLoggerFactory,
  ): ResourceOwner[SubmissionTracker] =
    for {
      cancellableTimeoutSupport <- CancellableTimeoutSupport.owner(
        "submission-tracker-timeout-timer",
        loggerFactory,
      )
      tracker <- ResourceOwner.forCloseable(() =>
        new SubmissionTrackerImpl(
          cancellableTimeoutSupport,
          maxCommandsInFlight,
          metrics,
          loggerFactory,
        )(tracer)
      )
    } yield tracker

  private[tracking] class SubmissionTrackerImpl(
      cancellableTimeoutSupport: CancellableTimeoutSupport,
      maxCommandsInFlight: Int,
      metrics: Metrics,
      val loggerFactory: NamedLoggerFactory,
  )(implicit val tracer: Tracer)
      extends SubmissionTracker
      with Spanning
      with NamedLogging {

    private val directEc = DirectExecutionContext(noTracingLogger)

    private[tracking] val pending =
      TrieMap.empty[SubmissionKey, (ContextualizedErrorLogger, Promise[CompletionResponse])]

    // Set max-in-flight capacity
    metrics.commands.maxInFlightCapacity.inc(maxCommandsInFlight.toLong)(MetricsContext.Empty)

    override def track(
        submissionKey: SubmissionKey,
        timeout: NonNegativeFiniteDuration,
        submit: TraceContext => Future[Any],
    )(implicit
        errorLogger: ContextualizedErrorLogger,
        traceContext: TraceContext,
    ): Future[CompletionResponse] =
      ensuringSubmissionIdPopulated(submissionKey) {
        ensuringMaximumInFlight {
          val promise = Promise[CompletionResponse]()
          pending.putIfAbsent(submissionKey, (errorLogger, promise)) match {
            case Some(_) =>
              promise.complete(
                CompletionResponse.duplicate(submissionKey.submissionId)(errorLogger)
              )

            case None =>
              trackWithCancelTimeout(submissionKey, timeout, promise, submit)(
                errorLogger,
                traceContext,
              )
          }
          promise.future
        }(errorLogger)
      }(errorLogger)

    override def onCompletion(completionResult: (CompletionStreamResponse, Submitters)): Unit = {
      val (completionStreamResponse, submitters) = completionResult
      completionStreamResponse.completion.foreach { completion =>
        attemptFinish(SubmissionKey.fromCompletion(completion, submitters))(
          CompletionResponse.fromCompletion(_, completion, completionStreamResponse.checkpoint)
        )
      }
    }

    override def close(): Unit =
      pending.values.foreach(p => p._2.tryComplete(CompletionResponse.closing(p._1)).discard)

    private def attemptFinish(submissionKey: SubmissionKey)(
        result: ContextualizedErrorLogger => Try[CompletionResponse]
    ): Unit =
      pending.get(submissionKey).foreach(p => p._2.tryComplete(result(p._1)).discard)

    private def trackWithCancelTimeout(
        submissionKey: SubmissionKey,
        timeout: config.NonNegativeFiniteDuration,
        promise: Promise[CompletionResponse],
        submit: TraceContext => Future[Any],
    )(implicit
        errorLogger: ContextualizedErrorLogger,
        traceContext: TraceContext,
    ): Unit =
      Try(
        // Start the timeout timer before submit to ensure that the timer scheduling
        // happens before its cancellation (on submission failure OR onCompletion)
        cancellableTimeoutSupport.scheduleOnce(
          duration = timeout,
          promise = promise,
          onTimeout =
            CompletionResponse.timeout(submissionKey.commandId, submissionKey.submissionId)(
              errorLogger
            ),
        )(traceContext)
      ) match {
        case Failure(err) =>
          logger.error(
            "An internal error occurred while trying to register the cancellation timeout. Aborting submission..",
            err,
          )
          pending.remove(submissionKey).discard
          promise.tryFailure(err).discard
        case Success(cancelTimeout) =>
          withSpan("SubmissionTracker.track") { childContext => _ =>
            submit(childContext)
              .onComplete {
                case Success(_) => // succeeded, nothing to do
                case Failure(throwable) =>
                  // Submitting command failed, finishing entry with the very same error
                  promise.tryComplete(Failure(throwable))
              }(directEc)
          }

          promise.future.onComplete { _ =>
            // register timeout cancellation and removal from map
            withSpan("SubmissionTracker.complete") { _ => _ =>
              cancelTimeout.close()
              pending.remove(submissionKey)
            }(traceContext, tracer)
          }(directEc)
      }

    private def ensuringMaximumInFlight[T](
        f: => Future[T]
    )(implicit errorLogger: ContextualizedErrorLogger): Future[T] =
      if (pending.size < maxCommandsInFlight) {
        metrics.commands.maxInFlightLength.inc()
        val ret = f
        ret.onComplete { _ =>
          metrics.commands.maxInFlightLength.dec()
        }(directEc)
        ret
      } else {
        Future.failed(
          LedgerApiErrors.ParticipantBackpressure
            .Rejection("Maximum number of commands in-flight reached")(errorLogger)
            .asGrpcError
        )
      }

    private def ensuringSubmissionIdPopulated[T](submissionKey: SubmissionKey)(f: => Future[T])(
        implicit errorLogger: ContextualizedErrorLogger
    ): Future[T] =
      // We need submissionId for tracking submissions
      if (submissionKey.submissionId.isEmpty) {
        Future.failed(
          CommonErrors.ServiceInternalError
            .Generic("Missing submission id in submission tracker")(errorLogger)
            .asGrpcError
        )
      } else {
        f
      }
  }

  final case class SubmissionKey(
      commandId: String,
      submissionId: String,
      applicationId: String,
      parties: Set[String],
  )

  object SubmissionKey {
    def fromCompletion(
        completion: com.daml.ledger.api.v2.completion.Completion,
        submitters: Submitters,
    ): SubmissionKey =
      SubmissionKey(
        commandId = completion.commandId,
        submissionId = completion.submissionId,
        applicationId = completion.applicationId,
        parties = submitters,
      )
  }
}
