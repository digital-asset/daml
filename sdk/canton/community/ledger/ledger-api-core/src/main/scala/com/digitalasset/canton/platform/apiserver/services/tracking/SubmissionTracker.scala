// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.tracking

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.resources.ResourceOwner
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker.SubmissionKey
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}
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

  def onCompletion(completionStreamResponse: CompletionStreamResponse): Unit
}

object SubmissionTracker {
  type Submitters = Set[String]

  def owner(
      maxCommandsInFlight: Int,
      metrics: LedgerApiServerMetrics,
      tracer: Tracer,
      loggerFactory: NamedLoggerFactory,
  ): ResourceOwner[SubmissionTracker] =
    for {
      streamTracker <- StreamTracker.owner(
        trackerThreadName = "submission-tracker",
        itemKey = SubmissionKey.fromCompletion,
        maxInFlight = maxCommandsInFlight,
        inFlightCount = metrics.commands.maxInFlightLength,
        loggerFactory = loggerFactory,
      )
      tracker <- ResourceOwner.forCloseable(() =>
        new SubmissionTrackerImpl(
          streamTracker,
          maxCommandsInFlight,
          metrics,
          loggerFactory,
        )(tracer)
      )
    } yield tracker

  private[tracking] class SubmissionTrackerImpl(
      streamTracker: StreamTracker[SubmissionKey, Completion],
      maxCommandsInFlight: Int,
      metrics: LedgerApiServerMetrics,
      val loggerFactory: NamedLoggerFactory,
  )(implicit val tracer: Tracer)
      extends SubmissionTracker
      with Spanning
      with NamedLogging {

    implicit val directEc: ExecutionContext = DirectExecutionContext(noTracingLogger)

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
        streamTracker
          .track(submissionKey, timeout, submit)
          .flatMap(c => Future.fromTry(Result.fromCompletion(errorLogger, c)))
      }

    override def onCompletion(completionStreamResponse: CompletionStreamResponse): Unit =
      completionStreamResponse.completionResponse.completion.foreach { completion =>
        streamTracker.onStreamItem(completion)
      }

    override def close(): Unit =
      streamTracker.close()

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
  ) extends Errors.KeyDescriptions {
    def requestDescription =
      s"a command submission with command-id=$commandId and submission-id=$submissionId"
    def streamItemDescription = "a completion"
    def requestId = submissionId
  }

  object SubmissionKey {
    def fromCompletion(completion: Completion): SubmissionKey =
      SubmissionKey(
        commandId = completion.commandId,
        submissionId = completion.submissionId,
        applicationId = completion.applicationId,
        parties = completion.actAs.toSet,
      )
  }

  object Result {
    import com.google.rpc.status
    import io.grpc.StatusRuntimeException
    import io.grpc.protobuf.StatusProto

    def fromCompletion(
        errorLogger: ContextualizedErrorLogger,
        completion: Completion,
    ): Try[CompletionResponse] =
      completion.status
        .toRight(missingStatusError(errorLogger))
        .toTry
        .flatMap {
          case status if status.code == 0 =>
            Success(CompletionResponse(completion))
          case nonZeroStatus =>
            Failure(
              StatusProto.toStatusRuntimeException(
                status.Status.toJavaProto(nonZeroStatus)
              )
            )
        }

    private def missingStatusError(errorLogger: ContextualizedErrorLogger): StatusRuntimeException =
      CommonErrors.ServiceInternalError
        .Generic(
          "Missing status in completion response",
          throwableO = None,
        )(errorLogger)
        .asGrpcError
  }
}
