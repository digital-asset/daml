// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import com.daml.error.ContextualizedErrorLogger
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.command_completion_service.*
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.grpc.StreamingServiceLifecycleManagement
import com.digitalasset.canton.ledger.api.services.CommandCompletionService
import com.digitalasset.canton.ledger.api.validation.CompletionServiceRequestValidator
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}

class ApiCommandCompletionService(
    service: CommandCompletionService,
    validator: CompletionServiceRequestValidator,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    mat: Materializer,
    esf: ExecutionSequencerFactory,
    executionContext: ExecutionContext,
) extends CommandCompletionServiceGrpc.CommandCompletionService
    with StreamingServiceLifecycleManagement
    with NamedLogging {

  protected implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    ErrorLoggingContext(
      logger,
      loggerFactory.properties,
      TraceContext.empty,
    )

  def completionStream(
      request: CompletionStreamRequest,
      responseObserver: StreamObserver[CompletionStreamResponse],
  ): Unit = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    registerStream(responseObserver) {
      validator
        .validateGrpcCompletionStreamRequest(request)(
          ErrorLoggingContext(
            logger,
            loggingContextWithTrace.toPropertiesMap,
            loggingContextWithTrace.traceContext,
          )
        )
        .fold(
          t =>
            Source.failed[CompletionStreamResponse](
              ValidationLogger.logFailureWithTrace(logger, request, t)
            ),
          service.completionStreamSource,
        )
    }
  }

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    validator
      .validateCompletionEndRequest(request)(
        ErrorLoggingContext(
          logger,
          loggingContextWithTrace.toPropertiesMap,
          loggingContextWithTrace.traceContext,
        )
      )
      .fold(
        t =>
          Future.failed[CompletionEndResponse](
            ValidationLogger.logFailureWithTrace(logger, request, t)
          ),
        _ =>
          service.getLedgerEnd
            .map(abs =>
              CompletionEndResponse(Some(LedgerOffset(LedgerOffset.Value.Absolute(abs.value))))
            ),
      )
  }

}
