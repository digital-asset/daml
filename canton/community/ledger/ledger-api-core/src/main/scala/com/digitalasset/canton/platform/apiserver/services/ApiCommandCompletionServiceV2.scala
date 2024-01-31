// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.daml.logging.entries.LoggingEntries
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain.LedgerOffset
import com.digitalasset.canton.ledger.api.grpc.StreamingServiceLifecycleManagement
import com.digitalasset.canton.ledger.api.validation.{
  CompletionServiceRequestValidator,
  PartyNameChecker,
}
import com.digitalasset.canton.ledger.api.{ValidationLogger, domain}
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexCompletionsService
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.Metrics
import io.grpc.stub.StreamObserver
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

final class ApiCommandCompletionServiceV2(
    completionsService: IndexCompletionsService,
    metrics: Metrics,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    esf: ExecutionSequencerFactory,
    mat: Materializer,
) extends CommandCompletionServiceGrpc.CommandCompletionService
    with StreamingServiceLifecycleManagement
    with NamedLogging {
  import ApiConversions.*

  private val validator = new CompletionServiceRequestValidator(
    domain.LedgerId(""), // not used
    PartyNameChecker.AllowAllParties,
  )

  override def completionStream(
      request: CompletionStreamRequest,
      responseObserver: StreamObserver[CompletionStreamResponse],
  ): Unit = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    registerStream(responseObserver) {
      implicit val errorLoggingContext = ErrorLoggingContext(
        logger,
        loggingContextWithTrace.toPropertiesMap,
        loggingContextWithTrace.traceContext,
      )
      logger.debug(s"Received new completion request $request.")
      Source.future(completionsService.currentLedgerEnd()).flatMapConcat { ledgerEnd =>
        validator
          .validateGrpcCompletionStreamRequest(toV1(request))
          .flatMap(validator.validateCompletionStreamRequest(_, ledgerEnd))
          .fold(
            t =>
              Source.failed[CompletionStreamResponse](
                ValidationLogger.logFailureWithTrace(logger, request, t)
              ),
            request => {
              logger.info(
                s"Received request for completion subscription, ${loggingContextWithTrace
                    .serializeFiltered("parties", "offset")}"
              )
              val offset = request.offset.getOrElse(LedgerOffset.LedgerEnd)

              completionsService
                .getCompletions(offset, request.applicationId, request.parties)
                .via(
                  logger.enrichedDebugStream(
                    "Responding with completions.",
                    response =>
                      response.completion match {
                        case Some(completion) =>
                          LoggingEntries(
                            "commandId" -> completion.commandId,
                            "statusCode" -> completion.status.map(_.code),
                          )
                        case None =>
                          LoggingEntries()
                      },
                  )
                )
                .via(logger.logErrorsOnStream)
                .via(StreamMetrics.countElements(metrics.lapi.streams.completions))
            },
          )
      }
    }
  }
}
