// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.command_completion_service.*
import com.daml.logging.entries.{LoggingEntries, LoggingValue}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.domain.{LedgerId, LedgerOffset}
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.messages.command.completion.CompletionStreamRequest
import com.digitalasset.canton.ledger.api.services.CommandCompletionService
import com.digitalasset.canton.ledger.api.validation.{
  CompletionServiceRequestValidator,
  PartyNameChecker,
}
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
import com.digitalasset.canton.platform.apiserver.services.command.CommandCompletionServiceImpl.*
import com.digitalasset.canton.platform.apiserver.services.{
  ApiCommandCompletionService,
  ApiConversions,
  StreamMetrics,
  logging,
}
import io.grpc.ServerServiceDefinition
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class CommandCompletionServiceImpl private (
    completionsService: IndexCompletionsService,
    validator: CompletionServiceRequestValidator,
    metrics: Metrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    protected val materializer: Materializer,
    protected val esf: ExecutionSequencerFactory,
    executionContext: ExecutionContext,
) extends CommandCompletionService
    with NamedLogging {

  import Logging.*

  override def completionStreamSource(
      request: CompletionStreamRequest
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace
  ): Source[CompletionStreamResponse, NotUsed] = {
    Source.future(getLedgerEndImpl).flatMapConcat { ledgerEnd =>
      validator
        .validateCompletionStreamRequest(request, ledgerEnd)(
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
          LoggingContextWithTrace.withEnrichedLoggingContext(
            logging.parties(request.parties),
            logging.offset(request.offset),
          )(processCompletionStreamRequest(_)),
        )
    }
  }

  private def processCompletionStreamRequest(implicit
      loggingContext: LoggingContextWithTrace
  ): CompletionStreamRequest => Source[CompletionStreamResponse, NotUsed] = { request =>
    logger.info(
      s"Received request for completion subscription, ${loggingContext.serializeFiltered("parties", "offset")}"
    )

    val offset = request.offset.getOrElse(LedgerOffset.LedgerEnd)

    completionsService
      .getCompletions(offset, request.applicationId, request.parties)
      .map(ApiConversions.toV1)
      .via(
        logger.enrichedDebugStream(
          "Responding with completions.",
          (response: CompletionStreamResponse) =>
            LoggingEntries("response" -> responseToLoggingValue(response)),
        )
      )
      .via(logger.logErrorsOnStream)
      .via(StreamMetrics.countElements(metrics.daml.lapi.streams.completions))
  }

  override def getLedgerEnd(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[LedgerOffset.Absolute] = {
    logger.info(s"Received request for completion ledger end")
    getLedgerEndImpl
  }

  private def getLedgerEndImpl(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[LedgerOffset.Absolute] =
    completionsService.currentLedgerEnd().andThen(logger.logErrorsOnCall[LedgerOffset.Absolute])

}

private[apiserver] object CommandCompletionServiceImpl {

  def createApiService(
      ledgerId: LedgerId,
      completionsService: IndexCompletionsService,
      metrics: Metrics,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      materializer: Materializer,
      esf: ExecutionSequencerFactory,
      executionContext: ExecutionContext,
  ): ApiCommandCompletionService & GrpcApiService = {
    val validator = new CompletionServiceRequestValidator(
      ledgerId,
      PartyNameChecker.AllowAllParties,
    )
    val impl: CommandCompletionService =
      new CommandCompletionServiceImpl(
        completionsService,
        validator,
        metrics,
        loggerFactory,
      )

    new ApiCommandCompletionService(
      impl,
      validator,
      telemetry,
      loggerFactory,
    ) with GrpcApiService {
      override def bindService(): ServerServiceDefinition =
        CommandCompletionServiceGrpc.bindService(this, executionContext)
    }
  }

  private object Logging {
    def responseToLoggingValue(response: CompletionStreamResponse): LoggingValue =
      LoggingValue.OfIterable(
        response.completions.view.map(completion =>
          LoggingValue.Nested.fromEntries(
            "commandId" -> completion.commandId,
            "statusCode" -> completion.status.map(_.code),
          )
        )
      )
  }
}
