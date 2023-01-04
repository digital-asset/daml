// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.{LedgerId, LedgerOffset}
import com.daml.ledger.api.messages.command.completion.CompletionStreamRequest
import com.daml.ledger.api.v1.command_completion_service._
import com.daml.ledger.api.validation.{CompletionServiceRequestValidator, PartyNameChecker}
import com.daml.ledger.participant.state.index.v2.IndexCompletionsService
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.entries.{LoggingEntries, LoggingValue}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.services.ApiCommandCompletionService._
import com.daml.platform.server.api.ValidationLogger
import com.daml.platform.server.api.services.domain.CommandCompletionService
import com.daml.platform.server.api.services.grpc.GrpcCommandCompletionService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiCommandCompletionService private (
    completionsService: IndexCompletionsService,
    validator: CompletionServiceRequestValidator,
    metrics: Metrics,
)(implicit
    protected val materializer: Materializer,
    protected val esf: ExecutionSequencerFactory,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends CommandCompletionService {

  import Logging._

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)
  private implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  private val subscriptionIdCounter = new AtomicLong()

  override def completionStreamSource(
      request: CompletionStreamRequest
  ): Source[CompletionStreamResponse, NotUsed] =
    Source.future(getLedgerEnd()).flatMapConcat { ledgerEnd =>
      validator
        .validateCompletionStreamRequest(request, ledgerEnd)
        .fold(
          t => Source.failed[CompletionStreamResponse](ValidationLogger.logFailure(request, t)),
          request =>
            withEnrichedLoggingContext(
              logging.parties(request.parties),
              logging.offset(request.offset),
            ) { implicit loggingContext =>
              val subscriptionId = subscriptionIdCounter.getAndIncrement().toString
              logger.info(s"Received request for completion subscription $subscriptionId: $request")

              val offset = request.offset.getOrElse(LedgerOffset.LedgerEnd)

              completionsService
                .getCompletions(offset, request.applicationId, request.parties)
                .via(
                  logger.enrichedDebugStream(
                    "Responding with completions.",
                    response => LoggingEntries("response" -> responseToLoggingValue(response)),
                  )
                )
                .via(logger.logErrorsOnStream)
                .via(StreamMetrics.countElements(metrics.daml.lapi.streams.completions))
            },
        )
    }

  override def getLedgerEnd(): Future[LedgerOffset.Absolute] =
    completionsService.currentLedgerEnd().andThen(logger.logErrorsOnCall[LedgerOffset.Absolute])
}

private[apiserver] object ApiCommandCompletionService {

  def create(
      ledgerId: LedgerId,
      completionsService: IndexCompletionsService,
      metrics: Metrics,
  )(implicit
      materializer: Materializer,
      esf: ExecutionSequencerFactory,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): (CommandCompletionService, GrpcCommandCompletionService with GrpcApiService) = {
    val validator = new CompletionServiceRequestValidator(
      ledgerId,
      PartyNameChecker.AllowAllParties,
    )
    val impl: CommandCompletionService =
      new ApiCommandCompletionService(completionsService, validator, metrics)

    impl -> new GrpcCommandCompletionService(
      impl,
      validator,
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
