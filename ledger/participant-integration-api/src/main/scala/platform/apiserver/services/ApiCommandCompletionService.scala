// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.util.concurrent.atomic.AtomicLong
import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.ErrorCodesVersionSwitcher
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{LedgerId, LedgerOffset}
import com.daml.ledger.api.messages.command.completion.CompletionStreamRequest
import com.daml.ledger.api.v1.command_completion_service._
import com.daml.ledger.api.validation.PartyNameChecker
import com.daml.ledger.participant.state.index.v2.IndexCompletionsService
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.entries.{LoggingEntries, LoggingValue}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.services.ApiCommandCompletionService._
import com.daml.platform.server.api.services.domain.CommandCompletionService
import com.daml.platform.server.api.services.grpc.GrpcCommandCompletionService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiCommandCompletionService private (
    completionsService: IndexCompletionsService,
    metrics: Metrics,
)(implicit
    protected val materializer: Materializer,
    protected val esf: ExecutionSequencerFactory,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends CommandCompletionService {

  import Logging._

  private val logger = ContextualizedLogger.get(this.getClass)

  private val subscriptionIdCounter = new AtomicLong()

  override def completionStreamSource(
      request: CompletionStreamRequest
  ): Source[CompletionStreamResponse, NotUsed] =
    withEnrichedLoggingContext(logging.parties(request.parties), logging.offset(request.offset)) {
      implicit loggingContext =>
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
    }

  override def getLedgerEnd(ledgerId: domain.LedgerId): Future[LedgerOffset.Absolute] =
    completionsService.currentLedgerEnd().andThen(logger.logErrorsOnCall[LedgerOffset.Absolute])
}

private[apiserver] object ApiCommandCompletionService {

  def create(
      ledgerId: LedgerId,
      completionsService: IndexCompletionsService,
      metrics: Metrics,
      errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
  )(implicit
      materializer: Materializer,
      esf: ExecutionSequencerFactory,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): GrpcCommandCompletionService with GrpcApiService = {
    val impl: CommandCompletionService =
      new ApiCommandCompletionService(completionsService, metrics)

    new GrpcCommandCompletionService(
      ledgerId,
      impl,
      PartyNameChecker.AllowAllParties,
      errorCodesVersionSwitcher,
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
