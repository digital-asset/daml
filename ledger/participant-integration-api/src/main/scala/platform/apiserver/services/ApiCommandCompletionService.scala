// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{LedgerId, LedgerOffset}
import com.daml.ledger.api.messages.command.completion.CompletionStreamRequest
import com.daml.ledger.api.v1.command_completion_service._
import com.daml.ledger.api.validation.PartyNameChecker
import com.daml.ledger.participant.state.index.v2.IndexCompletionsService
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.services.domain.CommandCompletionService
import com.daml.platform.server.api.services.grpc.GrpcCommandCompletionService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiCommandCompletionService private (
    completionsService: IndexCompletionsService
)(implicit
    protected val materializer: Materializer,
    protected val esf: ExecutionSequencerFactory,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends CommandCompletionService {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val subscriptionIdCounter = new AtomicLong()

  override def completionStreamSource(
      request: CompletionStreamRequest
  ): Source[CompletionStreamResponse, NotUsed] =
    withEnrichedLoggingContext(logging.parties(request.parties), logging.offset(request.offset)) {
      implicit loggingContext =>
        val subscriptionId = subscriptionIdCounter.getAndIncrement().toString
        logger.debug(s"Received request for completion subscription $subscriptionId: $request")

        val offset = request.offset.getOrElse(LedgerOffset.LedgerEnd)

        completionsService
          .getCompletions(offset, request.applicationId, request.parties)
          .via(logger.debugStream(completionsLoggable))
          .via(logger.logErrorsOnStream)
    }

  private def completionsLoggable(response: CompletionStreamResponse): String =
    s"Responding with completions: ${response.completions.toList
      .map(c => singleCompletionLoggable(c.commandId, c.status.map(_.code)))}"

  private def singleCompletionLoggable(
      commandId: String,
      statusCode: Option[Int],
  ): Map[String, String] =
    Map(
      logging.commandId(commandId),
      "statusCode" -> statusCode.map(_.toString).getOrElse(""),
    )

  override def getLedgerEnd(ledgerId: domain.LedgerId): Future[LedgerOffset.Absolute] =
    completionsService.currentLedgerEnd().andThen(logger.logErrorsOnCall[LedgerOffset.Absolute])

  override lazy val offsetOrdering: Ordering[LedgerOffset.Absolute] =
    Ordering.by[LedgerOffset.Absolute, String](_.value)

}

private[apiserver] object ApiCommandCompletionService {

  def create(ledgerId: LedgerId, completionsService: IndexCompletionsService)(implicit
      materializer: Materializer,
      esf: ExecutionSequencerFactory,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): GrpcCommandCompletionService with GrpcApiService = {
    val impl: CommandCompletionService =
      new ApiCommandCompletionService(completionsService)

    new GrpcCommandCompletionService(ledgerId, impl, PartyNameChecker.AllowAllParties)
      with GrpcApiService {
      override def bindService(): ServerServiceDefinition =
        CommandCompletionServiceGrpc.bindService(this, executionContext)
    }
  }
}
