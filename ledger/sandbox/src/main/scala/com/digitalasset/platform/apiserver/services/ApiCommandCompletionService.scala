// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver.services

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.IndexCompletionsService
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.{LedgerId, LedgerOffset}
import com.digitalasset.ledger.api.messages.command.completion.CompletionStreamRequest
import com.digitalasset.ledger.api.v1.command_completion_service._
import com.digitalasset.ledger.api.validation.PartyNameChecker
import com.digitalasset.logging.LoggingContext.withEnrichedLoggingContext
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.services.domain.CommandCompletionService
import com.digitalasset.platform.server.api.services.grpc.GrpcCommandCompletionService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class ApiCommandCompletionService private (completionsService: IndexCompletionsService)(
    implicit ec: ExecutionContext,
    protected val mat: Materializer,
    protected val esf: ExecutionSequencerFactory,
    logCtx: LoggingContext)
    extends CommandCompletionService {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val subscriptionIdCounter = new AtomicLong()

  override def completionStreamSource(
      request: CompletionStreamRequest): Source[CompletionStreamResponse, NotUsed] =
    withEnrichedLoggingContext(logging.parties(request.parties), logging.offset(request.offset)) {
      implicit logCtx =>
        val subscriptionId = subscriptionIdCounter.getAndIncrement().toString
        logger.debug(s"Received request for completion subscription $subscriptionId: $request")

        val offset = request.offset.getOrElse(LedgerOffset.LedgerEnd)

        completionsService
          .getCompletions(offset, request.applicationId, request.parties)
          .via(logger.logErrorsOnStream)
    }

  override def getLedgerEnd(ledgerId: domain.LedgerId): Future[LedgerOffset.Absolute] =
    completionsService.currentLedgerEnd().andThen(logger.logErrorsOnCall[LedgerOffset.Absolute])

}

object ApiCommandCompletionService {

  def create(ledgerId: LedgerId, completionsService: IndexCompletionsService)(
      implicit ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory,
      logCtx: LoggingContext): GrpcCommandCompletionService with GrpcApiService = {
    val impl: CommandCompletionService =
      new ApiCommandCompletionService(completionsService)

    new GrpcCommandCompletionService(ledgerId, impl, PartyNameChecker.AllowAllParties)
    with GrpcApiService {
      override def bindService(): ServerServiceDefinition =
        CommandCompletionServiceGrpc.bindService(this, DirectExecutionContext)
    }
  }
}
