// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver.services

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.IndexCompletionsService
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.{CompletionEvent, LedgerId, LedgerOffset}
import com.digitalasset.ledger.api.messages.command.completion.CompletionStreamRequest
import com.digitalasset.ledger.api.v1.command_completion_service._
import com.digitalasset.ledger.api.validation.PartyNameChecker
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.platform.participant.util.Slf4JLog
import com.digitalasset.platform.server.api.services.domain.CommandCompletionService
import com.digitalasset.platform.server.api.services.grpc.GrpcCommandCompletionService
import io.grpc.ServerServiceDefinition
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future}

class ApiCommandCompletionService private (
    completionsService: IndexCompletionsService,
    loggerFactory: NamedLoggerFactory
)(
    implicit ec: ExecutionContext,
    protected val mat: Materializer,
    protected val esf: ExecutionSequencerFactory)
    extends CommandCompletionService {

  private val logger = loggerFactory.getLogger(this.getClass)

  private val subscriptionIdCounter = new AtomicLong()

  override def completionStreamSource(
      request: CompletionStreamRequest): Source[CompletionEvent, NotUsed] = {

    val subscriptionId = subscriptionIdCounter.getAndIncrement().toString
    logger.debug(
      "Received request for completion subscription {}: {}",
      subscriptionId: Any,
      request)

    val offset = request.offset.getOrElse(LedgerOffset.LedgerEnd)

    completionsService
      .getCompletions(offset, request.applicationId, request.parties)
      .via(Slf4JLog(logger, s"Serving response for completion subscription $subscriptionId"))
  }

  override def getLedgerEnd(ledgerId: domain.LedgerId): Future[LedgerOffset.Absolute] =
    completionsService.currentLedgerEnd

}

object ApiCommandCompletionService {
  def create(
      ledgerId: LedgerId,
      completionsService: IndexCompletionsService,
      loggerFactory: NamedLoggerFactory)(
      implicit ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory)
    : GrpcCommandCompletionService with GrpcApiService with CommandCompletionServiceLogging = {
    val impl: CommandCompletionService =
      new ApiCommandCompletionService(completionsService, loggerFactory)

    new GrpcCommandCompletionService(ledgerId, impl, PartyNameChecker.AllowAllParties)
    with GrpcApiService with CommandCompletionServiceLogging {
      override val logger: Logger = loggerFactory.getLogger(impl.getClass)
      override def bindService(): ServerServiceDefinition =
        CommandCompletionServiceGrpc.bindService(this, DirectExecutionContext)
    }
  }
}
