// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

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
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.participant.util.Slf4JLog
import com.digitalasset.platform.server.api.services.domain.CommandCompletionService
import com.digitalasset.platform.server.api.services.grpc.GrpcCommandCompletionService
import io.grpc.{BindableService, ServerServiceDefinition}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

class SandboxCommandCompletionService private (
    completionsService: IndexCompletionsService
)(
    implicit ec: ExecutionContext,
    protected val mat: Materializer,
    protected val esf: ExecutionSequencerFactory)
    extends CommandCompletionService {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val subscriptionIdCounter = new AtomicLong()

  override def completionStreamSource(
      request: CompletionStreamRequest): Source[CompletionEvent, NotUsed] = {

    val subscriptionId = subscriptionIdCounter.getAndIncrement().toString
    logger.debug(
      "Received request for completion subscription {}: {}",
      subscriptionId: Any,
      request)

    completionsService
      .getCompletions(request.offset, request.applicationId, request.parties)
      .via(Slf4JLog(logger, s"Serving response for completion subscription $subscriptionId"))
  }

  //TODO: this seems completely redundant!
  override def getLedgerEnd(ledgerId: domain.LedgerId): Future[LedgerOffset.Absolute] =
    completionsService.currentLedgerEnd

}

object SandboxCommandCompletionService {
  def createApiService(ledgerId: LedgerId, completionsService: IndexCompletionsService)(
      implicit ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory): GrpcCommandCompletionService
    with BindableService
    with AutoCloseable
    with CommandCompletionServiceLogging = {
    val impl: CommandCompletionService = new SandboxCommandCompletionService(completionsService)

    new GrpcCommandCompletionService(ledgerId, impl, PartyNameChecker.AllowAllParties)
    with BindableService with CommandCompletionServiceLogging {
      override val logger = LoggerFactory.getLogger(impl.getClass)
      override def bindService(): ServerServiceDefinition =
        CommandCompletionServiceGrpc.bindService(this, DirectExecutionContext)
    }
  }
}
