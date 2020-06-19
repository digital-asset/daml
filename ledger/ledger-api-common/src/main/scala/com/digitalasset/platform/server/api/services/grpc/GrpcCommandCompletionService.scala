// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.dec.DirectExecutionContext
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.messages.command.completion.{
  CompletionStreamRequest => ValidatedCompletionStreamRequest
}
import com.daml.ledger.api.v1.command_completion_service._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.validation.{CompletionServiceRequestValidator, PartyNameChecker}
import com.daml.logging.ThreadLogger
import com.daml.platform.server.api.services.domain.CommandCompletionService

import scala.concurrent.Future

object GrpcCommandCompletionService {

  private[this] val completionStreamDefaultOffset = Some(domain.LedgerOffset.LedgerEnd)

  private def fillInWithDefaults(
      request: ValidatedCompletionStreamRequest): ValidatedCompletionStreamRequest =
    if (request.offset.isDefined) {
      request
    } else {
      request.copy(offset = completionStreamDefaultOffset)
    }

}

class GrpcCommandCompletionService(
    ledgerId: LedgerId,
    service: CommandCompletionService,
    partyNameChecker: PartyNameChecker
)(implicit protected val esf: ExecutionSequencerFactory, protected val mat: Materializer)
    extends CommandCompletionServiceAkkaGrpc {

  private val validator = new CompletionServiceRequestValidator(ledgerId, partyNameChecker)

  override def completionStreamSource(
      request: CompletionStreamRequest): Source[CompletionStreamResponse, akka.NotUsed] = {
    ThreadLogger.traceThread("GrpcCommandCompletionService.completionStreamSource")
    validator
      .validateCompletionStreamRequest(request)
      .fold(
        Source.failed[CompletionStreamResponse],
        GrpcCommandCompletionService.fillInWithDefaults _ andThen service.completionStreamSource
      )
  }

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] = {
    ThreadLogger.traceThread("GrpcCommandCompletionService.completionEnd")
    validator
      .validateCompletionEndRequest(request)
      .fold(
        Future.failed[CompletionEndResponse],
        req =>
          service
            .getLedgerEnd(req.ledgerId)
            .map(abs =>
              CompletionEndResponse(Some(LedgerOffset(LedgerOffset.Value.Absolute(abs.value)))))(
              DirectExecutionContext)
      )
  }

}
