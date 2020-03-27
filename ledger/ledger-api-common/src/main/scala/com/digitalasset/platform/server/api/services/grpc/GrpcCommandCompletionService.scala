// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.services.grpc

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.messages.command.completion.{
  CompletionStreamRequest => ValidatedCompletionStreamRequest
}
import com.digitalasset.ledger.api.v1.command_completion_service._
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.validation.{CompletionServiceRequestValidator, PartyNameChecker}
import com.digitalasset.platform.server.api.services.domain.CommandCompletionService

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
    validator
      .validateCompletionStreamRequest(request)
      .fold(
        Source.failed[CompletionStreamResponse],
        GrpcCommandCompletionService.fillInWithDefaults _ andThen service.completionStreamSource
      )
  }

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] =
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
