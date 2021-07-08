// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.messages.command.completion.{
  CompletionStreamRequest => ValidatedCompletionStreamRequest
}
import com.daml.ledger.api.v1.command_completion_service._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.validation.{CompletionServiceRequestValidator, PartyNameChecker}
import com.daml.platform.server.api.ValidationLogger
import com.daml.platform.server.api.services.domain.CommandCompletionService
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}

object GrpcCommandCompletionService {

  private[this] val completionStreamDefaultOffset = Some(domain.LedgerOffset.LedgerEnd)

  private def fillInWithDefaults(
      request: ValidatedCompletionStreamRequest
  ): ValidatedCompletionStreamRequest =
    if (request.offset.isDefined) {
      request
    } else {
      request.copy(offset = completionStreamDefaultOffset)
    }

}

class GrpcCommandCompletionService(
    ledgerId: LedgerId,
    service: CommandCompletionService,
    partyNameChecker: PartyNameChecker,
)(implicit
    protected val mat: Materializer,
    protected val esf: ExecutionSequencerFactory,
    executionContext: ExecutionContext,
) extends CommandCompletionServiceAkkaGrpc {

  private val validator = new CompletionServiceRequestValidator(ledgerId, partyNameChecker)
  protected implicit val logger: Logger = LoggerFactory.getLogger(service.getClass)

  override def completionStreamSource(
      request: CompletionStreamRequest
  ): Source[CompletionStreamResponse, akka.NotUsed] = {
    Source.future(service.getLedgerEnd(LedgerId(request.ledgerId))).flatMapConcat { ledgerEnd =>
      validator
        .validateCompletionStreamRequest(request, ledgerEnd, service.offsetOrdering)
        .fold(
          t => Source.failed[CompletionStreamResponse](ValidationLogger.logFailure(request, t)),
          GrpcCommandCompletionService.fillInWithDefaults _ andThen service.completionStreamSource,
        )
    }
  }

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] =
    validator
      .validateCompletionEndRequest(request)
      .fold(
        t => Future.failed[CompletionEndResponse](ValidationLogger.logFailure(request, t)),
        req =>
          service
            .getLedgerEnd(req.ledgerId)
            .map(abs =>
              CompletionEndResponse(Some(LedgerOffset(LedgerOffset.Value.Absolute(abs.value))))
            ),
      )

}
