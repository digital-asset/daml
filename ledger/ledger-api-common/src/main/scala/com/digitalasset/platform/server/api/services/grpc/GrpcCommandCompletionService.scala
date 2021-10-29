// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.{DamlContextualizedErrorLogger, ErrorCodesVersionSwitcher}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.messages.command.completion.{
  CompletionStreamRequest => ValidatedCompletionStreamRequest
}
import com.daml.ledger.api.v1.command_completion_service._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.validation.{CompletionServiceRequestValidator, PartyNameChecker}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.server.api.ValidationLogger
import com.daml.platform.server.api.services.domain.CommandCompletionService
import com.daml.ledger.api.v1.command_completion_service.{
  CommandCompletionService => CommandCompletionServiceAkkaGrpc
}

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
    errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
)(implicit
    protected val mat: Materializer,
    protected val esf: ExecutionSequencerFactory,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends CommandCompletionServiceAkkaGrpc {

  private val validator =
    new CompletionServiceRequestValidator(ledgerId, partyNameChecker, errorCodesVersionSwitcher)
  protected implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  private implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  override def completionStream(
      request: CompletionStreamRequest
  ): Source[CompletionStreamResponse, akka.NotUsed] = {
    Source.future(service.getLedgerEnd(LedgerId(request.ledgerId))).flatMapConcat { ledgerEnd =>
      validator
        .validateCompletionStreamRequest(request, ledgerEnd)
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
