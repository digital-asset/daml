// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.services.grpc

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.{CommandId, CompletionEvent, LedgerId, RejectionReason}
import com.digitalasset.ledger.api.v1.command_completion_service._
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.validation.{CompletionServiceRequestValidator, PartyNameChecker}
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.server.api.services.domain.CommandCompletionService
import com.google.rpc.status.Status
import io.grpc.Status.Code
import scalaz.syntax.tag._

import scala.concurrent.Future

class GrpcCommandCompletionService(
    ledgerId: LedgerId,
    service: CommandCompletionService,
    partyNameChecker: PartyNameChecker
)(implicit protected val esf: ExecutionSequencerFactory, protected val mat: Materializer)
    extends CommandCompletionServiceAkkaGrpc {

  private val validator = new CompletionServiceRequestValidator(ledgerId, partyNameChecker)

  override def completionStreamSource(
      request: CompletionStreamRequest): Source[CompletionStreamResponse, akka.NotUsed] =
    validator
      .validateCompletionStreamRequest(request)
      .fold(
        Source.failed[CompletionStreamResponse],
        validatedRequest =>
          service
            .completionStreamSource(validatedRequest)
            .map(toApiCompletion)
      )

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

  private def toApiCompletion(ce: domain.CompletionEvent): CompletionStreamResponse = {
    val checkpoint = Some(
      Checkpoint(
        Some(fromInstant(ce.recordTime)),
        Some(LedgerOffset(LedgerOffset.Value.Absolute(ce.offset.value)))))

    ce match {
      case CompletionEvent.CommandAccepted(_, _, commandId, transactionId) =>
        CompletionStreamResponse(
          checkpoint,
          List(Completion(commandId.unwrap, Some(Status()), transactionId.unwrap))
        )

      case CompletionEvent.CommandRejected(_, _, commandId, reason) =>
        CompletionStreamResponse(checkpoint, List(rejectionToCompletion(commandId, reason)))

      case _: CompletionEvent.Checkpoint =>
        CompletionStreamResponse(checkpoint)
    }
  }

  private def rejectionToCompletion(commandId: CommandId, error: RejectionReason): Completion = {
    val code = error match {
      case _: RejectionReason.Inconsistent => Code.INVALID_ARGUMENT
      case _: RejectionReason.OutOfQuota => Code.ABORTED
      case _: RejectionReason.TimedOut => Code.ABORTED
      case _: RejectionReason.Disputed => Code.INVALID_ARGUMENT
      case _: RejectionReason.DuplicateCommandId => Code.INVALID_ARGUMENT
      case _: RejectionReason.PartyNotKnownOnLedger => Code.INVALID_ARGUMENT
      case _: RejectionReason.SubmitterCannotActViaParticipant => Code.PERMISSION_DENIED
    }

    Completion(commandId.unwrap, Some(Status(code.value(), error.description)), traceContext = None)
  }

}
