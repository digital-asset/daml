// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.services

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.digitalasset.api.util.TimestampConversion._
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.v1.command_completion_service._
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.validation.LedgerOffsetValidator
import com.digitalasset.platform.server.api.validation.CommandCompletionServiceValidation
import com.google.rpc.status.Status
import io.grpc.Status.Code
import io.grpc.BindableService
import org.slf4j.LoggerFactory
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.daml.ledger.participant.state.v1.{Offset, RejectionReason}
import com.daml.ledger.participant.state.index.v1.{CompletionEvent, IndexService}
import com.digitalasset.daml.lf.data.Ref

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class DamlOnXCommandCompletionService private (indexService: IndexService)(
    implicit ec: ExecutionContext,
    protected val mat: Materializer,
    protected val esf: ExecutionSequencerFactory)
    extends CommandCompletionServiceAkkaGrpc
    with ErrorFactories {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def completionStreamSource(
      request: CompletionStreamRequest): Source[CompletionStreamResponse, NotUsed] = {

    val ledgerId = Ref.PackageId.assertFromString(request.ledgerId)

    val offsetFuture: Future[Option[Offset]] =
      request.offset match {
        case None => Future.successful(None)
        case Some(offset) =>
          LedgerOffsetValidator
            .validate(offset, "offset")
            // FIXME(JM): validate offset content
            .fold(
              Future.failed, {
                case domain.LedgerOffset.Absolute(value) =>
                  // FIXME(JM): properly handle failure
                  Future.successful(
                    Some(
                      Offset.assertFromString(value)
                    ))
                case domain.LedgerOffset.LedgerBegin => Future.successful(None)
                case domain.LedgerOffset.LedgerEnd =>
                  indexService.getLedgerEnd.map(Some(_))
              }
            )
      }

    Source
      .fromFuture(offsetFuture)
      .flatMapConcat { optOffset =>
        indexService
          .getCompletions(
            optOffset,
            Ref.LedgerIdString.assertFromString(request.applicationId),
            request.parties.toList.map(Ref.Party.assertFromString))
          .map {
            case CompletionEvent.CommandAccepted(offset, commandId, transactionId) =>
              logger.debug(s"sending completion accepted $offset: $commandId")

              CompletionStreamResponse(
                None, // FIXME(JM): is the checkpoint present in each response?
                List(Completion(commandId, Some(Status()), transactionId))
              )
            case CompletionEvent.CommandRejected(offset, commandId, reason) =>
              logger.debug(s"sending completion rejected $offset: $commandId: $reason")
              CompletionStreamResponse(
                None, // FIXME(JM): is the checkpoint present in each response?
                List(toCompletion(commandId, reason)))

            case CompletionEvent.Checkpoint(offset, recordTime) =>
              logger.debug(s"sending checkpoint $offset: $recordTime")

              CompletionStreamResponse(
                Some(
                  Checkpoint(
                    Some(fromInstant(recordTime.toInstant)), // FIXME(JM): conversion
                    Some(LedgerOffset(LedgerOffset.Value.Absolute(offset.toLedgerString)))))
              )
          }
      }
  }

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] =
    indexService.getLedgerEnd
      .map(
        offset =>
          CompletionEndResponse(
            Some(LedgerOffset(LedgerOffset.Value.Absolute(offset.toLedgerString)))))

  private def toCompletion(commandId: String, error: RejectionReason): Completion = {
    val code = error match {
      case RejectionReason.Inconsistent => Code.INVALID_ARGUMENT
      case RejectionReason.ResourcesExhausted => Code.ABORTED
      case RejectionReason.MaximumRecordTimeExceeded => Code.ABORTED
      case RejectionReason.Disputed(_) => Code.INVALID_ARGUMENT
      case RejectionReason.DuplicateCommand => Code.INVALID_ARGUMENT
      case RejectionReason.SubmitterCannotActViaParticipant(_) =>
        Code.INVALID_ARGUMENT
      case RejectionReason.PartyNotKnownOnLedger => Code.INVALID_ARGUMENT
    }
    Completion(commandId, Some(Status(code.value(), error.description)), traceContext = None)
  }

  override def close(): Unit = {
    super.close()
  }

}

object DamlOnXCommandCompletionService {
  def create(indexService: IndexService)(
      implicit ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory): CommandCompletionServiceValidation
    with BindableService
    with CommandCompletionServiceLogging = {

    val ledgerId = Await.result(indexService.getLedgerId(), 5.seconds)
    new CommandCompletionServiceValidation(
      new DamlOnXCommandCompletionService(indexService),
      ledgerId) with CommandCompletionServiceLogging
  }
}
