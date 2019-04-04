// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.services

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.api.util.TimestampConversion._
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.v1.command_completion_service._
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.validation.LedgerOffsetValidator
import com.digitalasset.platform.server.api.validation.CommandCompletionServiceValidation
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.google.rpc.status.Status
import io.grpc.Status.Code
import io.grpc.{BindableService, ServerServiceDefinition}
import org.slf4j.LoggerFactory
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.daml.ledger.participant.state.v1.RejectionReason
import com.daml.ledger.participant.state.index.v1.{CompletionEvent, IndexService, Offset}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class DamlOnXCommandCompletionService private (indexService: IndexService)(
    implicit ec: ExecutionContext,
    protected val mat: Materializer,
    protected val esf: ExecutionSequencerFactory)
    extends CommandCompletionServiceAkkaGrpc
    with ErrorFactories
    with DamlOnXServiceUtils {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def completionStreamSource(
      request: CompletionStreamRequest): Source[CompletionStreamResponse, NotUsed] = {

    val offsetFuture: Future[Option[Offset]] =
      request.offset match {
        case None => Future.successful(None)
        case Some(offset) =>
          LedgerOffsetValidator
            .validate(offset, "offset")
            .fold(
              Future.failed, {
                case domain.LedgerOffset.Absolute(value) => Future.successful(Some(value.toLong))
                case domain.LedgerOffset.LedgerBegin => Future.successful(None)
                case domain.LedgerOffset.LedgerEnd =>
                  consumeAsyncResult(indexService.getLedgerEnd(request.ledgerId)).map(Some(_))
              }
            )
      }
    val compsFuture = offsetFuture.flatMap { optOffset =>
      consumeAsyncResult(indexService
        .getCompletions(request.ledgerId, optOffset, request.applicationId, request.parties.toList))
    }

    Source
      .fromFuture(compsFuture)
      .flatMapConcat(src => {
        src.map {
          case CompletionEvent.CommandAccepted(offset, commandId) =>
            logger.debug(s"sending completion accepted $offset: $commandId")

            CompletionStreamResponse(
              None, // FIXME(JM): is the checkpoint present in each response?
              List(Completion(commandId, Some(Status())))
            )
          case CompletionEvent.CommandRejected(updateId, commandId, reason) =>
            logger.debug(s"sending completion rejected $updateId: $commandId: $reason")
            CompletionStreamResponse(
              None, // FIXME(JM): is the checkpoint present in each response?
              List(toCompletion(commandId, reason)))

          case CompletionEvent.Checkpoint(offset, recordTime) =>
            logger.debug(s"sending checkpoint $offset: $recordTime")

            CompletionStreamResponse(
              Some(
                Checkpoint(
                  Some(fromInstant(recordTime.toInstant)), // FIXME(JM): conversion
                  Some(LedgerOffset(LedgerOffset.Value.Absolute(offset.toString)))))
            )
        }
      })
      .alsoTo(Sink.onComplete { _ =>
        logger.trace("Completion stream closed")
      })
  }

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] =
    consumeAsyncResult(
      indexService
        .getLedgerEnd(request.ledgerId)
    ).map(offset =>
      CompletionEndResponse(Some(LedgerOffset(LedgerOffset.Value.Absolute(offset.toString)))))

  private def toCompletion(commandId: String, error: RejectionReason): Completion = {
    val code = error match {
      case RejectionReason.Inconsistent => Code.INVALID_ARGUMENT
      case RejectionReason.ResourcesExhausted => Code.ABORTED
      case RejectionReason.MaximumRecordTimeExceeded => Code.ABORTED
      case RejectionReason.Disputed(_) => Code.INVALID_ARGUMENT
      case RejectionReason.DuplicateCommandId => Code.INVALID_ARGUMENT
      case RejectionReason.SubmitterNotHostedOnParticipant => Code.INVALID_ARGUMENT
      case RejectionReason.PartyNotKnownOnLedger => Code.INVALID_ARGUMENT
    }
    Completion(commandId, Some(Status(code.value(), error.description)), None)
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
    with AutoCloseable
    with CommandCompletionServiceLogging = {
    val impl = new DamlOnXCommandCompletionService(indexService)

    // FIXME(JM): rewrite validation to not use static ledger id
    val indexId = Await.result(indexService.getCurrentIndexId(), 5.seconds)

    new CommandCompletionServiceValidation(impl, indexId) with BindableService with AutoCloseable
    with CommandCompletionServiceLogging {
      override def bindService(): ServerServiceDefinition =
        CommandCompletionServiceGrpc.bindService(this, DirectExecutionContext)

      override def close(): Unit = impl.close()
    }
  }
}
