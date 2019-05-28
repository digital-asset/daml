// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.{
  CompletionEvent,
  CompletionsService,
  RejectionReason
}
import com.digitalasset.api.util.TimestampConversion._
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.v1.command_completion_service._
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.validation.LedgerOffsetValidator
import com.digitalasset.platform.participant.util.Slf4JLog
import com.digitalasset.platform.server.api.validation.{
  CommandCompletionServiceValidation,
  FieldValidations
}
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.google.rpc.status.Status
import io.grpc.Status.Code
import io.grpc.{BindableService, ServerServiceDefinition}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import domain.{CommandId, LedgerId}
import scalaz.syntax.tag._

//TODO: use a validator object like in the other services!
class SandboxCommandCompletionService private (
    completionsService: CompletionsService
)(
    implicit ec: ExecutionContext,
    protected val mat: Materializer,
    protected val esf: ExecutionSequencerFactory)
    extends CommandCompletionServiceAkkaGrpc {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val subscriptionIdCounter = new AtomicLong()

  override def completionStreamSource(
      request: CompletionStreamRequest): Source[CompletionStreamResponse, NotUsed] = {
    val subscriptionId = subscriptionIdCounter.getAndIncrement().toString
    logger.debug(
      "Received request for completion subscription {}: {}",
      subscriptionId: Any,
      request)

    val offsetOrError = for {
      offset <- FieldValidations.requirePresence(request.offset, "offset")
      convertedOffset <- LedgerOffsetValidator.validate(offset, "offset")
    } yield convertedOffset

    offsetOrError.fold(
      Source.failed,
      offset => completionSourceWithOffset(request, offset, subscriptionId))
  }

  private def completionSourceWithOffset(
      request: CompletionStreamRequest,
      requestedOffset: domain.LedgerOffset,
      subscriptionId: String): Source[CompletionStreamResponse, NotUsed] = {
    //TODO: put these into a proper validator
    val requestingParties = request.parties.toSet.map(Ref.Party.assertFromString)
    val requestedApplicationId: domain.ApplicationId =
      domain.ApplicationId(Ref.LedgerString.assertFromString(request.applicationId))

    completionsService
      .getCompletions(requestedOffset, requestedApplicationId, requestingParties)
      .map { ce =>
        val checkpoint = Some(
          Checkpoint(
            Some(fromInstant(ce.recordTime)),
            Some(LedgerOffset(LedgerOffset.Value.Absolute(ce.offset.value)))))

        ce match {
          case CompletionEvent.CommandAccepted(_, _, commandId, transactionId) =>
            //TODO: code smell, if we don't send anything for empty command IDs, why do we get the event at all?
            CompletionStreamResponse(
              checkpoint,
              commandId.fold(List.empty[Completion])(cId =>
                List(Completion(cId.unwrap, Some(Status()), transactionId.unwrap))))

          case CompletionEvent.CommandRejected(_, _, commandId, reason) =>
            CompletionStreamResponse(checkpoint, List(rejectionToCompletion(commandId, reason)))

          case _: CompletionEvent.Checkpoint =>
            logger.trace("Emitting checkpoint for subscription {}", subscriptionId)
            CompletionStreamResponse(checkpoint)
        }
      }
      .via(Slf4JLog(logger, s"Serving response for completion subscription $subscriptionId"))
  }

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] =
    completionsService.currentLedgerEnd.map(le =>
      CompletionEndResponse(Some(LedgerOffset(LedgerOffset.Value.Absolute(le.value)))))

  private def rejectionToCompletion(commandId: CommandId, error: RejectionReason): Completion = {
    val code = error match {
      case _: RejectionReason.Inconsistent => Code.INVALID_ARGUMENT
      case _: RejectionReason.OutOfQuota => Code.ABORTED
      case _: RejectionReason.TimedOut => Code.ABORTED
      case _: RejectionReason.Disputed => Code.INVALID_ARGUMENT
      case _: RejectionReason.DuplicateCommandId => Code.INVALID_ARGUMENT
    }

    Completion(commandId.unwrap, Some(Status(code.value(), error.description)), traceContext = None)
  }

  override def close(): Unit =
    super.close()

}

object SandboxCommandCompletionService {
  def apply(ledgerId: LedgerId, completionsService: CompletionsService)(
      implicit ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory): CommandCompletionServiceValidation
    with BindableService
    with AutoCloseable
    with CommandCompletionServiceLogging = {
    val impl = new SandboxCommandCompletionService(completionsService)
    new CommandCompletionServiceValidation(impl, ledgerId) with BindableService with AutoCloseable
    with CommandCompletionServiceLogging {
      override def bindService(): ServerServiceDefinition =
        CommandCompletionServiceGrpc.bindService(this, DirectExecutionContext)

      override def close(): Unit = impl.close()
    }
  }
}
