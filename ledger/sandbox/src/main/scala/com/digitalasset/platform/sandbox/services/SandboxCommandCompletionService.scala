// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.digitalasset.api.util.TimestampConversion._
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.v1.command_completion_service._
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.validation.LedgerOffsetValidator
import com.digitalasset.platform.participant.util.Slf4JLog
import com.digitalasset.platform.server.api.validation.CommandCompletionServiceValidation
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.ledger.backend.api.v1.{
  LedgerBackend,
  LedgerSyncEvent,
  LedgerSyncOffset,
  RejectionReason
}
import com.google.rpc.status.Status
import io.grpc.Status.Code
import io.grpc.{BindableService, ServerServiceDefinition}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

import domain.LedgerId

class SandboxCommandCompletionService private (
    ledgerBackend: LedgerBackend
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
    val offsetOrError =
      request.offset.fold[Future[Option[Ref.LedgerString]]](Future.successful(None))(
        o =>
          LedgerOffsetValidator
            .validate(o, "offset")
            .fold(
              Future.failed, {
                case domain.LedgerOffset.Absolute(value) => Future.successful(Some(value))
                case domain.LedgerOffset.LedgerBegin => Future.successful(None)
                case domain.LedgerOffset.LedgerEnd =>
                  ledgerBackend.getCurrentLedgerEnd.map(Some.apply)
              }
          ))

    Source
      .fromFuture(offsetOrError)
      .flatMapConcat(completionSourceWithOffset(request, _, subscriptionId))
  }

  private def completionSourceWithOffset(
      request: CompletionStreamRequest,
      requestedOffset: Option[LedgerSyncOffset],
      subscriptionId: String): Source[CompletionStreamResponse, NotUsed] = {
    val requestingParties = request.parties.toSet
    val requestedApplicationId = request.applicationId
    ledgerBackend
      .ledgerSyncEvents(requestedOffset)
      .map { syncEvent =>
        val checkpoint =
          Some(
            Checkpoint(
              Some(fromInstant(syncEvent.recordTime)),
              Some(LedgerOffset(LedgerOffset.Value.Absolute(syncEvent.offset)))))

        syncEvent match {
          case tx: LedgerSyncEvent.AcceptedTransaction
              if isRequested(
                requestedApplicationId,
                requestingParties,
                tx.applicationId,
                tx.submitter) =>
            CompletionStreamResponse(
              checkpoint,
              tx.commandId.fold(List.empty[Completion])(c =>
                List(Completion(c, Some(Status()), tx.transactionId))))
          case err: LedgerSyncEvent.RejectedCommand
              if isRequested(
                requestedApplicationId,
                requestingParties,
                err.applicationId,
                Some(err.submitter)) =>
            CompletionStreamResponse(
              checkpoint,
              List(toCompletion(err.commandId, err.rejectionReason)))
          case _ =>
            logger.trace("Emitting checkpoint for subscription {}", subscriptionId)
            CompletionStreamResponse(checkpoint)
        }
      }
      .via(Slf4JLog(logger, s"Serving response for completion subscription $subscriptionId"))
  }

  private def isRequested(
      requestedApplicationId: String,
      requestingParties: Set[String],
      applicationId: Option[String],
      submittingParty: Option[String]) =
    applicationId.contains(requestedApplicationId) && submittingParty.fold(false)(
      requestingParties.contains)

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] =
    ledgerBackend.getCurrentLedgerEnd.map(le =>
      CompletionEndResponse(Some(LedgerOffset(LedgerOffset.Value.Absolute(le)))))

  private def toCompletion(commandId: String, error: RejectionReason): Completion = {
    val code = error match {
      case RejectionReason.Inconsistent(description) => Code.INVALID_ARGUMENT
      case RejectionReason.OutOfQuota(description) => Code.ABORTED
      case RejectionReason.TimedOut(description) => Code.ABORTED
      case RejectionReason.Disputed(description) => Code.INVALID_ARGUMENT
      case RejectionReason.DuplicateCommandId(description) => Code.INVALID_ARGUMENT
    }

    Completion(commandId, Some(Status(code.value(), error.description)), traceContext = None)
  }

  override def close(): Unit = {
    super.close()
  }
}

object SandboxCommandCompletionService {
  def apply(ledgerBackend: LedgerBackend)(
      implicit ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory): CommandCompletionServiceValidation
    with BindableService
    with AutoCloseable
    with CommandCompletionServiceLogging = {
    val impl = new SandboxCommandCompletionService(ledgerBackend)
    new CommandCompletionServiceValidation(impl, LedgerId(ledgerBackend.ledgerId))
    with BindableService with AutoCloseable with CommandCompletionServiceLogging {
      override def bindService(): ServerServiceDefinition =
        CommandCompletionServiceGrpc.bindService(this, DirectExecutionContext)

      override def close(): Unit = impl.close()
    }
  }
}
