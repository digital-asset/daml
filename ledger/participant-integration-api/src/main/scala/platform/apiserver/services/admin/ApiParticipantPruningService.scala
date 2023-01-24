// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.util.UUID
import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.api.v1.admin.participant_pruning_service.{
  LastPrunedOffsetsRequest,
  LastPrunedOffsetsResponse,
  ParticipantPruningServiceGrpc,
  PruneRequest,
  PruneResponse,
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.validation.ValidationErrors._
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.{IndexParticipantPruningService, LedgerEndService}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.lf.data.Ref.IdString
import com.daml.lf.data.Ref.IdString.LedgerString
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.{Metrics, Tracked}
import com.daml.platform.ApiOffset
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.services.logging
import com.daml.platform.server.api.services.grpc.Logging.traceId
import com.daml.platform.server.api.{ApiException, ValidationLogger}
import com.daml.tracing.Telemetry
import io.grpc.protobuf.StatusProto
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps

final class ApiParticipantPruningService private (
    readBackend: IndexParticipantPruningService with LedgerEndService,
    writeBackend: state.WriteParticipantPruningService,
    metrics: Metrics,
    telemetry: Telemetry,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends SinglePruningOpEnforcer
    with GrpcApiService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)

  override def bindService(): ServerServiceDefinition =
    ParticipantPruningServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()

  override def prune(request: PruneRequest): Future[PruneResponse] =
    withValidatedSubmissionId(request) { submissionId =>
      ifNotRunning {
        pruneInternal(submissionId, request)
      }
    }

  private def pruneInternal(
      submissionId: IdString.LedgerString,
      request: PruneRequest,
  ): Future[PruneResponse] =
    LoggingContext.withEnrichedLoggingContext(logging.submissionId(submissionId)) {
      implicit loggingContext =>
        logger.info(s"Pruning participant index and write service up to ${request.pruneUpTo}")

        val pruneResponse = for {
          pruneUpTo <- validateRequest(request)(
            loggingContext,
            contextualizedErrorLogger(submissionId),
          )

          // If write service pruning succeeds but ledger api server index pruning fails, the user can bring the
          // systems back in sync by reissuing the prune request at the currently specified or later offset.
          _ <- Tracked.future(
            metrics.daml.services.pruning.pruneCommandStarted,
            metrics.daml.services.pruning.pruneCommandCompleted,
            pruneWriteService(pruneUpTo, submissionId, request.pruneAllDivulgedContracts),
          )(MetricsContext(("phase", "underlyingLedger")))

          response <- Tracked.future(
            metrics.daml.services.pruning.pruneCommandStarted,
            metrics.daml.services.pruning.pruneCommandCompleted,
            pruneLedgerApiServerIndex(
              pruneUpTo,
              request.pruneAllDivulgedContracts,
            ),
          )(MetricsContext(("phase", "ledgerApiServerIndex")))
        } yield response

        pruneResponse.andThen(logger.logErrorsOnCall[PruneResponse])
    }

  protected def withValidatedSubmissionId(
      request: PruneRequest
  )(pruneWithSubmissionId: LedgerString => Future[PruneResponse]): Future[PruneResponse] =
    Ref.SubmissionId
      .fromString(
        if (request.submissionId.nonEmpty) request.submissionId else UUID.randomUUID().toString
      )
      .left
      .map(err =>
        invalidArgument(s"submission_id $err")(
          contextualizedErrorLogger(request.submissionId)
        )
      )
      .fold(
        t => Future.failed(ValidationLogger.logFailure(request, t)),
        pruneWithSubmissionId,
      )

  private def validateRequest(
      request: PruneRequest
  )(implicit
      loggingContext: LoggingContext,
      errorLogger: ContextualizedErrorLogger,
  ): Future[Offset] = {
    (for {
      pruneUpToString <- checkOffsetIsSpecified(request.pruneUpTo)
      pruneUpTo <- checkOffsetIsHexadecimal(pruneUpToString)
    } yield (pruneUpTo, pruneUpToString))
      .fold(
        t => Future.failed(ValidationLogger.logFailure(request, t)),
        o => checkOffsetIsBeforeLedgerEnd(o._1, o._2),
      )
  }

  private def pruneWriteService(
      pruneUpTo: Offset,
      submissionId: Ref.SubmissionId,
      pruneAllDivulgedContracts: Boolean,
  )(implicit loggingContext: LoggingContext): Future[Unit] = {
    import state.PruningResult._
    logger.info(
      s"About to prune participant ledger up to ${pruneUpTo.toApiString} inclusively starting with the write service"
    )
    writeBackend
      .prune(pruneUpTo, submissionId, pruneAllDivulgedContracts)
      .asScala
      .flatMap {
        case NotPruned(status) =>
          Future.failed(new ApiException(StatusProto.toStatusRuntimeException(status)))
        case ParticipantPruned =>
          logger.info(s"Pruned participant ledger up to ${pruneUpTo.toApiString} inclusively.")
          Future.successful(())
      }
  }

  private def pruneLedgerApiServerIndex(
      pruneUpTo: Offset,
      pruneAllDivulgedContracts: Boolean,
  )(implicit loggingContext: LoggingContext): Future[PruneResponse] = {
    logger.info(s"About to prune ledger api server index to ${pruneUpTo.toApiString} inclusively")
    readBackend
      .prune(pruneUpTo, pruneAllDivulgedContracts)
      .map { _ =>
        logger.info(s"Pruned ledger api server index up to ${pruneUpTo.toApiString} inclusively.")
        PruneResponse()
      }
  }

  private def checkOffsetIsSpecified(
      offset: String
  )(implicit errorLogger: ContextualizedErrorLogger): Either[StatusRuntimeException, String] =
    Either.cond(
      offset.nonEmpty,
      offset,
      invalidArgument("prune_up_to not specified"),
    )

  private def checkOffsetIsHexadecimal(
      pruneUpToString: String
  )(implicit errorLogger: ContextualizedErrorLogger): Either[StatusRuntimeException, Offset] =
    ApiOffset
      .tryFromString(pruneUpToString)
      .toEither
      .left
      .map(t =>
        LedgerApiErrors.RequestValidation.NonHexOffset
          .Error(
            fieldName = "prune_up_to",
            offsetValue = pruneUpToString,
            message =
              s"prune_up_to needs to be a hexadecimal string and not $pruneUpToString: ${t.getMessage}",
          )
          .asGrpcError
      )

  private def checkOffsetIsBeforeLedgerEnd(
      pruneUpToProto: Offset,
      pruneUpToString: String,
  )(implicit
      loggingContext: LoggingContext,
      errorLogger: ContextualizedErrorLogger,
  ): Future[Offset] =
    for {
      ledgerEnd <- readBackend.currentLedgerEnd()
      _ <-
        // NOTE: This constraint should be relaxed to (pruneUpToString <= ledgerEnd.value)
        if (pruneUpToString < ledgerEnd.value) Future.successful(())
        else
          Future.failed(
            LedgerApiErrors.RequestValidation.OffsetOutOfRange
              .Reject(
                s"prune_up_to needs to be before ledger end ${ledgerEnd.value}"
              )
              .asGrpcError
          )
    } yield pruneUpToProto

  private def contextualizedErrorLogger(submissionId: String)(implicit
      loggingContext: LoggingContext
  ): ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, Some(submissionId))

  override def lastPrunedOffsets(
      request: LastPrunedOffsetsRequest
  ): Future[LastPrunedOffsetsResponse] =
    readBackend.lastPrunedOffsets().map { case (divulgencePrunedUpTo, prunedUpToInclusive) =>
      LastPrunedOffsetsResponse(
        participantPrunedUpToInclusive =
          Some(LedgerOffset(LedgerOffset.Value.Absolute(divulgencePrunedUpTo.value))),
        allDivulgedContractsPrunedUpToInclusive =
          Some(LedgerOffset(LedgerOffset.Value.Absolute(prunedUpToInclusive.value))),
      )
    }
}

trait SinglePruningOpEnforcer extends ParticipantPruningServiceGrpc.ParticipantPruningService {
  this: ApiParticipantPruningService =>
  private var running: Option[Future[PruneResponse]] = None

  def ifNotRunning(go: => Future[PruneResponse]): Future[PruneResponse] =
    synchronized {
      def accept(): Future[PruneResponse] = {
        val newRunning = go
        running = Some(newRunning)
        newRunning
      }

      running match {
        case None => accept()
        case Some(currentF) =>
          if (currentF.isCompleted) accept()
          else
            Future.failed(new IllegalStateException("Pruning in progress. TODO log as error code"))
      }
    }
}

object ApiParticipantPruningService {
  def createApiService(
      readBackend: IndexParticipantPruningService with LedgerEndService,
      writeBackend: state.WriteParticipantPruningService,
      metrics: Metrics,
      telemetry: Telemetry,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ParticipantPruningServiceGrpc.ParticipantPruningService with GrpcApiService =
    new ApiParticipantPruningService(readBackend, writeBackend, metrics, telemetry)

}
