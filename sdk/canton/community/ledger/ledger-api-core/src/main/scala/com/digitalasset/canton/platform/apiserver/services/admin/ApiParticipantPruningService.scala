// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.admin.participant_pruning_service.{
  ParticipantPruningServiceGrpc,
  PruneRequest,
  PruneResponse,
}
import com.daml.metrics.Tracked
import com.daml.metrics.api.MetricsContext
import com.daml.scalautil.future.FutureConversion.CompletionStageConversionOps
import com.daml.tracing.Telemetry
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.ParticipantOffsetValidator
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.*
import com.digitalasset.canton.ledger.error.CommonErrors.ServerIsShuttingDown
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.SyncService
import com.digitalasset.canton.ledger.participant.state.index.{
  IndexParticipantPruningService,
  LedgerEndService,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{
  LedgerErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.ApiException
import com.digitalasset.canton.platform.apiserver.services.logging
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.daml.lf.data.Ref
import io.grpc.protobuf.StatusProto
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

final class ApiParticipantPruningService private (
    readBackend: IndexParticipantPruningService with LedgerEndService,
    syncService: SyncService,
    metrics: LedgerApiServerMetrics,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends ParticipantPruningServiceGrpc.ParticipantPruningService
    with GrpcApiService
    with NamedLogging {

  override def bindService(): ServerServiceDefinition =
    ParticipantPruningServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()

  override def prune(request: PruneRequest): Future[PruneResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)

    val submissionIdOrErr = Ref.SubmissionId
      .fromString(
        if (request.submissionId.nonEmpty) request.submissionId else UUID.randomUUID().toString
      )
      .left
      .map(err =>
        invalidArgument(s"submission_id $err")(
          contextualizedErrorLogger(request.submissionId)
        )
      )

    submissionIdOrErr.fold(
      t => Future.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
      submissionId =>
        withEnrichedLoggingContext(logging.submissionId(submissionId)) { implicit loggingContext =>
          implicit val tc: TraceContext = loggingContext.traceContext
          logger.info(
            s"Pruning up to ${request.pruneUpTo}, ${loggingContext.serializeFiltered("submissionId")}."
          )
          (for {

            pruneUpTo <- validateRequest(request)(
              loggingContext,
              contextualizedErrorLogger(submissionId)(loggingContext),
            )

            // If write service pruning succeeds but ledger api server index pruning fails, the user can bring the
            // systems back in sync by reissuing the prune request at the currently specified or later offset.
            _ = logger.debug("Pruning write service")
            _ <- Tracked.future(
              metrics.services.pruning.pruneCommandStarted,
              metrics.services.pruning.pruneCommandCompleted,
              pruneSyncService(pruneUpTo, submissionId, request.pruneAllDivulgedContracts)(
                loggingContext
              ),
            )(MetricsContext(("phase", "underlyingLedger")))

            _ = logger.debug("Getting incomplete reassignments")
            incompleteReassignmentOffsets <- syncService
              .incompleteReassignmentOffsets(
                validAt = pruneUpTo,
                stakeholders = Set.empty, // getting all incomplete reassignments
              )
              .failOnShutdownTo(ServerIsShuttingDown.Reject().asGrpcError)

            _ = logger.debug("Pruning Ledger API Server")
            pruneResponse <- Tracked.future(
              metrics.services.pruning.pruneCommandStarted,
              metrics.services.pruning.pruneCommandCompleted,
              pruneLedgerApiServerIndex(
                pruneUpTo,
                request.pruneAllDivulgedContracts,
                incompleteReassignmentOffsets,
              )(loggingContext),
            )(MetricsContext(("phase", "ledgerApiServerIndex")))

          } yield pruneResponse)
            .thereafter(logger.logErrorsOnCall[PruneResponse](loggingContext.traceContext))
        },
    )
  }

  private def validateRequest(
      request: PruneRequest
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ContextualizedErrorLogger,
  ): Future[Offset] =
    (for {
      _ <- checkOffsetIsSpecified(request.pruneUpTo)
      pruneUpTo <- ParticipantOffsetValidator.validatePositive(request.pruneUpTo, "prune_up_to")
    } yield pruneUpTo)
      .fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
        checkOffsetIsBeforeLedgerEnd,
      )

  private def pruneSyncService(
      pruneUpTo: Offset,
      submissionId: Ref.SubmissionId,
      pruneAllDivulgedContracts: Boolean,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Unit] = {
    import state.PruningResult.*
    logger.info(
      s"About to prune participant ledger up to ${pruneUpTo.unwrap} inclusively starting with the write service."
    )
    syncService
      .prune(pruneUpTo, submissionId, pruneAllDivulgedContracts)
      .toScalaUnwrapped
      .flatMap {
        case NotPruned(status) =>
          Future.failed(new ApiException(StatusProto.toStatusRuntimeException(status)))
        case ParticipantPruned =>
          logger.info(s"Pruned participant ledger up to ${pruneUpTo.unwrap} inclusively.")
          Future.successful(())
      }
  }

  private def pruneLedgerApiServerIndex(
      pruneUpTo: Offset,
      pruneAllDivulgedContracts: Boolean,
      incompletReassignmentOffsets: Vector[Offset],
  )(implicit loggingContext: LoggingContextWithTrace): Future[PruneResponse] = {
    logger.info(s"About to prune ledger api server index to ${pruneUpTo.unwrap} inclusively.")
    readBackend
      .prune(pruneUpTo, pruneAllDivulgedContracts, incompletReassignmentOffsets)
      .map { _ =>
        logger.info(s"Pruned ledger api server index up to ${pruneUpTo.unwrap} inclusively.")
        PruneResponse()
      }
  }

  private def checkOffsetIsSpecified(
      offset: Long
  )(implicit errorLogger: ContextualizedErrorLogger): Either[StatusRuntimeException, Unit] =
    Either.cond(
      offset != 0,
      (),
      invalidArgument("prune_up_to not specified or zero"),
    )

  private def checkOffsetIsBeforeLedgerEnd(
      pruneUpTo: Offset
  )(implicit
      errorLogger: ContextualizedErrorLogger
  ): Future[Offset] =
    for {
      ledgerEnd <- readBackend.currentLedgerEnd()
      _ <-
        if (Option(pruneUpTo) < ledgerEnd) Future.successful(())
        else
          Future.failed(
            RequestValidationErrors.OffsetOutOfRange
              .Reject(
                s"prune_up_to needs to be before ledger end $ledgerEnd"
              )
              .asGrpcError
          )
    } yield pruneUpTo

  private def contextualizedErrorLogger(submissionId: String)(implicit
      loggingContext: LoggingContextWithTrace
  ): ContextualizedErrorLogger =
    LedgerErrorLoggingContext(
      logger,
      loggingContext.toPropertiesMap,
      loggingContext.traceContext,
      submissionId,
    )
}

object ApiParticipantPruningService {
  def createApiService(
      readBackend: IndexParticipantPruningService with LedgerEndService,
      syncService: SyncService,
      metrics: LedgerApiServerMetrics,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): ParticipantPruningServiceGrpc.ParticipantPruningService with GrpcApiService =
    new ApiParticipantPruningService(
      readBackend,
      syncService,
      metrics,
      telemetry,
      loggerFactory,
    )

}
