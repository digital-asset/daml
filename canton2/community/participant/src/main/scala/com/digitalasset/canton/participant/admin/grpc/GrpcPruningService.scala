// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.admin.grpc.{GrpcPruningScheduler, HasPruningScheduler}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.PruningServiceErrorGroup
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.NonHexOffset
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.participant.admin.v0.*
import com.digitalasset.canton.participant.scheduler.{
  ParticipantPruningSchedule,
  ParticipantPruningScheduler,
}
import com.digitalasset.canton.participant.sync.{CantonSyncService, UpstreamOffsetConvert}
import com.digitalasset.canton.participant.{GlobalOffset, Pruning}
import com.digitalasset.canton.pruning.admin.v0
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

class GrpcPruningService(
    sync: CantonSyncService,
    scheduleAccessorBuilder: () => Option[ParticipantPruningScheduler],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    val ec: ExecutionContext
) extends PruningServiceGrpc.PruningService
    with HasPruningScheduler
    with GrpcPruningScheduler
    with NamedLogging {

  override def prune(request: PruneRequest): Future[PruneResponse] =
    TraceContextGrpc.withGrpcTraceContext { implicit traceContext =>
      EitherTUtil.toFuture {
        for {
          ledgerSyncOffset <-
            EitherT.fromEither[Future](
              UpstreamOffsetConvert
                .toLedgerSyncOffset(request.pruneUpTo)
                .leftMap(err =>
                  NonHexOffset.Error("prune_up_to", request.pruneUpTo, err).asGrpcError
                )
            )
          _ <- CantonGrpcUtil.mapErrNewETUS(sync.pruneInternally(ledgerSyncOffset))
        } yield PruneResponse()
      }
    }

  override def getSafePruningOffset(
      request: GetSafePruningOffsetRequest
  ): Future[GetSafePruningOffsetResponse] = TraceContextGrpc.withGrpcTraceContext {
    implicit traceContext =>
      val validatedRequestE: ParsingResult[(CantonTimestamp, GlobalOffset)] = for {
        beforeOrAt <-
          ProtoConverter.parseRequired(
            CantonTimestamp.fromProtoPrimitive,
            "before_or_at",
            request.beforeOrAt,
          )

        ledgerEndOffset <- UpstreamOffsetConvert
          .toLedgerSyncOffset(request.ledgerEnd)
          .flatMap(UpstreamOffsetConvert.toGlobalOffset)
          .leftMap(err => ProtoDeserializationError.ValueConversionError("ledger_end", err))
      } yield (beforeOrAt, ledgerEndOffset)

      val res = for {
        validatedRequest <- EitherT.fromEither[Future](
          validatedRequestE.leftMap(err =>
            Status.INVALID_ARGUMENT
              .withDescription(s"Invalid GetSafePruningOffsetRequest: $err")
              .asRuntimeException()
          )
        )

        (beforeOrAt, ledgerEndOffset) = validatedRequest

        safeOffsetO <- sync.stateInspection
          .safeToPrune(beforeOrAt, ledgerEndOffset)
          .leftMap {
            case Pruning.LedgerPruningOnlySupportedInEnterpriseEdition =>
              PruningServiceError.PruningNotSupportedInCommunityEdition.Error().asGrpcError
            case error => PruningServiceError.InternalServerError.Error(error.message).asGrpcError
          }

      } yield toProtoResponse(safeOffsetO)

      EitherTUtil.toFuture(res)
  }

  override def setParticipantSchedule(
      request: v0.SetParticipantSchedule.Request
  ): Future[v0.SetParticipantSchedule.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      participantSchedule <- convertRequiredF(
        "participant_schedule",
        request.schedule,
        ParticipantPruningSchedule.fromProtoV0,
      )
      _ <- handlePassiveHAStorageError(
        scheduler.setParticipantSchedule(participantSchedule),
        "set_participant_schedule",
      )
    } yield v0.SetParticipantSchedule.Response()
  }

  override def getParticipantSchedule(
      request: v0.GetParticipantSchedule.Request
  ): Future[v0.GetParticipantSchedule.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      schedule <- scheduler.getParticipantSchedule()
    } yield v0.GetParticipantSchedule.Response(schedule.map(_.toProtoV0))
  }

  private def toProtoResponse(safeOffsetO: Option[GlobalOffset]): GetSafePruningOffsetResponse = {

    val response = safeOffsetO
      .fold[GetSafePruningOffsetResponse.Response](
        GetSafePruningOffsetResponse.Response
          .NoSafePruningOffset(GetSafePruningOffsetResponse.NoSafePruningOffset())
      )(offset =>
        GetSafePruningOffsetResponse.Response
          .SafePruningOffset(UpstreamOffsetConvert.fromGlobalOffset(offset).toHexString)
      )

    GetSafePruningOffsetResponse(response)
  }

  private lazy val maybeScheduleAccessor: Option[ParticipantPruningScheduler] =
    scheduleAccessorBuilder()

  override protected def ensureScheduler(implicit
      traceContext: TraceContext
  ): Future[ParticipantPruningScheduler] =
    maybeScheduleAccessor match {
      case None =>
        Future.failed(
          PruningServiceError.PruningNotSupportedInCommunityEdition.Error().asGrpcError
        )
      case Some(scheduler) => Future.successful(scheduler)
    }
}

sealed trait PruningServiceError extends CantonError
object PruningServiceError extends PruningServiceErrorGroup {

  @Explanation("""The supplied offset has an unexpected lengths.""")
  @Resolution(
    "Ensure the offset has originated from this participant and is 9 bytes in length."
  )
  object NonCantonOffset
      extends ErrorCode(id = "NON_CANTON_OFFSET", ErrorCategory.InvalidIndependentOfSystemState) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Offset length does not match ledger standard of 9 bytes"
        )
        with PruningServiceError
  }

  @Explanation("""Pruning is not supported in the Community Edition.""")
  @Resolution("Upgrade to the Enterprise Edition.")
  object PruningNotSupportedInCommunityEdition
      extends ErrorCode(
        id = "PRUNING_NOT_SUPPORTED_IN_COMMUNITY_EDITION",
        // TODO(#5990) According to the WriteParticipantPruningService, this should give the status code UNIMPLEMENTED. Introduce a new error category for that!
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error()(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Pruning is only supported in the Enterprise Edition"
        )
        with PruningServiceError
  }

  @Explanation(
    """Pruning is not possible at the specified offset at the current time."""
  )
  @Resolution(
    """Specify a lower offset or retry pruning after a while. Generally, you can only prune
       older events. In particular, the events must be older than the sum of mediator reaction timeout
       and participant timeout for every domain. And, you can only prune events that are older than the
       deduplication time configured for this participant.
       Therefore, if you observe this error, you either just prune older events or you adjust the settings
       for this participant.
       The error details field `safe_offset` contains the highest offset that can currently be pruned, if any.
      """
  )
  object UnsafeToPrune
      extends ErrorCode(id = "UNSAFE_TO_PRUNE", ErrorCategory.InvalidGivenCurrentSystemStateOther) {
    final case class Error(_cause: String, reason: String, safe_offset: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Participant cannot prune at specified offset due to ${_cause}"
        )
        with PruningServiceError
  }

  @Explanation("""Pruning has been aborted because the participant is shutting down.""")
  @Resolution(
    """After the participant is restarted, the participant ensures that it is in a consistent state.
      |Therefore no intervention is necessary. After the restart, pruning can be invoked again as usual to
      |prune the participant up to the desired offset."""
  )
  object ParticipantShuttingDown
      extends ErrorCode(
        id = "SHUTDOWN_INTERRUPTED_PRUNING",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error()(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Participant has been pruned only partially due to shutdown."
        )
        with PruningServiceError
  }

  @Explanation("""Pruning has failed because of an internal server error.""")
  @Resolution("Identify the error in the server log.")
  object InternalServerError
      extends ErrorCode(
        id = "INTERNAL_PRUNING_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Internal error such as the inability to write to the database"
        )
        with PruningServiceError
  }

}
