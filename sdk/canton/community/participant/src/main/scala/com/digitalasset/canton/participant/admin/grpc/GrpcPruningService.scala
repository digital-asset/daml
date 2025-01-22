// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.implicits.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.admin.grpc.{GrpcPruningScheduler, HasPruningScheduler}
import com.digitalasset.canton.admin.participant.v30.*
import com.digitalasset.canton.admin.pruning.v30
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.PruningServiceErrorGroup
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.{GrpcETFUSExtended, wrapErrUS}
import com.digitalasset.canton.participant.Pruning
import com.digitalasset.canton.participant.scheduler.{
  ParticipantPruningSchedule,
  ParticipantPruningScheduler,
}
import com.digitalasset.canton.participant.sync.{CantonSyncService, SyncPersistentStateManager}
import com.digitalasset.canton.pruning.ConfigForNoWaitCounterParticipants
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.client.IdentityProvidingServiceClient
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

class GrpcPruningService(
    participantId: ParticipantId,
    sync: CantonSyncService,
    pruningScheduler: ParticipantPruningScheduler,
    syncPersistentStateManager: SyncPersistentStateManager,
    ips: IdentityProvidingServiceClient,
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
              Offset
                .fromLong(request.pruneUpTo)
                .leftMap(err =>
                  InvalidArgument
                    .Reject(s"The prune_up_to field (${request.pruneUpTo}) is invalid: $err")
                    .asGrpcError
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
      val validatedRequestE: ParsingResult[(CantonTimestamp, Offset)] = for {
        beforeOrAt <-
          ProtoConverter.parseRequired(
            CantonTimestamp.fromProtoTimestamp,
            "before_or_at",
            request.beforeOrAt,
          )

        ledgerEndOffset <- Offset
          .fromLong(request.ledgerEnd)
          .leftMap(err => ProtoDeserializationError.ValueConversionError("ledger_end", err))
      } yield (beforeOrAt, ledgerEndOffset)

      val res = for {
        validatedRequest <- EitherT.fromEither[FutureUnlessShutdown](
          validatedRequestE.leftMap(err =>
            Status.INVALID_ARGUMENT
              .withDescription(s"Invalid GetSafePruningOffsetRequest: $err")
              .asRuntimeException()
          )
        )

        (beforeOrAt, ledgerEndOffset) = validatedRequest

        safeOffsetO <- sync.pruningProcessor
          .safeToPrune(beforeOrAt, ledgerEndOffset)
          .leftFlatMap[Option[Offset], StatusRuntimeException] {
            case e @ Pruning.LedgerPruningNothingToPrune =>
              // Let the user know that no internal canton data exists prior to the specified
              // time and offset. Return this condition as an error instead of None, so that
              // the caller can distinguish this case from LedgerPruningOffsetUnsafeSynchronizer.
              logger.info(e.message)
              EitherT.leftT(
                PruningServiceError.NoInternalParticipantDataBefore
                  .Error(beforeOrAt, ledgerEndOffset)
                  .asGrpcError
              )
            case e @ Pruning.LedgerPruningOffsetUnsafeSynchronizer(_) =>
              // Turn error indicating that there is no safe pruning offset to a None.
              logger.info(e.message)
              EitherT.rightT(None)
            case error =>
              EitherT.leftT(
                PruningServiceError.InternalServerError.Error(error.message).asGrpcError
              )
          }

      } yield toProtoResponse(safeOffsetO)

      res.asGrpcResponse
  }

  override def setParticipantSchedule(
      request: v30.SetParticipantScheduleRequest
  ): Future[v30.SetParticipantScheduleResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      participantSchedule <- convertRequiredF(
        "participant_schedule",
        request.schedule,
        ParticipantPruningSchedule.fromProtoV30,
      )
      _ <- handlePassiveHAStorageError(
        scheduler.setParticipantSchedule(participantSchedule),
        "set_participant_schedule",
      )
    } yield v30.SetParticipantScheduleResponse()
  }

  override def getParticipantSchedule(
      request: v30.GetParticipantScheduleRequest
  ): Future[v30.GetParticipantScheduleResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      schedule <- scheduler.getParticipantSchedule()
    } yield v30.GetParticipantScheduleResponse(schedule.map(_.toProtoV30))
  }

  private def toProtoResponse(safeOffsetO: Option[Offset]): GetSafePruningOffsetResponse = {

    val response = safeOffsetO
      .fold[GetSafePruningOffsetResponse.Response](
        GetSafePruningOffsetResponse.Response
          .NoSafePruningOffset(GetSafePruningOffsetResponse.NoSafePruningOffset())
      )(offset =>
        GetSafePruningOffsetResponse.Response
          .SafePruningOffset(offset.unwrap)
      )

    GetSafePruningOffsetResponse(response)
  }

  override protected def ensureScheduler(implicit
      traceContext: TraceContext
  ): Future[ParticipantPruningScheduler] =
    Future.successful(pruningScheduler)

  /** Enable or disable waiting for commitments from the given counter-participants
    * Disabling waiting for commitments disregards these counter-participants w.r.t. pruning, which gives up
    * non-repudiation for those counter-participants, but increases pruning resilience to failures
    * and slowdowns of those counter-participants and/or the network
    */
  override def setNoWaitCommitmentsFrom(
      request: v30.SetNoWaitCommitmentsFromRequest
  ): Future[v30.SetNoWaitCommitmentsFromResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val result = for {
      synchronizers <- wrapErrUS(
        request.synchronizerIds.traverse(SynchronizerId.fromProtoPrimitive(_, "synchronizer_id"))
      )
      participants <- wrapErrUS(
        request.counterParticipantUids.traverse(
          ParticipantId.fromProtoPrimitive(_, "counter_participant_uid")
        )
      )

      noWaits = synchronizers.flatMap(dom =>
        participants.map(part => ConfigForNoWaitCounterParticipants(dom, part))
      )
      noWaitDistinct <- EitherT.fromEither[FutureUnlessShutdown](
        if (noWaits.distinct.lengthIs == noWaits.length) Right(noWaits)
        else
          Left(
            PruningServiceError.IllegalArgumentError.Error(
              "Synchronizer Participant pairs is not distinct"
            )
          )
      )
      _ <- EitherTUtil
        .fromFuture(
          sync.pruningProcessor
            .acsSetNoWaitCommitmentsFrom(noWaitDistinct),
          err => PruningServiceError.InternalServerError.Error(err.toString),
        )
        .leftWiden[CantonError]
    } yield {
      v30.SetNoWaitCommitmentsFromResponse()
    }
    CantonGrpcUtil.mapErrNewEUS(result)
  }

  /** Retrieve the configuration of waiting for commitments from counter-participants
    */
  override def getNoWaitCommitmentsFrom(
      request: v30.GetNoWaitCommitmentsFromRequest
  ): Future[v30.GetNoWaitCommitmentsFromResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val result = for {
      synchronizers <- wrapErrUS(
        request.synchronizerIds.traverse(SynchronizerId.fromProtoPrimitive(_, "synchronizer_id"))
      )
      participants <- wrapErrUS(
        request.participantUids.traverse(
          ParticipantId.fromProtoPrimitive(_, "counter_participant_uid")
        )
      )
      noWaitConfig <- EitherTUtil
        .fromFuture(
          sync.pruningProcessor.acsGetNoWaitCommitmentsFrom(synchronizers, participants),
          err => PruningServiceError.InternalServerError.Error(err.toString),
        )
        .leftWiden[CantonError]

      allParticipants <- EitherTUtil
        .fromFuture(
          findAllKnownParticipants(synchronizers, participants),
          err => PruningServiceError.InternalServerError.Error(err.toString),
        )
        .leftWiden[CantonError]

      allParticipantsFiltered = allParticipants
        .map { case (synchronizerId, participants) =>
          val noWaitParticipants =
            noWaitConfig.filter(_.synchronizerId == synchronizerId).collect(_.participantId)
          (synchronizerId, participants.filter(!noWaitParticipants.contains(_)))
        }
    } yield v30.GetNoWaitCommitmentsFromResponse(
      noWaitConfig.map(_.toProtoV30),
      allParticipantsFiltered
        .flatMap { case (synchronizerId, participants) =>
          participants.map(ConfigForNoWaitCounterParticipants(synchronizerId, _))
        }
        .toSeq
        .map(_.toProtoV30),
    )
    CantonGrpcUtil.mapErrNewEUS(result)
  }

  /** Enable waiting for commitments from the given counter-participants
    * Waiting for commitments is the default behavior; explicitly enabling it is useful if it was explicitly disabled
    */
  override def resetNoWaitCommitmentsFrom(
      request: v30.ResetNoWaitCommitmentsFromRequest
  ): Future[v30.ResetNoWaitCommitmentsFromResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val result =
      for {
        synchronizers <- wrapErrUS(
          request.synchronizerIds.traverse(SynchronizerId.fromProtoPrimitive(_, "synchronizer_id"))
        )
        participants <- wrapErrUS(
          request.counterParticipantUids
            .traverse(ParticipantId.fromProtoPrimitive(_, "counter_participant_uid"))
        )
        configs = synchronizers.zip(participants).map { case (synchronizerId, participant) =>
          ConfigForNoWaitCounterParticipants(synchronizerId, participant)
        }
        _ <- EitherTUtil
          .fromFuture(
            sync.pruningProcessor.acsResetNoWaitCommitmentsFrom(configs),
            err => PruningServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

      } yield v30.ResetNoWaitCommitmentsFromResponse()
    CantonGrpcUtil.mapErrNewEUS(result)
  }

  private def findAllKnownParticipants(
      synchronizerFilter: Seq[SynchronizerId],
      participantFilter: Seq[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SynchronizerId, Set[ParticipantId]]] = {
    val result = for {
      (synchronizerId, _) <-
        syncPersistentStateManager.getAll.filter { case (synchronizerId, _) =>
          synchronizerFilter.contains(synchronizerId) || synchronizerFilter.isEmpty
        }
    } yield for {
      _ <- FutureUnlessShutdown.unit
      synchronizerTopoClient = ips.tryForSynchronizer(synchronizerId)
      ipsSnapshot <- synchronizerTopoClient.awaitSnapshot(
        synchronizerTopoClient.approximateTimestamp
      )
      allMembers <- ipsSnapshot.allMembers()
      allParticipants = allMembers
        .filter(_.code == ParticipantId.Code)
        .map(member => ParticipantId.apply(member.uid))
        .excl(participantId)
        .filter(participantFilter.contains(_) || participantFilter.isEmpty)
    } yield (synchronizerId, allParticipants)

    FutureUnlessShutdown.sequence(result).map(_.toMap)
  }
}

sealed trait PruningServiceError extends CantonError
object PruningServiceError extends PruningServiceErrorGroup {

  @Explanation(
    """Pruning is not possible at the specified offset at the current time."""
  )
  @Resolution(
    """Specify a lower offset or retry pruning after a while. Generally, you can only prune
       older events. In particular, the events must be older than the sum of mediator reaction timeout
       and participant timeout for every synchronizer. And, you can only prune events that are older than the
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

  @Explanation(
    """The participant does not hold internal ledger state up to and including the specified time and offset."""
  )
  @Resolution(
    """The participant holds no internal ledger data before or at the time and offset specified as parameters to `find_safe_offset`.
       |Typically this means that the participant has already pruned all internal data up to the specified time and offset.
       |Accordingly this error indicates that no safe offset to prune could be located prior."""
  )
  object NoInternalParticipantDataBefore
      extends ErrorCode(
        id = "NO_INTERNAL_PARTICIPANT_DATA_BEFORE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(beforeOrAt: CantonTimestamp, boundInclusive: Offset)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "No internal participant data to prune up to time " +
            s"$beforeOrAt and offset ${boundInclusive.unwrap}."
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

  @Explanation("""Pruning has failed because of an illegal argument.""")
  @Resolution(
    "Identify the illegal argument in the error details of the gRPC status message that the call returned."
  )
  object IllegalArgumentError
      extends ErrorCode(
        id = "ILLEGAL_ARGUMENT_PRUNING_ERROR",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "The pruning service received an illegal argument: " + reason
        )
        with PruningServiceError
  }

  @Explanation("""Synchronizer purging has been invoked on an unknown synchronizer.""")
  @Resolution("Ensure that the specified synchronizer id exists.")
  object PurgingUnknownSynchronizer
      extends ErrorCode(
        id = "PURGE_UNKNOWN_SYNCHRONIZER_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(synchronizerId: SynchronizerId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = s"Synchronizer $synchronizerId does not exist.")
        with PruningServiceError
  }
}
