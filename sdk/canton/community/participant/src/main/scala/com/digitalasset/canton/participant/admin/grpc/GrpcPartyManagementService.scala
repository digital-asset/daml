// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.mapErrNewEUS
import com.digitalasset.canton.participant.admin.data.ActiveContract as ActiveContractValueClass
import com.digitalasset.canton.participant.admin.party.PartyReplicationAdminWorkflow.PartyReplicationArguments
import com.digitalasset.canton.participant.admin.party.{
  PartyManagementServiceError,
  PartyParticipantPermission,
  PartyReplicationAdminWorkflow,
}
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SynchronizerOffset
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils, OptionUtil, ResourceUtil}
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}
import org.apache.pekko.actor.ActorSystem

import java.io.OutputStream
import java.util.zip.GZIPOutputStream
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** grpc service to allow modifying party hosting on participants
  */
class GrpcPartyManagementService(
    adminWorkflowO: Option[PartyReplicationAdminWorkflow],
    processingTimeout: ProcessingTimeout,
    sync: CantonSyncService,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext,
    actorSystem: ActorSystem,
) extends v30.PartyManagementServiceGrpc.PartyManagementService
    with NamedLogging {

  override def addPartyAsync(
      request: v30.AddPartyAsyncRequest
  ): Future[v30.AddPartyAsyncResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    EitherTUtil.toFuture(for {
      adminWorkflow <- EitherT.fromEither[Future](
        ensureAdminWorkflowIfOnlinePartyReplicationEnabled()
      )

      args <- EitherT.fromEither[Future](
        verifyArguments(request).leftMap(toStatusRuntimeException(Status.INVALID_ARGUMENT))
      )

      hash <- adminWorkflow.partyReplicator
        .addPartyAsync(args, adminWorkflow)
        .leftMap(toStatusRuntimeException(Status.FAILED_PRECONDITION))
        .onShutdown(Left(AbortedDueToShutdown.Error().asGrpcError))
    } yield v30.AddPartyAsyncResponse(addPartyRequestId = hash.toHexString))
  }

  private def verifyArguments(
      request: v30.AddPartyAsyncRequest
  ): Either[String, PartyReplicationArguments] =
    for {
      partyId <- convert(request.partyId, "party_id", PartyId(_))
      sourceParticipantId <- convert(
        request.sourceParticipantUid,
        "source_participant_uid",
        ParticipantId(_),
      )
      synchronizerId <- convert(
        request.synchronizerId,
        "synchronizer_id",
        SynchronizerId(_),
      )
      serial <- ProtoConverter
        .parsePositiveInt("topology_serial", request.topologySerial)
        .leftMap(_.message)
      participantPermission <- ProtoConverter
        .parseEnum[ParticipantPermission, v30.ParticipantPermission](
          PartyParticipantPermission.fromProtoV30,
          "participant_permission",
          request.participantPermission,
        )
        .leftMap(_.message)
    } yield PartyReplicationArguments(
      partyId,
      synchronizerId,
      sourceParticipantId,
      serial,
      participantPermission,
    )

  private def convert[T](
      rawId: String,
      field: String,
      wrap: UniqueIdentifier => T,
  ): Either[String, T] =
    UniqueIdentifier.fromProtoPrimitive(rawId, field).bimap(_.toString, wrap)

  override def getAddPartyStatus(
      request: v30.GetAddPartyStatusRequest
  ): Future[v30.GetAddPartyStatusResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    (for {
      adminWorkflow <- ensureAdminWorkflowIfOnlinePartyReplicationEnabled()

      requestId <- Hash
        .fromHexString(request.addPartyRequestId)
        .leftMap(err => toStatusRuntimeException(Status.INVALID_ARGUMENT)(err.message))

      status <- adminWorkflow.partyReplicator
        .getAddPartyStatus(requestId)
        .toRight(
          toStatusRuntimeException(Status.UNKNOWN)(
            s"Add party request id ${request.addPartyRequestId} not found"
          )
        )
    } yield {
      val statusP = v30.GetAddPartyStatusResponse.Status(status.toProto)
      v30.GetAddPartyStatusResponse(
        partyId = status.params.partyId.toProtoPrimitive,
        synchronizerId = status.params.synchronizerId.toProtoPrimitive,
        sourceParticipantUid = status.params.sourceParticipantId.uid.toProtoPrimitive,
        targetParticipantUid = status.params.targetParticipantId.uid.toProtoPrimitive,
        topologySerial = status.params.serial.unwrap,
        participantPermission =
          PartyParticipantPermission.toProtoPrimitive(status.params.participantPermission),
        status = Some(statusP),
      )
    })
      .fold(Future.failed, Future.successful)
  }

  private def toStatusRuntimeException(status: Status)(err: String): StatusRuntimeException =
    status.withDescription(err).asRuntimeException()

  private def ensureAdminWorkflowIfOnlinePartyReplicationEnabled() = (adminWorkflowO match {
    case Some(value) => Right(value)
    case None =>
      Left(
        "The add_party_async command requires the `unsafe_online_party_replication` configuration"
      )
  })
    .leftMap(toStatusRuntimeException(Status.UNIMPLEMENTED))

  override def exportAcs(
      request: v30.ExportAcsRequest,
      responseObserver: StreamObserver[v30.ExportAcsResponse],
  ): Unit = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => processExportAcsAtOffset(request, new GZIPOutputStream(out)),
      responseObserver,
      byteString => v30.ExportAcsResponse(byteString),
      processingTimeout.unbounded.duration,
      chunkSizeO = None,
    )
  }

  private def processExportAcsAtOffset(
      request: v30.ExportAcsRequest,
      out: OutputStream,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val allLogicalSynchronizerIds = sync.syncPersistentStateManager.getAllLatest.keySet

    val ledgerEnd = sync.participantNodePersistentState.value.ledgerApiStore.ledgerEndCache
      .apply()
      .map(_.lastOffset)

    val res = for {
      ledgerEnd <- EitherT.fromOption[FutureUnlessShutdown](
        ledgerEnd,
        PartyManagementServiceError.InternalError.Error("No ledger end found"),
      )
      validRequest <- EitherT.fromEither[FutureUnlessShutdown](
        validateExportAcsAtOffsetRequest(request, ledgerEnd, allLogicalSynchronizerIds)
      )
      snapshotResult <- createAcsSnapshot(validRequest, out)
    } yield snapshotResult

    mapErrNewEUS(res.leftMap(_.toCantonRpcError))
  }

  private def validateExportAcsAtOffsetRequest(
      request: v30.ExportAcsRequest,
      ledgerEnd: Offset,
      synchronizerIds: Set[SynchronizerId],
  )(implicit
      elc: ErrorLoggingContext
  ): Either[PartyManagementServiceError, ValidExportAcsRequest] = {
    val parsingResult = for {
      parties <- request.partyIds.traverse(party =>
        UniqueIdentifier.fromProtoPrimitive(party, "party_ids").map(PartyId(_).toLf)
      )
      parsedFilterSynchronizerId <- OptionUtil
        .emptyStringAsNone(request.synchronizerId)
        .traverse(SynchronizerId.fromProtoPrimitive(_, "filter_synchronizer_id"))
      filterSynchronizerId <- Either.cond(
        parsedFilterSynchronizerId.forall(synchronizerIds.contains),
        parsedFilterSynchronizerId,
        OtherError(s"Filter synchronizer id $parsedFilterSynchronizerId is unknown"),
      )
      parsedOffset <- ProtoConverter
        .parsePositiveLong("ledger_offset", request.ledgerOffset)
      offset <- Offset.fromLong(parsedOffset.unwrap).leftMap(OtherError.apply)
      ledgerOffset <- Either.cond(
        offset <= ledgerEnd,
        offset,
        OtherError(
          s"Ledger offset $offset needs to be smaller or equal to the ledger end $ledgerEnd"
        ),
      )
      contractSynchronizerRenames <- request.contractSynchronizerRenames.toList.traverse {
        case (source, v30.ExportAcsTargetSynchronizer(target)) =>
          for {
            _ <- SynchronizerId.fromProtoPrimitive(source, "source synchronizer id")
            _ <- SynchronizerId.fromProtoPrimitive(target, "target synchronizer id")
          } yield (source, target)
      }
    } yield ValidExportAcsRequest(
      parties.toSet,
      filterSynchronizerId,
      ledgerOffset,
      contractSynchronizerRenames.toMap,
    )
    parsingResult.leftMap(error => PartyManagementServiceError.InvalidArgument.Error(error.message))
  }

  private def createAcsSnapshot(
      request: ValidExportAcsRequest,
      out: OutputStream,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, PartyManagementServiceError, Unit] =
    for {
      service <- EitherT.fromOption[FutureUnlessShutdown](
        sync.internalStateService,
        PartyManagementServiceError.InternalError.Error("Unavailable internal state service"),
      )
      _ <- EitherT
        .apply[Future, PartyManagementServiceError, Unit](
          ResourceUtil.withResourceFuture(out)(out =>
            service
              .activeContracts(request.parties, Some(request.offset))
              .map(response => response.getActiveContract)
              .filter(contract =>
                request.filterSynchronizerId
                  .forall(filterId => contract.synchronizerId == filterId.toProtoPrimitive)
              )
              .map { contract =>
                if (request.contractSynchronizerRenames.contains(contract.synchronizerId)) {
                  val synchronizerId = request.contractSynchronizerRenames
                    .getOrElse(contract.synchronizerId, contract.synchronizerId)
                  contract.copy(synchronizerId = synchronizerId)
                } else {
                  contract
                }
              }
              .map(ActiveContractValueClass.tryCreate)
              .map {
                _.writeDelimitedTo(out) match {
                  // throwing intentionally to immediately interrupt any further Pekko source stream processing
                  case Left(errorMessage) => throw new RuntimeException(errorMessage)
                  case Right(_) => out.flush()
                }
              }
              .run()
              .transform {
                case Failure(e) =>
                  Success(Left(PartyManagementServiceError.IOStream.Error(e.getMessage)))
                case Success(_) => Success(Right(()))
              }
          )
        )
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield ()

  override def exportAcsAtTimestamp(
      request: v30.ExportAcsAtTimestampRequest,
      responseObserver: StreamObserver[v30.ExportAcsAtTimestampResponse],
  ): Unit = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => processExportAcsAtTimestamp(request, new GZIPOutputStream(out)),
      responseObserver,
      byteString => v30.ExportAcsAtTimestampResponse(byteString),
      processingTimeout.unbounded.duration,
      chunkSizeO = None,
    )
  }

  private def processExportAcsAtTimestamp(
      request: v30.ExportAcsAtTimestampRequest,
      out: OutputStream,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val res = for {
      validRequest <- validateExportAcsAtTimestampRequest(request)
      snapshotResult <- createAcsSnapshot(validRequest, out)
    } yield snapshotResult

    mapErrNewEUS(res.leftMap(_.toCantonRpcError))
  }

  private def validateExportAcsAtTimestampRequest(
      request: v30.ExportAcsAtTimestampRequest
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, PartyManagementServiceError, ValidExportAcsRequest] = {

    final case class ParsedRequest(
        parties: Set[LfPartyId],
        synchronizerId: SynchronizerId,
        topologyTransactionEffectiveTime: CantonTimestamp,
    )

    def parseRequest(
        request: v30.ExportAcsAtTimestampRequest
    ): ParsingResult[ParsedRequest] =
      for {
        parties <- request.partyIds.traverse(party =>
          UniqueIdentifier.fromProtoPrimitive(party, "party_ids").map(PartyId(_).toLf)
        )
        synchronizerId <- SynchronizerId.fromProtoPrimitive(
          request.synchronizerId,
          "synchronizer_id",
        )
        topologyTxEffectiveTime <- ProtoConverter.parseRequired(
          CantonTimestamp.fromProtoTimestamp,
          "topology_transaction_effective_time",
          request.topologyTransactionEffectiveTime,
        )
      } yield ParsedRequest(
        parties.toSet,
        synchronizerId,
        topologyTxEffectiveTime,
      )

    val allSynchronizerIds = sync.syncPersistentStateManager.allKnownLSIds

    for {
      parsedRequest <- EitherT.fromEither[FutureUnlessShutdown](
        parseRequest(request).leftMap(error =>
          PartyManagementServiceError.InvalidArgument.Error(error.message)
        )
      )

      synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        Either.cond(
          allSynchronizerIds.contains(parsedRequest.synchronizerId),
          parsedRequest.synchronizerId,
          PartyManagementServiceError.InvalidArgument.Error(
            s"Synchronizer id ${parsedRequest.synchronizerId} for ACS export is unknown"
          ),
        )
      )

      topologyTransactionEffectiveOffset <- EitherT
        .fromOptionF[FutureUnlessShutdown, PartyManagementServiceError, Offset](
          FutureUnlessShutdown.outcomeF(
            sync.participantNodePersistentState.value.ledgerApiStore
              .topologyEventOffsetPublishedOnRecordTime(
                synchronizerId,
                parsedRequest.topologyTransactionEffectiveTime,
              )
          ),
          PartyManagementServiceError.InvalidAcsSnapshotTimestamp
            .Error(parsedRequest.topologyTransactionEffectiveTime, synchronizerId),
        )

    } yield ValidExportAcsRequest(
      parsedRequest.parties,
      Some(synchronizerId),
      topologyTransactionEffectiveOffset,
      Map.empty,
    )
  }

  override def getHighestOffsetByTimestamp(
      request: v30.GetHighestOffsetByTimestampRequest
  ): Future[v30.GetHighestOffsetByTimestampResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val allSynchronizerIds = sync.syncPersistentStateManager.getAllLatest.keySet

    val res = for {
      synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        SynchronizerId
          .fromProtoPrimitive(
            request.synchronizerId,
            "synchronizer_id",
          )
          .leftMap(error => PartyManagementServiceError.InvalidArgument.Error(error.message))
      )
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        Either.cond(
          allSynchronizerIds.contains(synchronizerId),
          (),
          PartyManagementServiceError.InvalidArgument.Error(
            s"Synchronizer id ${synchronizerId.uid} is unknown"
          ),
        )
      )
      timestamp <- EitherT.fromEither[FutureUnlessShutdown](
        ProtoConverter
          .parseRequired(
            CantonTimestamp.fromProtoTimestamp,
            "timestamp",
            request.timestamp,
          )
          .leftMap(error => PartyManagementServiceError.InvalidArgument.Error(error.message))
      )
      forceFlag = request.force
      cleanSynchronizerIndex <- EitherT
        .fromOptionF[FutureUnlessShutdown, PartyManagementServiceError, SynchronizerIndex](
          sync.participantNodePersistentState.value.ledgerApiStore
            .cleanSynchronizerIndex(synchronizerId),
          PartyManagementServiceError.InvalidTimestamp
            .Error(
              synchronizerId,
              timestamp,
              forceFlag,
              "Cannot use clean synchronizer index because it is empty",
            ),
        )
      // Retrieve the ledger end offset for potential use in force mode. Do so before retrieving the synchronizer
      // offset to prevent a race condition of the ledger end being bumped after the synchronizer offset retrieval.
      ledgerEnd <- EitherT.fromOption[FutureUnlessShutdown](
        sync.participantNodePersistentState.value.ledgerApiStore.ledgerEndCache.apply(),
        PartyManagementServiceError.InvalidTimestamp
          .Error(
            synchronizerId,
            timestamp,
            forceFlag,
            "Cannot find the ledger end",
          ),
      )
      synchronizerOffsetBeforeOrAtRequestedTimestamp <- EitherT
        .fromOptionF[FutureUnlessShutdown, PartyManagementServiceError, SynchronizerOffset](
          sync.participantNodePersistentState.value.ledgerApiStore
            .lastSynchronizerOffsetBeforeOrAtRecordTime(
              synchronizerId,
              timestamp,
            ),
          PartyManagementServiceError.InvalidTimestamp
            .Error(
              synchronizerId,
              timestamp,
              forceFlag,
              s"The participant does not yet have a ledger offset before or at the requested timestamp: $timestamp",
            ),
        )
      offset <- EitherT.fromEither[FutureUnlessShutdown](
        GrpcPartyManagementService.identifyHighestOffsetByTimestamp(
          requestedTimestamp = timestamp,
          synchronizerOffsetBeforeOrAtRequestedTimestamp =
            synchronizerOffsetBeforeOrAtRequestedTimestamp,
          forceFlag = forceFlag,
          cleanSynchronizerTimestamp = cleanSynchronizerIndex.recordTime,
          ledgerEnd = ledgerEnd,
          synchronizerId = synchronizerId,
        )
      )
    } yield v30.GetHighestOffsetByTimestampResponse(offset.unwrap)
    mapErrNewEUS(res.leftMap(_.toCantonRpcError))
  }

}

object GrpcPartyManagementService {

  /** OffPR getHighestOffsetByTimestamp computation of offset from timestamp placed in a pure
    * function for unit testing.
    */
  def identifyHighestOffsetByTimestamp(
      requestedTimestamp: CantonTimestamp,
      synchronizerOffsetBeforeOrAtRequestedTimestamp: SynchronizerOffset,
      forceFlag: Boolean,
      cleanSynchronizerTimestamp: CantonTimestamp,
      ledgerEnd: LedgerEnd,
      synchronizerId: SynchronizerId,
  )(implicit
      elc: ErrorLoggingContext
  ): Either[PartyManagementServiceError, Offset] = {
    val synchronizerTimestampBeforeOrAtRequestedTimestamp =
      CantonTimestamp(synchronizerOffsetBeforeOrAtRequestedTimestamp.recordTime)
    for {
      _ <- Either.cond(
        synchronizerTimestampBeforeOrAtRequestedTimestamp <= requestedTimestamp,
        (),
        PartyManagementServiceError.InvalidTimestamp.Error(
          synchronizerId,
          requestedTimestamp,
          forceFlag,
          s"Coding bug: Returned offset record time $synchronizerTimestampBeforeOrAtRequestedTimestamp must be before or at the requested timestamp $requestedTimestamp.",
        ),
      )
      _ <- Either.cond(
        forceFlag || requestedTimestamp <= cleanSynchronizerTimestamp,
        (),
        PartyManagementServiceError.InvalidTimestamp.Error(
          synchronizerId,
          requestedTimestamp,
          forceFlag,
          s"Not all events have been processed fully and/or published to the Ledger API DB until the requested timestamp: $requestedTimestamp",
        ),
      )
      offsetBeforeOrAtRequestedTimestamp <-
        // Use the ledger end offset only if the requested timestamp is at least
        // the clean synchronizer timestamp which caps the ledger end offset.
        if (forceFlag && requestedTimestamp >= cleanSynchronizerTimestamp)
          ledgerEnd.lastOffset.asRight[PartyManagementServiceError]
        else {
          // Sanity check that the synchronizer offset is less than or equal to the ledger end offset.
          Either.cond(
            synchronizerOffsetBeforeOrAtRequestedTimestamp.offset <= ledgerEnd.lastOffset,
            synchronizerOffsetBeforeOrAtRequestedTimestamp.offset,
            PartyManagementServiceError.InvalidTimestamp.Error(
              synchronizerId,
              requestedTimestamp,
              forceFlag,
              s"The synchronizer offset ${synchronizerOffsetBeforeOrAtRequestedTimestamp.offset} is not less than or equal to the ledger end offset ${ledgerEnd.lastOffset}",
            ),
          )
        }
    } yield offsetBeforeOrAtRequestedTimestamp
  }
}

private final case class ValidExportAcsRequest(
    parties: Set[LfPartyId],
    filterSynchronizerId: Option[SynchronizerId],
    offset: Offset,
    contractSynchronizerRenames: Map[String, String],
)
