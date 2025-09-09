// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.implicits.toTraverseOps
import cats.syntax.either.*
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction as LapiTopologyTransaction
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.{InternalIndexService, SynchronizerIndex}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.mapErrNewEUS
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
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils}
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Sink

import java.io.OutputStream
import java.util.zip.GZIPOutputStream
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

/** grpc service to allow modifying party hosting on participants
  */
class GrpcPartyManagementService(
    adminWorkflowO: Option[PartyReplicationAdminWorkflow],
    processingTimeout: ProcessingTimeout,
    sync: CantonSyncService,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContextExecutor,
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
  ): Future[v30.GetAddPartyStatusResponse] =
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

  override def exportPartyAcs(
      request: v30.ExportPartyAcsRequest,
      responseObserver: StreamObserver[v30.ExportPartyAcsResponse],
  ): Unit = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => processExportPartyAcsRequest(request, new GZIPOutputStream(out)),
      responseObserver,
      byteString => v30.ExportPartyAcsResponse(byteString),
      processingTimeout.unbounded.duration,
      chunkSizeO = None,
    )
  }

  private def processExportPartyAcsRequest(
      request: v30.ExportPartyAcsRequest,
      out: OutputStream,
  )(implicit traceContext: TraceContext): Future[Unit] = {

    val res = for {
      ledgerEnd <- EitherT
        .fromEither[FutureUnlessShutdown](ParticipantCommon.findLedgerEnd(sync))
        .leftMap(PartyManagementServiceError.InvalidState.Error(_))
      allLogicalSynchronizerIds = sync.syncPersistentStateManager.getAllLatest.keySet

      validRequest <- validateExportPartyAcsRequest(request, ledgerEnd, allLogicalSynchronizerIds)
      ValidExportPartyAcsRequest(
        party,
        synchronizerId,
        targetParticipant,
        beginOffsetExclusive,
        waitForActivationTimeout,
      ) = validRequest

      indexService <- EitherT.fromOption[FutureUnlessShutdown](
        sync.internalIndexService,
        PartyManagementServiceError.InvalidState.Error("Unavailable internal index service"),
      )

      topologyTx <-
        findSinglePartyActivationTopologyTransaction(
          indexService,
          party,
          beginOffsetExclusive,
          synchronizerId,
          targetParticipant,
          waitForActivationTimeout,
        )

      (activationOffset, activationTimestamp) = extractOffsetAndTimestamp(topologyTx)

      client <- EitherT
        .fromEither[FutureUnlessShutdown](findTopologyClient(synchronizerId, sync))
        .leftMap(PartyManagementServiceError.InvalidState.Error(_))

      partiesHostedByTargetParticipant <- EitherT.right(
        client
          .awaitSnapshot(activationTimestamp)
          .flatMap(snapshot =>
            snapshot.inspectKnownParties(
              filterParty = "",
              filterParticipant = targetParticipant.uid.toProtoPrimitive,
            )
          )
      )

      otherPartiesHostedByTargetParticipant =
        partiesHostedByTargetParticipant excl party excl targetParticipant.adminParty

      snapshot <- ParticipantCommon
        .writeAcsSnapshot(
          indexService,
          Set(party),
          atOffset = activationOffset,
          out,
          excludedStakeholders = otherPartiesHostedByTargetParticipant,
          Some(synchronizerId),
        )(ec, traceContext, actorSystem)
        .leftMap(msg =>
          PartyManagementServiceError.IOStream.Error(msg): PartyManagementServiceError
        )
    } yield snapshot

    mapErrNewEUS(res.leftMap(_.toCantonRpcError))
  }

  private def validateExportPartyAcsRequest(
      request: v30.ExportPartyAcsRequest,
      ledgerEnd: Offset,
      synchronizerIds: Set[SynchronizerId],
  )(implicit
      elc: ErrorLoggingContext
  ): EitherT[FutureUnlessShutdown, PartyManagementServiceError, ValidExportPartyAcsRequest] = {
    val parsingResult = for {
      party <- UniqueIdentifier
        .fromProtoPrimitive(request.partyId, "party_id")
        .map(PartyId(_))
      parsedSynchronizerId <- SynchronizerId.fromProtoPrimitive(
        request.synchronizerId,
        "synchronizer_id",
      )
      synchronizerId <- Either.cond(
        synchronizerIds.contains(parsedSynchronizerId),
        parsedSynchronizerId,
        OtherError(s"Synchronizer ID $parsedSynchronizerId is unknown"),
      )
      targetParticipantId <- UniqueIdentifier
        .fromProtoPrimitive(
          request.targetParticipantUid,
          "target_participant_uid",
        )
        .map(ParticipantId(_))
      parsedBeginOffsetExclusive <- ProtoConverter
        .parseOffset("begin_offset_exclusive", request.beginOffsetExclusive)
      beginOffsetExclusive <- Either.cond(
        parsedBeginOffsetExclusive <= ledgerEnd,
        parsedBeginOffsetExclusive,
        OtherError(
          s"Begin ledger offset $parsedBeginOffsetExclusive needs to be smaller or equal to the ledger end $ledgerEnd"
        ),
      )
      waitForActivationTimeout <- request.waitForActivationTimeout.traverse(
        NonNegativeFiniteDuration.fromProtoPrimitive("wait_for_activation_timeout")(_)
      )
    } yield ValidExportPartyAcsRequest(
      party,
      synchronizerId,
      targetParticipantId,
      beginOffsetExclusive,
      waitForActivationTimeout,
    )
    EitherT.fromEither[FutureUnlessShutdown](
      parsingResult.leftMap(error =>
        PartyManagementServiceError.InvalidArgument.Error(error.message)
      )
    )
  }

  private def findSinglePartyActivationTopologyTransaction(
      indexService: InternalIndexService,
      party: PartyId,
      beginOffsetExclusive: Offset,
      synchronizerId: SynchronizerId,
      targetParticipant: ParticipantId,
      waitForActivationTimeout: Option[NonNegativeFiniteDuration],
  )(implicit
      ec: ExecutionContextExecutor,
      traceContext: TraceContext,
      actorSystem: ActorSystem,
  ): EitherT[FutureUnlessShutdown, PartyManagementServiceError, LapiTopologyTransaction] =
    for {
      topologyTx <- EitherT
        .apply[Future, PartyManagementServiceError, LapiTopologyTransaction](
          indexService
            .topologyTransactions(party.toLf, beginOffsetExclusive)
            .filter(_.synchronizerId == synchronizerId.toProtoPrimitive)
            .filter { topologyTransaction =>
              topologyTransaction.events.exists { event =>
                event.event.isParticipantAuthorizationAdded &&
                event.getParticipantAuthorizationAdded.participantId == targetParticipant.uid.toProtoPrimitive
              }
            }
            .take(1)
            .completionTimeout(
              waitForActivationTimeout.getOrElse(NonNegativeFiniteDuration.tryOfMinutes(2)).toScala
            )
            .runWith(Sink.head)
            .transform {
              case Success(tx) => Success(Right(tx))
              case Failure(e) =>
                val message = s"${e.getMessage} – Possibly missing party activation?"
                Success(Left(PartyManagementServiceError.IOStream.Error(message)))
            }
        )
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield topologyTx

  private def extractOffsetAndTimestamp(
      topologyTransaction: LapiTopologyTransaction
  ): (Offset, CantonTimestamp) = (for {
    offset <- ProtoConverter.parseOffset("offset", topologyTransaction.offset)
    effectiveTime <- ProtoConverter.parseRequired(
      CantonTimestamp.fromProtoTimestamp,
      "record_time",
      topologyTransaction.recordTime,
    )
  } yield (offset, effectiveTime)).valueOr(error => throw new IllegalStateException(error.message))

  private def findTopologyClient(
      synchronizerId: SynchronizerId,
      sync: CantonSyncService,
  ): Either[String, SynchronizerTopologyClientWithInit] =
    for {
      psid <- sync.syncPersistentStateManager
        .latestKnownPSId(synchronizerId)
        .toRight(s"Undefined physical synchronizer ID for given $synchronizerId")
      topoClient <- sync.lookupTopologyClient(psid).toRight("Absent topology client")
    } yield topoClient

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

private final case class ValidExportPartyAcsRequest(
    party: PartyId,
    synchronizerId: SynchronizerId,
    targetParticipant: ParticipantId,
    beginOffsetExclusive: Offset,
    waitForActivationTimeout: Option[NonNegativeFiniteDuration],
)
