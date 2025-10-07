// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.all.*
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction as LapiTopologyTransaction
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.admin.participant.v30.*
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.{InternalIndexService, SynchronizerIndex}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.mapErrNewEUS
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.data.{
  ContractImportMode,
  RepresentativePackageIdOverride,
}
import com.digitalasset.canton.participant.admin.party.PartyReplicationAdminWorkflow.PartyReplicationArguments
import com.digitalasset.canton.participant.admin.party.{
  PartyManagementServiceError,
  PartyOnboardingCompletion,
  PartyParticipantPermission,
  PartyReplicationAdminWorkflow,
}
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SynchronizerOffset
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{
  ParticipantId,
  PartyId,
  SynchronizerId,
  SynchronizerTopologyManager,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterAsyncOps
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils}
import com.google.protobuf.ByteString
import com.google.protobuf.duration.Duration
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Sink

import java.io.{ByteArrayOutputStream, OutputStream}
import java.util.UUID
import java.util.zip.GZIPOutputStream
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/** grpc service to allow modifying party hosting on participants
  */
class GrpcPartyManagementService(
    adminWorkflowO: Option[PartyReplicationAdminWorkflow],
    processingTimeout: ProcessingTimeout,
    sync: CantonSyncService,
    parameters: ParticipantNodeParameters,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
) extends v30.PartyManagementServiceGrpc.PartyManagementService
    with NamedLogging {

  private val batching: BatchingConfig = parameters.batchingConfig

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
      ValidPartyReplicationCommonRequestParams(
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

      snapshot <- EitherT.right(
        client.awaitSnapshot(activationTimestamp.immediateSuccessor)
      )
      // TODO(#28208) - Indirection because LAPI topology transaction does not include the onboarding flag
      activeParticipants <- EitherT.right(
        snapshot.activeParticipantsOf(party.toLf)
      )
      _ <-
        EitherT.cond[FutureUnlessShutdown](
          activeParticipants.exists { case (participantId, participantAttributes) =>
            participantId == targetParticipant &&
            participantAttributes.onboarding
          },
          (),
          PartyManagementServiceError.InvalidState
            .AbortAcsExportForMissingOnboardingFlag(
              party,
              targetParticipant,
            ): PartyManagementServiceError,
        )

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

      _ <- ParticipantCommon
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
    } yield ()

    mapErrNewEUS(res.leftMap(_.toCantonRpcError))
  }

  private def validateExportPartyAcsRequest(
      request: v30.ExportPartyAcsRequest,
      ledgerEnd: Offset,
      synchronizerIds: Set[SynchronizerId],
  )(implicit
      elc: ErrorLoggingContext
  ): EitherT[
    FutureUnlessShutdown,
    PartyManagementServiceError,
    ValidPartyReplicationCommonRequestParams,
  ] =
    validatePartyReplicationCommonRequestParams(
      request.partyId,
      request.synchronizerId,
      request.targetParticipantUid,
      request.beginOffsetExclusive,
      request.waitForActivationTimeout,
    )(ledgerEnd, synchronizerIds)

  private def validatePartyReplicationCommonRequestParams(
      partyId: String,
      synchronizerId: String,
      targetParticipantUid: String,
      beginOffsetExclusive: Long,
      waitForActivationTimeout: Option[Duration],
  )(
      ledgerEnd: Offset,
      synchronizerIds: Set[SynchronizerId],
  )(implicit
      elc: ErrorLoggingContext
  ): EitherT[
    FutureUnlessShutdown,
    PartyManagementServiceError,
    ValidPartyReplicationCommonRequestParams,
  ] = {
    val parsingResult = for {
      party <- UniqueIdentifier
        .fromProtoPrimitive(partyId, "party_id")
        .map(PartyId(_))
      parsedSynchronizerId <- SynchronizerId.fromProtoPrimitive(
        synchronizerId,
        "synchronizer_id",
      )
      synchronizerId <- Either.cond(
        synchronizerIds.contains(parsedSynchronizerId),
        parsedSynchronizerId,
        OtherError(s"Synchronizer ID $parsedSynchronizerId is unknown"),
      )
      targetParticipantId <- UniqueIdentifier
        .fromProtoPrimitive(
          targetParticipantUid,
          "target_participant_uid",
        )
        .map(ParticipantId(_))
      parsedBeginOffsetExclusive <- ProtoConverter
        .parseOffset("begin_offset_exclusive", beginOffsetExclusive)
      beginOffsetExclusive <- Either.cond(
        parsedBeginOffsetExclusive <= ledgerEnd,
        parsedBeginOffsetExclusive,
        OtherError(
          s"Begin ledger offset $parsedBeginOffsetExclusive needs to be smaller or equal to the ledger end $ledgerEnd"
        ),
      )
      waitForActivationTimeout <- waitForActivationTimeout.traverse(
        NonNegativeFiniteDuration.fromProtoPrimitive("wait_for_activation_timeout")(_)
      )
    } yield ValidPartyReplicationCommonRequestParams(
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

  // TODO(#24065) - There may be multiple party on- and offboarding transactions which may break this method
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

  private def findTopologyServices(
      synchronizerId: SynchronizerId,
      sync: CantonSyncService,
  ): Either[
    String,
    (SynchronizerTopologyManager, TopologyStore[SynchronizerStore], SynchronizerTimeTracker),
  ] =
    for {
      psid <- sync.syncPersistentStateManager
        .latestKnownPSId(synchronizerId)
        .toRight(s"Undefined physical synchronizer ID for given $synchronizerId")
      topologyStore <- sync.syncPersistentStateManager
        .get(psid)
        .map(_.topologyStore)
        .toRight("Cannot get topology store")
      topologyManager <- sync.syncPersistentStateManager
        .get(psid)
        .map(_.topologyManager)
        .toRight("Cannot get topology manager")
      timeTracker <- sync.lookupSynchronizerTimeTracker(synchronizerId)
    } yield (topologyManager, topologyStore, timeTracker)

  /*
 Note that `responseObserver` originates from `GrpcStreamingUtils.streamToServer` which is
 a wrapper that turns the responses into a promise/future. This is not a true bidirectional stream.
   */
  override def importPartyAcs(
      responseObserver: StreamObserver[ImportPartyAcsResponse]
  ): StreamObserver[ImportPartyAcsRequest] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    // TODO(#23818): This buffer will contain the whole ACS snapshot.
    val outputStream = new ByteArrayOutputStream()

    new StreamObserver[ImportPartyAcsRequest] {

      override def onNext(request: ImportPartyAcsRequest): Unit =
        outputStream.write(request.acsSnapshot.toByteArray)

      override def onError(t: Throwable): Unit =
        try {
          outputStream.close()
        } finally {
          responseObserver.onError(t)
        }

      override def onCompleted(): Unit = {
        // Synchronously try to get the snapshot and start the import
        val importFuture =
          try {
            ParticipantCommon.importAcsNewSnapshot(
              acsSnapshot = ByteString.copyFrom(outputStream.toByteArray),
              batching = batching,
              contractImportMode = ContractImportMode.Validation,
              excludedStakeholders = Set.empty,
              loggerFactory = loggerFactory,
              // TODO(#27872): Consider allowing package-id overrides for party imports
              representativePackageIdOverride = RepresentativePackageIdOverride.NoOverride,
              sync = sync,
              workflowIdPrefix = s"import-party-acs-${UUID.randomUUID}",
            )
          } catch {
            // If toByteArray or importAcsNewSnapshot fails
            case NonFatal(e) => Future.failed(e)
          }

        importFuture
          .thereafter { _ =>
            outputStream.close()
          }
          .onComplete {
            case Failure(exception) =>
              responseObserver.onError(exception)
            case Success(_) =>
              responseObserver.onNext(ImportPartyAcsResponse())
              responseObserver.onCompleted()
          }
      }
    }
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

  override def completePartyOnboarding(
      request: v30.CompletePartyOnboardingRequest
  ): Future[v30.CompletePartyOnboardingResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      r <- processPartyOnboardingRequest(request)
      (onboarded, safeTime) = r
    } yield v30.CompletePartyOnboardingResponse(onboarded, safeTime.map(x => x.toProtoTimestamp))
    mapErrNewEUS(res.leftMap(_.toCantonRpcError))
  }

  private def processPartyOnboardingRequest(
      request: v30.CompletePartyOnboardingRequest
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    PartyManagementServiceError,
    (Boolean, Option[CantonTimestamp]),
  ] =
    for {
      ledgerEnd <- EitherT
        .fromEither[FutureUnlessShutdown](ParticipantCommon.findLedgerEnd(sync))
        .leftMap(PartyManagementServiceError.InvalidState.Error(_))
      allLogicalSynchronizerIds = sync.syncPersistentStateManager.getAllLatest.keySet

      validRequest <- validateCompletePartyOnboardingRequest(
        request,
        ledgerEnd,
        allLogicalSynchronizerIds,
      )
      ValidPartyReplicationCommonRequestParams(
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

      (_activationOffset, activationTimestamp) = extractOffsetAndTimestamp(topologyTx)

      client <- EitherT
        .fromEither[FutureUnlessShutdown](findTopologyClient(synchronizerId, sync))
        .leftMap(PartyManagementServiceError.InvalidState.Error(_))

      snapshot <- EitherT.right(
        client.awaitSnapshot(activationTimestamp.immediateSuccessor)
      )
      // TODO(#28208) - Indirection because LAPI topology transaction does not include the onboarding flag
      activeParticipants <- EitherT.right(
        snapshot.activeParticipantsOf(party.toLf)
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        activeParticipants.exists { case (participantId, participantAttributes) =>
          participantId == targetParticipant &&
          participantAttributes.onboarding
        },
        (),
        PartyManagementServiceError.InvalidState.MissingOnboardingFlagCannotCompleteOnboarding(
          party,
          targetParticipant,
        ): PartyManagementServiceError,
      )

      topoServices <- EitherT
        .fromEither[FutureUnlessShutdown](findTopologyServices(synchronizerId, sync))
        .leftMap(PartyManagementServiceError.InvalidState.Error(_): PartyManagementServiceError)

      (topologyManager, topologyStore, timeTracker) = topoServices

      onboarding = new PartyOnboardingCompletion(
        party,
        synchronizerId,
        targetParticipant,
        timeTracker,
        topologyManager,
        topologyStore,
        client,
        loggerFactory,
      )

      onboardingCompletionOutcome <- onboarding
        .attemptCompletion(activationTimestamp)
        .leftMap(PartyManagementServiceError.InvalidState.Error(_): PartyManagementServiceError)

    } yield (onboardingCompletionOutcome)

  private def validateCompletePartyOnboardingRequest(
      request: v30.CompletePartyOnboardingRequest,
      ledgerEnd: Offset,
      synchronizerIds: Set[SynchronizerId],
  )(implicit
      elc: ErrorLoggingContext
  ): EitherT[
    FutureUnlessShutdown,
    PartyManagementServiceError,
    ValidPartyReplicationCommonRequestParams,
  ] =
    validatePartyReplicationCommonRequestParams(
      request.partyId,
      request.synchronizerId,
      request.targetParticipantUid,
      request.beginOffsetExclusive,
      request.waitForActivationTimeout,
    )(ledgerEnd, synchronizerIds)

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

private final case class ValidPartyReplicationCommonRequestParams(
    party: PartyId,
    synchronizerId: SynchronizerId,
    targetParticipant: ParticipantId,
    beginOffsetExclusive: Offset,
    waitForActivationTimeout: Option[NonNegativeFiniteDuration],
)
