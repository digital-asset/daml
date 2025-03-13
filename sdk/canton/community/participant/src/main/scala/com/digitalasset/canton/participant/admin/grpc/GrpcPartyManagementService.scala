// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v2.state_service.ActiveContract as LapiActiveContract
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.admin.participant.v30.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.mapErrNewEUS
import com.digitalasset.canton.participant.admin.grpc.GrpcPartyManagementService.ValidExportAcsRequest
import com.digitalasset.canton.participant.admin.party.PartyReplicationAdminWorkflow.PartyReplicationArguments
import com.digitalasset.canton.participant.admin.party.{
  PartyManagementServiceError,
  PartyReplicationAdminWorkflow,
}
import com.digitalasset.canton.participant.store.SyncPersistentState
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils, OptionUtil, ResourceUtil}
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Sink

import java.io.OutputStream
import java.util.zip.GZIPOutputStream
import scala.concurrent.{ExecutionContext, Future}

/** grpc service to allow modifying party hosting on participants
  */
class GrpcPartyManagementService(
    adminWorkflow: PartyReplicationAdminWorkflow,
    processingTimeout: ProcessingTimeout,
    sync: CantonSyncService,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext,
    actorSystem: ActorSystem,
) extends PartyManagementServiceGrpc.PartyManagementService
    with NamedLogging {

  override def addPartyAsync(
      request: AddPartyAsyncRequest
  ): Future[AddPartyAsyncResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    def toStatusRuntimeException(status: Status)(err: String): StatusRuntimeException =
      status.withDescription(err).asRuntimeException()

    EitherTUtil.toFuture(for {
      args <- EitherT.fromEither[Future](
        verifyArguments(request).leftMap(toStatusRuntimeException(Status.INVALID_ARGUMENT))
      )

      hash <- adminWorkflow.partyReplicator
        .addPartyAsync(args, adminWorkflow)
        .leftMap(toStatusRuntimeException(Status.FAILED_PRECONDITION))
        .onShutdown(Left(AbortedDueToShutdown.Error().asGrpcError))
    } yield AddPartyAsyncResponse(partyReplicationId = hash.toHexString))
  }

  private def verifyArguments(
      request: AddPartyAsyncRequest
  ): Either[String, PartyReplicationArguments] =
    for {
      partyId <- convert(request.partyUid, "partyUid", PartyId(_))
      sourceParticipantIdO <- Option
        .when(request.sourceParticipantUid.nonEmpty)(request.sourceParticipantUid)
        .traverse(convert(_, "sourceParticipantUid", ParticipantId(_)))
      synchronizerId <- convert(
        request.synchronizerId,
        "synchronizer_id",
        SynchronizerId(_),
      )
      serialO <- Option
        .when(request.serial != 0)(request.serial)
        .traverse(ProtoConverter.parsePositiveInt("serial", _).leftMap(_.message))

    } yield PartyReplicationArguments(partyId, synchronizerId, sourceParticipantIdO, serialO)

  private def convert[T](
      rawId: String,
      field: String,
      wrap: UniqueIdentifier => T,
  ): Either[String, T] =
    UniqueIdentifier.fromProtoPrimitive(rawId, field).bimap(_.toString, wrap)

  override def exportAcsNew(
      request: ExportAcsNewRequest,
      responseObserver: StreamObserver[ExportAcsNewResponse],
  ): Unit = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => createAcsSnapshot(request, new GZIPOutputStream(out)),
      responseObserver,
      byteString => ExportAcsNewResponse(byteString),
      processingTimeout.unbounded.duration,
      chunkSizeO = None,
    )
  }

  private def createAcsSnapshot(
      request: ExportAcsNewRequest,
      out: OutputStream,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val allSynchronizers: Map[SynchronizerId, SyncPersistentState] =
      sync.syncPersistentStateManager.getAll
    val allSynchronizerIds = allSynchronizers.keySet

    val ledgerEnd = sync.participantNodePersistentState.value.ledgerApiStore.ledgerEndCache
      .apply()
      .map(_.lastOffset)

    val res = for {
      service <- EitherT.fromOption[FutureUnlessShutdown](
        sync.internalStateService,
        PartyManagementServiceError.InternalError.Error("Unavailable internal state service"),
      )
      ledgerEnd <- EitherT.fromOption[FutureUnlessShutdown](
        ledgerEnd,
        PartyManagementServiceError.InternalError.Error("No ledger end found"),
      )
      validRequest <- EitherT.fromEither[FutureUnlessShutdown](
        ValidExportAcsRequest.validateRequest(request, ledgerEnd, allSynchronizerIds)
      )
      lapiActiveContracts <- EitherT
        .liftF[Future, PartyManagementServiceError, Seq[LapiActiveContract]](
          service
            .activeContracts(validRequest.parties, Some(validRequest.offset))
            .map(response => response.getActiveContract)
            .filter(contract =>
              validRequest.filterSynchronizerId
                .forall(filterId => contract.synchronizerId == filterId.toProtoPrimitive)
            )
            .runWith(Sink.seq)
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      activeContracts = lapiActiveContracts.map { c =>
        val contract = if (validRequest.contractSynchronizerRenames.contains(c.synchronizerId)) {
          val synchronizerId =
            validRequest.contractSynchronizerRenames.getOrElse(c.synchronizerId, c.synchronizerId)
          c.copy(synchronizerId = synchronizerId)
        } else { c }
        com.digitalasset.canton.participant.admin.data.ActiveContractNew.tryCreate(contract)
      }

      _ <- EitherT
        .fromEither[FutureUnlessShutdown](
          ResourceUtil
            .withResource(out) { outputStream =>
              activeContracts.traverse(c =>
                c.writeDelimitedTo(outputStream) match {
                  case Left(errorMessage) =>
                    Left(
                      PartyManagementServiceError.InvalidArgument.Error(errorMessage)
                    )
                  case Right(_) =>
                    outputStream.flush()
                    Either.unit
                }
              )
            }
        )
        .leftWiden[PartyManagementServiceError]
    } yield ()

    mapErrNewEUS(res.leftMap(_.toCantonError))
  }
}

object GrpcPartyManagementService {

  private object ValidExportAcsRequest {

    def validateRequest(
        request: ExportAcsNewRequest,
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
          .emptyStringAsNone(request.filterSynchronizerId)
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
          case (source, ExportAcsTargetSynchronizer(target)) =>
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
      parsingResult.leftMap(error =>
        PartyManagementServiceError.InvalidArgument.Error(error.message)
      )
    }
  }

  private final case class ValidExportAcsRequest(
      parties: Set[LfPartyId],
      filterSynchronizerId: Option[SynchronizerId],
      offset: Offset,
      contractSynchronizerRenames: Map[String, String],
  )

}
