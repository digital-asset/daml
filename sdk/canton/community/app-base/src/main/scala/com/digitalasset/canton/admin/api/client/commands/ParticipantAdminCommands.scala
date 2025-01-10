// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.implicits.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  ServerEnforcedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.api.client.data.{
  DarMetadata,
  InFlightCount,
  ListConnectedSynchronizersResult,
  NodeStatus,
  ParticipantPruningSchedule,
  ParticipantStatus,
}
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.admin.participant.v30.EnterpriseParticipantReplicationServiceGrpc.EnterpriseParticipantReplicationServiceStub
import com.digitalasset.canton.admin.participant.v30.InspectionServiceGrpc.InspectionServiceStub
import com.digitalasset.canton.admin.participant.v30.PackageServiceGrpc.PackageServiceStub
import com.digitalasset.canton.admin.participant.v30.ParticipantRepairServiceGrpc.ParticipantRepairServiceStub
import com.digitalasset.canton.admin.participant.v30.ParticipantStatusServiceGrpc.ParticipantStatusServiceStub
import com.digitalasset.canton.admin.participant.v30.PartyManagementServiceGrpc.PartyManagementServiceStub
import com.digitalasset.canton.admin.participant.v30.PingServiceGrpc.PingServiceStub
import com.digitalasset.canton.admin.participant.v30.PruningServiceGrpc.PruningServiceStub
import com.digitalasset.canton.admin.participant.v30.ResourceManagementServiceGrpc.ResourceManagementServiceStub
import com.digitalasset.canton.admin.participant.v30.SynchronizerConnectivityServiceGrpc.SynchronizerConnectivityServiceStub
import com.digitalasset.canton.admin.participant.v30.{ResourceLimits as _, *}
import com.digitalasset.canton.admin.pruning
import com.digitalasset.canton.admin.pruning.v30.WaitCommitmentsSetup
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.admin.ResourceLimits
import com.digitalasset.canton.participant.admin.traffic.TrafficStateAdmin
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.{
  ReceivedCmtState,
  SentCmtState,
}
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.protocol.messages.{AcsCommitment, CommitmentPeriod}
import com.digitalasset.canton.sequencing.SequencerConnectionValidation
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{BinaryFileUtil, GrpcStreamingUtils}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{SequencerCounter, SynchronizerAlias, config}
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver
import io.grpc.{Context, ManagedChannel}

import java.io.IOException
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, MILLISECONDS}

object ParticipantAdminCommands {

  /** Daml Package Management Commands
    */
  object Package {

    sealed trait PackageCommand[Req, Res, Result] extends GrpcAdminCommand[Req, Res, Result] {
      override type Svc = PackageServiceStub

      override def createService(channel: ManagedChannel): PackageServiceStub =
        PackageServiceGrpc.stub(channel)
    }

    final case class List(limit: PositiveInt)
        extends PackageCommand[ListPackagesRequest, ListPackagesResponse, Seq[PackageDescription]] {
      override protected def createRequest() = Right(ListPackagesRequest(limit.value))

      override protected def submitRequest(
          service: PackageServiceStub,
          request: ListPackagesRequest,
      ): Future[ListPackagesResponse] =
        service.listPackages(request)

      override protected def handleResponse(
          response: ListPackagesResponse
      ): Either[String, Seq[PackageDescription]] =
        Right(response.packageDescriptions)
    }

    final case class ListContents(packageId: String)
        extends PackageCommand[ListPackageContentsRequest, ListPackageContentsResponse, Seq[
          ModuleDescription
        ]] {
      override protected def createRequest() = Right(ListPackageContentsRequest(packageId))

      override protected def submitRequest(
          service: PackageServiceStub,
          request: ListPackageContentsRequest,
      ): Future[ListPackageContentsResponse] =
        service.listPackageContents(request)

      override protected def handleResponse(
          response: ListPackageContentsResponse
      ): Either[String, Seq[ModuleDescription]] =
        Right(response.modules)
    }

    final case class UploadDar(
        darPath: Option[String],
        vetAllPackages: Boolean,
        synchronizeVetting: Boolean,
        logger: TracedLogger,
    ) extends PackageCommand[UploadDarRequest, UploadDarResponse, String] {

      override protected def createRequest(): Either[String, UploadDarRequest] =
        for {
          pathValue <- darPath.toRight("DAR path not provided")
          nonEmptyPathValue <- Either.cond(
            pathValue.nonEmpty,
            pathValue,
            "Provided DAR path is empty",
          )
          filename = Paths.get(nonEmptyPathValue).getFileName.toString
          darData <- BinaryFileUtil.readByteStringFromFile(nonEmptyPathValue)
        } yield UploadDarRequest(
          darData,
          filename,
          vetAllPackages = vetAllPackages,
          synchronizeVetting = synchronizeVetting,
        )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: UploadDarRequest,
      ): Future[UploadDarResponse] =
        service.uploadDar(request)

      override protected def handleResponse(response: UploadDarResponse): Either[String, String] =
        response.value match {
          case UploadDarResponse.Value.Success(UploadDarResponse.Success(hash)) => Right(hash)
          case UploadDarResponse.Value.Failure(UploadDarResponse.Failure(msg)) => Left(msg)
          case UploadDarResponse.Value.Empty => Left("unexpected empty response")
        }

      // file can be big. checking & vetting might take a while
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ValidateDar(
        darPath: Option[String],
        logger: TracedLogger,
    ) extends PackageCommand[ValidateDarRequest, ValidateDarResponse, String] {

      override protected def createRequest(): Either[String, ValidateDarRequest] =
        for {
          pathValue <- darPath.toRight("DAR path not provided")
          nonEmptyPathValue <- Either.cond(
            pathValue.nonEmpty,
            pathValue,
            "Provided DAR path is empty",
          )
          filename = Paths.get(nonEmptyPathValue).getFileName.toString
          darData <- BinaryFileUtil.readByteStringFromFile(nonEmptyPathValue)
        } yield ValidateDarRequest(
          darData,
          filename,
        )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: ValidateDarRequest,
      ): Future[ValidateDarResponse] =
        service.validateDar(request)

      override protected def handleResponse(response: ValidateDarResponse): Either[String, String] =
        response match {
          case ValidateDarResponse(hash) => Right(hash)
        }

      // file can be big. checking & vetting might take a while
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class RemovePackage(
        packageId: String,
        force: Boolean,
    ) extends PackageCommand[RemovePackageRequest, RemovePackageResponse, Unit] {

      override protected def createRequest() = Right(RemovePackageRequest(packageId, force))

      override protected def submitRequest(
          service: PackageServiceStub,
          request: RemovePackageRequest,
      ): Future[RemovePackageResponse] =
        service.removePackage(request)

      override protected def handleResponse(
          response: RemovePackageResponse
      ): Either[String, Unit] =
        response.success match {
          case None => Left("unexpected empty response")
          case Some(_success) => Right(())
        }

    }

    final case class GetDar(
        darHash: Option[String],
        destinationDirectory: Option[String],
        logger: TracedLogger,
    ) extends PackageCommand[GetDarRequest, GetDarResponse, Path] {
      override protected def createRequest(): Either[String, GetDarRequest] =
        for {
          _ <- destinationDirectory.toRight("DAR destination directory not provided")
          hash <- darHash.toRight("DAR hash not provided")
        } yield GetDarRequest(hash)

      override protected def submitRequest(
          service: PackageServiceStub,
          request: GetDarRequest,
      ): Future[GetDarResponse] =
        service.getDar(request)

      override protected def handleResponse(response: GetDarResponse): Either[String, Path] =
        for {
          directory <- destinationDirectory.toRight("DAR directory not provided")
          data <- if (response.data.isEmpty) Left("DAR was not found") else Right(response.data)
          path <-
            try {
              val path = Paths.get(directory, s"${response.name}.dar")
              Files.write(path, data.toByteArray)
              Right(path)
            } catch {
              case ex: IOException =>
                // the trace context for admin commands is started by the submit-request call
                // however we can't get at it here
                logger.debug(s"Error saving DAR to $directory: $ex")(TraceContext.empty)
                Left(s"Error saving DAR to $directory")
            }
        } yield path

      // might be a big file to download
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ListDarContents(darId: String)
        extends PackageCommand[ListDarContentsRequest, ListDarContentsResponse, DarMetadata] {
      override protected def createRequest() = Right(ListDarContentsRequest(darId))

      override protected def submitRequest(
          service: PackageServiceStub,
          request: ListDarContentsRequest,
      ): Future[ListDarContentsResponse] =
        service.listDarContents(request)

      override protected def handleResponse(
          response: ListDarContentsResponse
      ): Either[String, DarMetadata] =
        DarMetadata.fromProtoV30(response).leftMap(_.toString)

    }

    final case class RemoveDar(
        darHash: String
    ) extends PackageCommand[RemoveDarRequest, RemoveDarResponse, Unit] {

      override protected def createRequest(): Either[String, RemoveDarRequest] = Right(
        RemoveDarRequest(darHash)
      )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: RemoveDarRequest,
      ): Future[RemoveDarResponse] =
        service.removeDar(request)

      override protected def handleResponse(
          response: RemoveDarResponse
      ): Either[String, Unit] =
        response.success match {
          case None => Left("unexpected empty response")
          case Some(success) => Right(())
        }

    }

    final case class VetDar(darDash: String, synchronize: Boolean)
        extends PackageCommand[VetDarRequest, VetDarResponse, Unit] {
      override protected def createRequest(): Either[String, VetDarRequest] = Right(
        VetDarRequest(darDash, synchronize)
      )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: VetDarRequest,
      ): Future[VetDarResponse] = service.vetDar(request)

      override protected def handleResponse(response: VetDarResponse): Either[String, Unit] =
        Either.unit
    }

    // TODO(#14432): Add `synchronize` flag which makes the call block until the unvetting operation
    //               is observed by the participant on all connected synchronizers.
    final case class UnvetDar(darDash: String)
        extends PackageCommand[UnvetDarRequest, UnvetDarResponse, Unit] {

      override protected def createRequest(): Either[String, UnvetDarRequest] = Right(
        UnvetDarRequest(darDash)
      )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: UnvetDarRequest,
      ): Future[UnvetDarResponse] = service.unvetDar(request)

      override protected def handleResponse(response: UnvetDarResponse): Either[String, Unit] =
        Either.unit
    }

    final case class ListDars(limit: PositiveInt)
        extends PackageCommand[ListDarsRequest, ListDarsResponse, Seq[DarDescription]] {
      override protected def createRequest(): Either[String, ListDarsRequest] = Right(
        ListDarsRequest(limit.value)
      )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: ListDarsRequest,
      ): Future[ListDarsResponse] =
        service.listDars(request)

      override protected def handleResponse(
          response: ListDarsResponse
      ): Either[String, Seq[DarDescription]] =
        Right(response.dars)
    }

  }

  object PartyManagement {

    final case class StartPartyReplication(
        id: Option[String],
        party: PartyId,
        sourceParticipant: ParticipantId,
        synchronizerId: SynchronizerId,
    ) extends GrpcAdminCommand[StartPartyReplicationRequest, StartPartyReplicationResponse, Unit] {
      override type Svc = PartyManagementServiceStub

      override def createService(channel: ManagedChannel): PartyManagementServiceStub =
        PartyManagementServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, StartPartyReplicationRequest] =
        Right(
          StartPartyReplicationRequest(
            id = id,
            partyUid = party.uid.toProtoPrimitive,
            sourceParticipantUid = sourceParticipant.uid.toProtoPrimitive,
            synchronizerId = synchronizerId.toProtoPrimitive,
          )
        )

      override protected def submitRequest(
          service: PartyManagementServiceStub,
          request: StartPartyReplicationRequest,
      ): Future[StartPartyReplicationResponse] =
        service.startPartyReplication(request)

      override protected def handleResponse(
          response: StartPartyReplicationResponse
      ): Either[String, Unit] =
        Either.unit
    }
  }

  object ParticipantRepairManagement {

    final case class ExportAcs(
        parties: Set[PartyId],
        partiesOffboarding: Boolean,
        filterSynchronizerId: Option[SynchronizerId],
        timestamp: Option[Instant],
        observer: StreamObserver[ExportAcsResponse],
        contractDomainRenames: Map[SynchronizerId, (SynchronizerId, ProtocolVersion)],
        force: Boolean,
    ) extends GrpcAdminCommand[
          ExportAcsRequest,
          CancellableContext,
          CancellableContext,
        ] {

      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, ExportAcsRequest] =
        Right(
          ExportAcsRequest(
            parties.map(_.toLf).toSeq,
            filterSynchronizerId.map(_.toProtoPrimitive).getOrElse(""),
            timestamp.map(Timestamp.apply),
            contractDomainRenames.map {
              case (source, (targetSynchronizerId, targetProtocolVersion)) =>
                val targetSynchronizer = ExportAcsRequest.TargetSynchronizer(
                  synchronizerId = targetSynchronizerId.toProtoPrimitive,
                  protocolVersion = targetProtocolVersion.toProtoPrimitive,
                )

                (source.toProtoPrimitive, targetSynchronizer)
            },
            force = force,
            partiesOffboarding = partiesOffboarding,
          )
        )

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: ExportAcsRequest,
      ): Future[CancellableContext] = {
        val context = Context.current().withCancellation()
        context.run(() => service.exportAcs(request, observer))
        Future.successful(context)
      }

      override protected def handleResponse(
          response: CancellableContext
      ): Either[String, CancellableContext] = Right(response)

      override def timeoutType: GrpcAdminCommand.TimeoutType =
        GrpcAdminCommand.DefaultUnboundedTimeout
    }

    final case class ImportAcs(
        acsChunk: ByteString,
        workflowIdPrefix: String,
        allowContractIdSuffixRecomputation: Boolean,
    ) extends GrpcAdminCommand[
          ImportAcsRequest,
          ImportAcsResponse,
          Map[LfContractId, LfContractId],
        ] {

      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, ImportAcsRequest] =
        Right(
          ImportAcsRequest(
            acsChunk,
            workflowIdPrefix,
            allowContractIdSuffixRecomputation,
          )
        )

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: ImportAcsRequest,
      ): Future[ImportAcsResponse] =
        GrpcStreamingUtils.streamToServer(
          service.importAcs,
          (bytes: Array[Byte]) =>
            ImportAcsRequest(
              ByteString.copyFrom(bytes),
              workflowIdPrefix,
              allowContractIdSuffixRecomputation,
            ),
          request.acsSnapshot,
        )

      override protected def handleResponse(
          response: ImportAcsResponse
      ): Either[String, Map[LfContractId, LfContractId]] =
        response.contractIdMapping.toSeq
          .traverse { case (oldCid, newCid) =>
            for {
              oldCidParsed <- LfContractId.fromString(oldCid)
              newCidParsed <- LfContractId.fromString(newCid)
            } yield oldCidParsed -> newCidParsed
          }
          .map(_.toMap)
    }

    final case class PurgeContracts(
        synchronizerAlias: SynchronizerAlias,
        contracts: Seq[LfContractId],
        ignoreAlreadyPurged: Boolean,
    ) extends GrpcAdminCommand[PurgeContractsRequest, PurgeContractsResponse, Unit] {

      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, PurgeContractsRequest] =
        Right(
          PurgeContractsRequest(
            synchronizerAlias = synchronizerAlias.toProtoPrimitive,
            contractIds = contracts.map(_.coid),
            ignoreAlreadyPurged = ignoreAlreadyPurged,
          )
        )

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: PurgeContractsRequest,
      ): Future[PurgeContractsResponse] = service.purgeContracts(request)

      override protected def handleResponse(
          response: PurgeContractsResponse
      ): Either[String, Unit] =
        Either.unit
    }

    final case class MigrateSynchronizer(
        sourceSynchronizerAlias: SynchronizerAlias,
        targetSynchronizerConfig: SynchronizerConnectionConfig,
        force: Boolean,
    ) extends GrpcAdminCommand[MigrateSynchronizerRequest, MigrateSynchronizerResponse, Unit] {
      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: MigrateSynchronizerRequest,
      ): Future[MigrateSynchronizerResponse] = service.migrateSynchronizer(request)

      override protected def createRequest(): Either[String, MigrateSynchronizerRequest] =
        Right(
          MigrateSynchronizerRequest(
            sourceSynchronizerAlias.toProtoPrimitive,
            Some(targetSynchronizerConfig.toProtoV30),
            force = force,
          )
        )

      override protected def handleResponse(
          response: MigrateSynchronizerResponse
      ): Either[String, Unit] =
        Either.unit

      // migration command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    final case class PurgeDeactivatedSynchronizer(synchronizerAlias: SynchronizerAlias)
        extends GrpcAdminCommand[
          PurgeDeactivatedSynchronizerRequest,
          PurgeDeactivatedSynchronizerResponse,
          Unit,
        ] {
      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: PurgeDeactivatedSynchronizerRequest,
      ): Future[PurgeDeactivatedSynchronizerResponse] =
        service.purgeDeactivatedSynchronizer(request)

      override protected def createRequest(): Either[String, PurgeDeactivatedSynchronizerRequest] =
        Right(PurgeDeactivatedSynchronizerRequest(synchronizerAlias.toProtoPrimitive))

      override protected def handleResponse(
          response: PurgeDeactivatedSynchronizerResponse
      ): Either[String, Unit] = Either.unit
    }

    final case class IgnoreEvents(
        synchronizerId: SynchronizerId,
        fromInclusive: SequencerCounter,
        toInclusive: SequencerCounter,
        force: Boolean,
    ) extends GrpcAdminCommand[
          IgnoreEventsRequest,
          IgnoreEventsResponse,
          Unit,
        ] {
      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: IgnoreEventsRequest,
      ): Future[IgnoreEventsResponse] =
        service.ignoreEvents(request)

      override protected def createRequest(): Either[String, IgnoreEventsRequest] =
        Right(
          IgnoreEventsRequest(
            synchronizerId = synchronizerId.toProtoPrimitive,
            fromInclusive = fromInclusive.toProtoPrimitive,
            toInclusive = toInclusive.toProtoPrimitive,
            force = force,
          )
        )

      override protected def handleResponse(response: IgnoreEventsResponse): Either[String, Unit] =
        Either.unit
    }

    final case class UnignoreEvents(
        synchronizerId: SynchronizerId,
        fromInclusive: SequencerCounter,
        toInclusive: SequencerCounter,
        force: Boolean,
    ) extends GrpcAdminCommand[
          UnignoreEventsRequest,
          UnignoreEventsResponse,
          Unit,
        ] {
      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: UnignoreEventsRequest,
      ): Future[UnignoreEventsResponse] =
        service.unignoreEvents(request)

      override protected def createRequest(): Either[String, UnignoreEventsRequest] =
        Right(
          UnignoreEventsRequest(
            synchronizerId = synchronizerId.toProtoPrimitive,
            fromInclusive = fromInclusive.toProtoPrimitive,
            toInclusive = toInclusive.toProtoPrimitive,
            force = force,
          )
        )

      override protected def handleResponse(
          response: UnignoreEventsResponse
      ): Either[String, Unit] = Either.unit
    }

    final case class RollbackUnassignment(
        unassignId: String,
        source: SynchronizerId,
        target: SynchronizerId,
    ) extends GrpcAdminCommand[RollbackUnassignmentRequest, RollbackUnassignmentResponse, Unit] {
      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, RollbackUnassignmentRequest] =
        Right(
          RollbackUnassignmentRequest(
            unassignId = unassignId,
            sourceSynchronizerId = source.toProtoPrimitive,
            targetSynchronizerId = target.toProtoPrimitive,
          )
        )

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: RollbackUnassignmentRequest,
      ): Future[RollbackUnassignmentResponse] =
        service.rollbackUnassignment(request)

      override protected def handleResponse(
          response: RollbackUnassignmentResponse
      ): Either[String, Unit] = Either.unit
    }
  }

  object Ping {

    final case class Ping(
        targets: Set[String],
        validators: Set[String],
        timeout: config.NonNegativeDuration,
        levels: Int,
        synchronizerId: Option[SynchronizerId],
        workflowId: String,
        id: String,
    ) extends GrpcAdminCommand[PingRequest, PingResponse, Either[String, Duration]] {
      override type Svc = PingServiceStub

      override def createService(channel: ManagedChannel): PingServiceStub =
        PingServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, PingRequest] =
        Right(
          PingRequest(
            targets.toSeq,
            validators.toSeq,
            Some(timeout.toProtoPrimitive),
            levels,
            synchronizerId.map(_.toProtoPrimitive).getOrElse(""),
            workflowId,
            id,
          )
        )

      override protected def submitRequest(
          service: PingServiceStub,
          request: PingRequest,
      ): Future[PingResponse] =
        service.ping(request)

      override protected def handleResponse(
          response: PingResponse
      ): Either[String, Either[String, Duration]] =
        response.response match {
          case PingResponse.Response.Success(PingSuccess(pingTime, responder)) =>
            Right(Right(Duration(pingTime, MILLISECONDS)))
          case PingResponse.Response.Failure(failure) => Right(Left(failure.reason))
          case PingResponse.Response.Empty => Left("Ping client: unexpected empty response")
        }

      override def timeoutType: TimeoutType = ServerEnforcedTimeout
    }

  }

  object SynchronizerConnectivity {

    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = SynchronizerConnectivityServiceStub

      override def createService(channel: ManagedChannel): SynchronizerConnectivityServiceStub =
        SynchronizerConnectivityServiceGrpc.stub(channel)
    }

    final case class ReconnectSynchronizers(ignoreFailures: Boolean)
        extends Base[ReconnectSynchronizersRequest, ReconnectSynchronizersResponse, Unit] {

      override protected def createRequest(): Either[String, ReconnectSynchronizersRequest] =
        Right(ReconnectSynchronizersRequest(ignoreFailures = ignoreFailures))

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: ReconnectSynchronizersRequest,
      ): Future[ReconnectSynchronizersResponse] =
        service.reconnectSynchronizers(request)

      override protected def handleResponse(
          response: ReconnectSynchronizersResponse
      ): Either[String, Unit] = Either.unit

      // depending on the confirmation timeout and the load, this might take a bit longer
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ReconnectSynchronizer(synchronizerAlias: SynchronizerAlias, retry: Boolean)
        extends Base[ReconnectSynchronizerRequest, ReconnectSynchronizerResponse, Boolean] {

      override protected def createRequest(): Either[String, ReconnectSynchronizerRequest] =
        Right(ReconnectSynchronizerRequest(synchronizerAlias.toProtoPrimitive, retry))

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: ReconnectSynchronizerRequest,
      ): Future[ReconnectSynchronizerResponse] =
        service.reconnectSynchronizer(request)

      override protected def handleResponse(
          response: ReconnectSynchronizerResponse
      ): Either[String, Boolean] =
        Right(response.connectedSuccessfully)

      // can take long if we need to wait to become active
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class GetSynchronizerId(synchronizerAlias: SynchronizerAlias)
        extends Base[GetSynchronizerIdRequest, GetSynchronizerIdResponse, SynchronizerId] {

      override protected def createRequest(): Either[String, GetSynchronizerIdRequest] =
        Right(GetSynchronizerIdRequest(synchronizerAlias.toProtoPrimitive))

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: GetSynchronizerIdRequest,
      ): Future[GetSynchronizerIdResponse] =
        service.getSynchronizerId(request)

      override protected def handleResponse(
          response: GetSynchronizerIdResponse
      ): Either[String, SynchronizerId] =
        SynchronizerId
          .fromProtoPrimitive(response.synchronizerId, "synchronizer_id")
          .leftMap(_.toString)
    }

    final case class DisconnectSynchronizer(synchronizerAlias: SynchronizerAlias)
        extends Base[DisconnectSynchronizerRequest, DisconnectSynchronizerResponse, Unit] {

      override protected def createRequest(): Either[String, DisconnectSynchronizerRequest] =
        Right(DisconnectSynchronizerRequest(synchronizerAlias.toProtoPrimitive))

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: DisconnectSynchronizerRequest,
      ): Future[DisconnectSynchronizerResponse] =
        service.disconnectSynchronizer(request)

      override protected def handleResponse(
          response: DisconnectSynchronizerResponse
      ): Either[String, Unit] = Either.unit
    }

    final case class DisconnectAllSynchronizers()
        extends Base[DisconnectAllSynchronizersRequest, DisconnectAllSynchronizersResponse, Unit] {

      override protected def createRequest(): Either[String, DisconnectAllSynchronizersRequest] =
        Right(DisconnectAllSynchronizersRequest())

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: DisconnectAllSynchronizersRequest,
      ): Future[DisconnectAllSynchronizersResponse] =
        service.disconnectAllSynchronizers(request)

      override protected def handleResponse(
          response: DisconnectAllSynchronizersResponse
      ): Either[String, Unit] = Either.unit
    }

    final case class ListConnectedSynchronizers()
        extends Base[ListConnectedSynchronizersRequest, ListConnectedSynchronizersResponse, Seq[
          ListConnectedSynchronizersResult
        ]] {

      override protected def createRequest(): Either[String, ListConnectedSynchronizersRequest] =
        Right(
          ListConnectedSynchronizersRequest()
        )

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: ListConnectedSynchronizersRequest,
      ): Future[ListConnectedSynchronizersResponse] =
        service.listConnectedSynchronizers(request)

      override protected def handleResponse(
          response: ListConnectedSynchronizersResponse
      ): Either[String, Seq[ListConnectedSynchronizersResult]] =
        response.connectedSynchronizers.traverse(
          ListConnectedSynchronizersResult.fromProtoV30(_).leftMap(_.toString)
        )

    }

    final case object ListRegisteredSynchronizers
        extends Base[ListRegisteredSynchronizersRequest, ListRegisteredSynchronizersResponse, Seq[
          (SynchronizerConnectionConfig, Boolean)
        ]] {

      override protected def createRequest(): Either[String, ListRegisteredSynchronizersRequest] =
        Right(
          ListRegisteredSynchronizersRequest()
        )

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: ListRegisteredSynchronizersRequest,
      ): Future[ListRegisteredSynchronizersResponse] =
        service.listRegisteredSynchronizers(request)

      override protected def handleResponse(
          response: ListRegisteredSynchronizersResponse
      ): Either[String, Seq[(SynchronizerConnectionConfig, Boolean)]] = {

        def mapRes(
            result: ListRegisteredSynchronizersResponse.Result
        ): Either[String, (SynchronizerConnectionConfig, Boolean)] =
          for {
            configP <- result.config.toRight("Server has sent empty config")
            config <- SynchronizerConnectionConfig.fromProtoV30(configP).leftMap(_.toString)
          } yield (config, result.connected)

        response.results.traverse(mapRes)
      }
    }

    final case class ConnectSynchronizer(
        config: SynchronizerConnectionConfig,
        sequencerConnectionValidation: SequencerConnectionValidation,
    ) extends Base[ConnectSynchronizerRequest, ConnectSynchronizerResponse, Unit] {

      override protected def createRequest(): Either[String, ConnectSynchronizerRequest] =
        Right(
          ConnectSynchronizerRequest(
            config = Some(config.toProtoV30),
            sequencerConnectionValidation = sequencerConnectionValidation.toProtoV30,
          )
        )

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: ConnectSynchronizerRequest,
      ): Future[ConnectSynchronizerResponse] =
        service.connectSynchronizer(request)

      override protected def handleResponse(
          response: ConnectSynchronizerResponse
      ): Either[String, Unit] = Either.unit

      // can take long if we need to wait to become active
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class RegisterSynchronizer(
        config: SynchronizerConnectionConfig,
        performHandshake: Boolean,
        sequencerConnectionValidation: SequencerConnectionValidation,
    ) extends Base[RegisterSynchronizerRequest, RegisterSynchronizerResponse, Unit] {

      override protected def createRequest(): Either[String, RegisterSynchronizerRequest] = {
        val synchronizerConnection =
          if (performHandshake)
            RegisterSynchronizerRequest.SynchronizerConnection.SYNCHRONIZER_CONNECTION_HANDSHAKE
          else RegisterSynchronizerRequest.SynchronizerConnection.SYNCHRONIZER_CONNECTION_NONE

        Right(
          RegisterSynchronizerRequest(
            config = Some(config.toProtoV30),
            synchronizerConnection = synchronizerConnection,
            sequencerConnectionValidation = sequencerConnectionValidation.toProtoV30,
          )
        )
      }

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: RegisterSynchronizerRequest,
      ): Future[RegisterSynchronizerResponse] =
        service.registerSynchronizer(request)

      override protected def handleResponse(
          response: RegisterSynchronizerResponse
      ): Either[String, Unit] = Either.unit

      // can take long if we need to wait to become active
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ModifySynchronizerConnection(
        config: SynchronizerConnectionConfig,
        sequencerConnectionValidation: SequencerConnectionValidation,
    ) extends Base[ModifySynchronizerRequest, ModifySynchronizerResponse, Unit] {

      override protected def createRequest(): Either[String, ModifySynchronizerRequest] =
        Right(
          ModifySynchronizerRequest(
            newConfig = Some(config.toProtoV30),
            sequencerConnectionValidation = sequencerConnectionValidation.toProtoV30,
          )
        )

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: ModifySynchronizerRequest,
      ): Future[ModifySynchronizerResponse] =
        service.modifySynchronizer(request)

      override protected def handleResponse(
          response: ModifySynchronizerResponse
      ): Either[String, Unit] =
        Either.unit
    }

    final case class Logout(synchronizerAlias: SynchronizerAlias)
        extends Base[LogoutRequest, LogoutResponse, Unit] {

      override protected def createRequest(): Either[String, LogoutRequest] =
        Right(LogoutRequest(synchronizerAlias.toProtoPrimitive))

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: LogoutRequest,
      ): Future[LogoutResponse] =
        service.logout(request)

      override protected def handleResponse(response: LogoutResponse): Either[String, Unit] =
        Either.unit
    }
  }

  object Resources {
    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = ResourceManagementServiceStub

      override def createService(channel: ManagedChannel): ResourceManagementServiceStub =
        ResourceManagementServiceGrpc.stub(channel)
    }

    final case class GetResourceLimits()
        extends Base[v30.GetResourceLimitsRequest, v30.GetResourceLimitsResponse, ResourceLimits] {
      override protected def createRequest(): Either[String, v30.GetResourceLimitsRequest] = Right(
        v30.GetResourceLimitsRequest()
      )

      override protected def submitRequest(
          service: ResourceManagementServiceStub,
          request: v30.GetResourceLimitsRequest,
      ): Future[v30.GetResourceLimitsResponse] =
        service.getResourceLimits(request)

      override protected def handleResponse(
          response: v30.GetResourceLimitsResponse
      ): Either[String, ResourceLimits] =
        response.currentLimits.toRight("No limits returned").flatMap { limitsP =>
          ResourceLimits
            .fromProtoV30(limitsP)
            .leftMap(err => s"Failed to parse current limits: $err")
        }
    }

    final case class SetResourceLimits(limits: ResourceLimits)
        extends Base[v30.SetResourceLimitsRequest, v30.SetResourceLimitsResponse, Unit] {
      override protected def createRequest(): Either[String, v30.SetResourceLimitsRequest] = Right(
        v30.SetResourceLimitsRequest(newLimits = Some(limits.toProtoV30))
      )

      override protected def submitRequest(
          service: ResourceManagementServiceStub,
          request: v30.SetResourceLimitsRequest,
      ): Future[v30.SetResourceLimitsResponse] =
        service.setResourceLimits(request)

      override protected def handleResponse(
          response: v30.SetResourceLimitsResponse
      ): Either[String, Unit] = Either.unit
    }
  }

  object Inspection {

    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = InspectionServiceStub

      override def createService(channel: ManagedChannel): InspectionServiceStub =
        InspectionServiceGrpc.stub(channel)
    }

    final case class LookupOffsetByTime(ts: Timestamp)
        extends Base[
          v30.LookupOffsetByTimeRequest,
          v30.LookupOffsetByTimeResponse,
          Option[Long],
        ] {
      override protected def createRequest() = Right(v30.LookupOffsetByTimeRequest(Some(ts)))

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.LookupOffsetByTimeRequest,
      ): Future[v30.LookupOffsetByTimeResponse] =
        service.lookupOffsetByTime(request)

      override protected def handleResponse(
          response: v30.LookupOffsetByTimeResponse
      ): Either[String, Option[Long]] =
        Right(response.offset)
    }

    // TODO(#9557) R2 The code below should be sufficient
    final case class OpenCommitment(
        observer: StreamObserver[v30.OpenCommitmentResponse],
        commitment: AcsCommitment.CommitmentType,
        synchronizerId: SynchronizerId,
        computedForCounterParticipant: ParticipantId,
        toInclusive: CantonTimestamp,
    ) extends Base[
          v30.OpenCommitmentRequest,
          CancellableContext,
          CancellableContext,
        ] {
      override protected def createRequest() = Right(
        v30.OpenCommitmentRequest(
          AcsCommitment.commitmentTypeToProto(commitment),
          synchronizerId.toProtoPrimitive,
          computedForCounterParticipant.toProtoPrimitive,
          Some(toInclusive.toProtoTimestamp),
        )
      )

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.OpenCommitmentRequest,
      ): Future[CancellableContext] = {
        val context = Context.current().withCancellation()
        context.run(() => service.openCommitment(request, observer))
        Future.successful(context)
      }

      override protected def handleResponse(
          response: CancellableContext
      ): Either[String, CancellableContext] =
        Right(response)

      //  command might take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    // TODO(#9557) R2 The code below should be sufficient
    final case class CommitmentContracts(
        observer: StreamObserver[v30.InspectCommitmentContractsResponse],
        contracts: Seq[LfContractId],
        expectedDomainId: SynchronizerId,
        timestamp: CantonTimestamp,
        downloadPayload: Boolean,
    ) extends Base[
          v30.InspectCommitmentContractsRequest,
          CancellableContext,
          CancellableContext,
        ] {
      override protected def createRequest() = Right(
        v30.InspectCommitmentContractsRequest(
          contracts.map(_.toBytes.toByteString),
          expectedDomainId.toProtoPrimitive,
          Some(timestamp.toProtoTimestamp),
          downloadPayload,
        )
      )

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.InspectCommitmentContractsRequest,
      ): Future[CancellableContext] = {
        val context = Context.current().withCancellation()
        context.run(() => service.inspectCommitmentContracts(request, observer))
        Future.successful(context)
      }

      override protected def handleResponse(
          response: CancellableContext
      ): Either[String, CancellableContext] =
        Right(response)

      //  command might take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    final case class LookupReceivedAcsCommitments(
        synchronizerTimeRanges: Seq[SynchronizerTimeRange],
        counterParticipants: Seq[ParticipantId],
        commitmentState: Seq[ReceivedCmtState],
        verboseMode: Boolean,
    ) extends Base[
          v30.LookupReceivedAcsCommitmentsRequest,
          v30.LookupReceivedAcsCommitmentsResponse,
          Map[SynchronizerId, Seq[ReceivedAcsCmt]],
        ] {

      override protected def createRequest() = Right(
        v30.LookupReceivedAcsCommitmentsRequest(
          synchronizerTimeRanges.map { case synchronizerTimeRange =>
            v30.SynchronizerTimeRange(
              synchronizerTimeRange.synchronizerId.toProtoPrimitive,
              synchronizerTimeRange.timeRange.map { timeRange =>
                v30.TimeRange(
                  Some(timeRange.startExclusive.toProtoTimestamp),
                  Some(timeRange.endInclusive.toProtoTimestamp),
                )
              },
            )
          },
          counterParticipants.map(_.toProtoPrimitive),
          commitmentState.map(_.toProtoV30),
          verboseMode,
        )
      )

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.LookupReceivedAcsCommitmentsRequest,
      ): Future[v30.LookupReceivedAcsCommitmentsResponse] =
        service.lookupReceivedAcsCommitments(request)

      override protected def handleResponse(
          response: v30.LookupReceivedAcsCommitmentsResponse
      ): Either[
        String,
        Map[SynchronizerId, Seq[ReceivedAcsCmt]],
      ] =
        if (response.received.sizeIs != response.received.map(_.synchronizerId).toSet.size)
          Left(s"Some synchronizers are not unique in the response: ${response.received}")
        else
          response.received
            .traverse(receivedCmtPerDomain =>
              for {
                synchronizerId <- SynchronizerId.fromString(receivedCmtPerDomain.synchronizerId)
                receivedCmts <- receivedCmtPerDomain.received
                  .map(fromProtoToReceivedAcsCmt)
                  .sequence
              } yield synchronizerId -> receivedCmts
            )
            .map(_.toMap)
    }

    final case class TimeRange(startExclusive: CantonTimestamp, endInclusive: CantonTimestamp)

    final case class SynchronizerTimeRange(
        synchronizerId: SynchronizerId,
        timeRange: Option[TimeRange],
    )

    final case class ReceivedAcsCmt(
        receivedCmtPeriod: CommitmentPeriod,
        originCounterParticipant: ParticipantId,
        receivedCommitment: Option[AcsCommitment.CommitmentType],
        localCommitment: Option[AcsCommitment.CommitmentType],
        state: ReceivedCmtState,
    )

    private def fromIntervalToCommitmentPeriod(
        interval: Option[v30.Interval]
    ): Either[String, CommitmentPeriod] =
      interval match {
        case None => Left("Interval is missing")
        case Some(v) =>
          for {
            from <- v.startTickExclusive
              .traverse(CantonTimestamp.fromProtoTimestamp)
              .leftMap(_.toString)
            fromSecond <- CantonTimestampSecond.fromCantonTimestamp(
              from.getOrElse(CantonTimestamp.MinValue)
            )
            to <- v.endTickInclusive
              .traverse(CantonTimestamp.fromProtoTimestamp)
              .leftMap(_.toString)
            toSecond <- CantonTimestampSecond.fromCantonTimestamp(
              to.getOrElse(CantonTimestamp.MinValue)
            )
            len <- PositiveSeconds.create(
              java.time.Duration.ofSeconds(
                toSecond.minusSeconds(fromSecond.getEpochSecond).getEpochSecond
              )
            )
          } yield CommitmentPeriod(fromSecond, len)
      }

    private def fromProtoToReceivedAcsCmt(
        cmt: v30.ReceivedAcsCommitment
    ): Either[String, ReceivedAcsCmt] =
      for {
        state <- ReceivedCmtState.fromProtoV30(cmt.state).leftMap(_.toString)
        period <- fromIntervalToCommitmentPeriod(cmt.interval)
        participantId <- ParticipantId
          .fromProtoPrimitive(cmt.originCounterParticipantUid, "")
          .leftMap(_.toString)
      } yield ReceivedAcsCmt(
        period,
        participantId,
        Option
          .when(cmt.receivedCommitment.isDefined)(
            cmt.receivedCommitment.map(AcsCommitment.commitmentTypeFromByteString)
          )
          .flatten,
        Option
          .when(cmt.ownCommitment.isDefined)(
            cmt.ownCommitment.map(AcsCommitment.commitmentTypeFromByteString)
          )
          .flatten,
        state,
      )

    final case class LookupSentAcsCommitments(
        synchronizerTimeRanges: Seq[SynchronizerTimeRange],
        counterParticipants: Seq[ParticipantId],
        commitmentState: Seq[SentCmtState],
        verboseMode: Boolean,
    ) extends Base[
          v30.LookupSentAcsCommitmentsRequest,
          v30.LookupSentAcsCommitmentsResponse,
          Map[SynchronizerId, Seq[SentAcsCmt]],
        ] {

      override protected def createRequest() = Right(
        v30.LookupSentAcsCommitmentsRequest(
          synchronizerTimeRanges.map { case synchronizerTimeRange =>
            v30.SynchronizerTimeRange(
              synchronizerTimeRange.synchronizerId.toProtoPrimitive,
              synchronizerTimeRange.timeRange.map { timeRange =>
                v30.TimeRange(
                  Some(timeRange.startExclusive.toProtoTimestamp),
                  Some(timeRange.endInclusive.toProtoTimestamp),
                )
              },
            )
          },
          counterParticipants.map(_.toProtoPrimitive),
          commitmentState.map(_.toProtoV30),
          verboseMode,
        )
      )

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.LookupSentAcsCommitmentsRequest,
      ): Future[v30.LookupSentAcsCommitmentsResponse] =
        service.lookupSentAcsCommitments(request)

      override protected def handleResponse(
          response: v30.LookupSentAcsCommitmentsResponse
      ): Either[
        String,
        Map[SynchronizerId, Seq[SentAcsCmt]],
      ] =
        if (response.sent.sizeIs != response.sent.map(_.synchronizerId).toSet.size)
          Left(
            s"Some synchronizers are not unique in the response: ${response.sent}"
          )
        else
          response.sent
            .traverse(sentCmtPerDomain =>
              for {
                synchronizerId <- SynchronizerId.fromString(sentCmtPerDomain.synchronizerId)
                sentCmts <- sentCmtPerDomain.sent.map(fromProtoToSentAcsCmt).sequence
              } yield synchronizerId -> sentCmts
            )
            .map(_.toMap)
    }

    final case class SentAcsCmt(
        receivedCmtPeriod: CommitmentPeriod,
        destCounterParticipant: ParticipantId,
        sentCommitment: Option[AcsCommitment.CommitmentType],
        receivedCommitment: Option[AcsCommitment.CommitmentType],
        state: SentCmtState,
    )

    private def fromProtoToSentAcsCmt(
        cmt: v30.SentAcsCommitment
    ): Either[String, SentAcsCmt] =
      for {
        state <- SentCmtState.fromProtoV30(cmt.state).leftMap(_.toString)
        period <- fromIntervalToCommitmentPeriod(cmt.interval)
        participantId <- ParticipantId
          .fromProtoPrimitive(cmt.destCounterParticipantUid, "")
          .leftMap(_.toString)
      } yield SentAcsCmt(
        period,
        participantId,
        Option
          .when(cmt.ownCommitment.isDefined)(
            cmt.ownCommitment.map(AcsCommitment.commitmentTypeFromByteString)
          )
          .flatten,
        Option
          .when(cmt.receivedCommitment.isDefined)(
            cmt.receivedCommitment.map(AcsCommitment.commitmentTypeFromByteString)
          )
          .flatten,
        state,
      )

    final case class SetConfigForSlowCounterParticipants(
        configs: Seq[SlowCounterParticipantSynchronizerConfig]
    ) extends Base[
          v30.SetConfigForSlowCounterParticipantsRequest,
          v30.SetConfigForSlowCounterParticipantsResponse,
          Unit,
        ] {

      override protected def createRequest() = Right(
        v30.SetConfigForSlowCounterParticipantsRequest(
          configs.map(_.toProtoV30)
        )
      )

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.SetConfigForSlowCounterParticipantsRequest,
      ): Future[v30.SetConfigForSlowCounterParticipantsResponse] =
        service.setConfigForSlowCounterParticipants(request)

      override protected def handleResponse(
          response: v30.SetConfigForSlowCounterParticipantsResponse
      ): Either[String, Unit] = Either.unit
    }

    final case class SlowCounterParticipantSynchronizerConfig(
        synchronizerIds: Seq[SynchronizerId],
        distinguishedParticipants: Seq[ParticipantId],
        thresholdDistinguished: NonNegativeInt,
        thresholdDefault: NonNegativeInt,
        participantsMetrics: Seq[ParticipantId],
    ) {
      def toProtoV30: v30.SlowCounterParticipantSynchronizerConfig =
        v30.SlowCounterParticipantSynchronizerConfig(
          synchronizerIds.map(_.toProtoPrimitive),
          distinguishedParticipants.map(_.toProtoPrimitive),
          thresholdDistinguished.value.toLong,
          thresholdDefault.value.toLong,
          participantsMetrics.map(_.toProtoPrimitive),
        )
    }

    object SlowCounterParticipantSynchronizerConfig {
      def fromProtoV30(
          config: v30.SlowCounterParticipantSynchronizerConfig
      ): Either[String, SlowCounterParticipantSynchronizerConfig] = {
        val thresholdDistinguished = NonNegativeInt.tryCreate(config.thresholdDistinguished.toInt)
        val thresholdDefault = NonNegativeInt.tryCreate(config.thresholdDefault.toInt)
        val distinguishedParticipants =
          config.distinguishedParticipantUids.map(ParticipantId.tryFromProtoPrimitive)
        val participantsMetrics =
          config.participantUidsMetrics.map(ParticipantId.tryFromProtoPrimitive)
        for {
          synchronizerIds <- config.synchronizerIds.map(SynchronizerId.fromString).sequence
        } yield SlowCounterParticipantSynchronizerConfig(
          synchronizerIds,
          distinguishedParticipants,
          thresholdDistinguished,
          thresholdDefault,
          participantsMetrics,
        )
      }
    }

    final case class GetConfigForSlowCounterParticipants(
        synchronizerIds: Seq[SynchronizerId]
    ) extends Base[
          v30.GetConfigForSlowCounterParticipantsRequest,
          v30.GetConfigForSlowCounterParticipantsResponse,
          Seq[SlowCounterParticipantSynchronizerConfig],
        ] {

      override protected def createRequest() = Right(
        v30.GetConfigForSlowCounterParticipantsRequest(
          synchronizerIds.map(_.toProtoPrimitive)
        )
      )

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.GetConfigForSlowCounterParticipantsRequest,
      ): Future[v30.GetConfigForSlowCounterParticipantsResponse] =
        service.getConfigForSlowCounterParticipants(request)

      override protected def handleResponse(
          response: v30.GetConfigForSlowCounterParticipantsResponse
      ): Either[String, Seq[SlowCounterParticipantSynchronizerConfig]] =
        response.configs.map(SlowCounterParticipantSynchronizerConfig.fromProtoV30).sequence
    }

    final case class CounterParticipantInfo(
        participantId: ParticipantId,
        synchronizerId: SynchronizerId,
        intervalsBehind: NonNegativeLong,
        asOfSequencingTimestamp: Instant,
    )

    final case class GetIntervalsBehindForCounterParticipants(
        counterParticipants: Seq[ParticipantId],
        synchronizerIds: Seq[SynchronizerId],
        threshold: NonNegativeInt,
    ) extends Base[
          v30.GetIntervalsBehindForCounterParticipantsRequest,
          v30.GetIntervalsBehindForCounterParticipantsResponse,
          Seq[CounterParticipantInfo],
        ] {

      override protected def createRequest() = Right(
        v30.GetIntervalsBehindForCounterParticipantsRequest(
          counterParticipants.map(_.toProtoPrimitive),
          synchronizerIds.map(_.toProtoPrimitive),
          Some(threshold.value.toLong),
        )
      )

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.GetIntervalsBehindForCounterParticipantsRequest,
      ): Future[v30.GetIntervalsBehindForCounterParticipantsResponse] =
        service.getIntervalsBehindForCounterParticipants(request)

      override protected def handleResponse(
          response: v30.GetIntervalsBehindForCounterParticipantsResponse
      ): Either[String, Seq[CounterParticipantInfo]] =
        response.intervalsBehind.map { info =>
          for {
            synchronizerId <- SynchronizerId.fromString(info.synchronizerId)
            participantId <- ParticipantId
              .fromProtoPrimitive(info.counterParticipantUid, "")
              .leftMap(_.toString)
            asOf <- ProtoConverter
              .parseRequired(
                CantonTimestamp.fromProtoTimestamp,
                "as_of",
                info.asOfSequencingTimestamp,
              )
              .leftMap(_.toString)
            intervalsBehind <- NonNegativeLong.create(info.intervalsBehind).leftMap(_.toString)
          } yield CounterParticipantInfo(
            participantId,
            synchronizerId,
            intervalsBehind,
            asOf.toInstant,
          )
        }.sequence
    }

    final case class CountInFlight(synchronizerId: SynchronizerId)
        extends Base[
          v30.CountInFlightRequest,
          v30.CountInFlightResponse,
          InFlightCount,
        ] {

      override protected def createRequest(): Either[String, v30.CountInFlightRequest] =
        Right(v30.CountInFlightRequest(synchronizerId.toProtoPrimitive))

      override protected def submitRequest(
          service: InspectionServiceStub,
          request: v30.CountInFlightRequest,
      ): Future[v30.CountInFlightResponse] =
        service.countInFlight(request)

      override protected def handleResponse(
          response: v30.CountInFlightResponse
      ): Either[String, InFlightCount] =
        for {
          pendingSubmissions <- ProtoConverter
            .parseNonNegativeInt("CountInFlight.pending_submissions", response.pendingSubmissions)
            .leftMap(_.toString)
          pendingTransactions <- ProtoConverter
            .parseNonNegativeInt("CountInFlight.pending_transactions", response.pendingTransactions)
            .leftMap(_.toString)
        } yield {
          InFlightCount(pendingSubmissions, pendingTransactions)
        }
    }

  }

  object Pruning {
    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = PruningServiceStub

      override def createService(channel: ManagedChannel): PruningServiceStub =
        PruningServiceGrpc.stub(channel)
    }

    final case class GetSafePruningOffsetCommand(beforeOrAt: Instant, ledgerEnd: Long)
        extends Base[v30.GetSafePruningOffsetRequest, v30.GetSafePruningOffsetResponse, Option[
          Long
        ]] {

      override protected def createRequest(): Either[String, v30.GetSafePruningOffsetRequest] =
        for {
          beforeOrAt <- CantonTimestamp.fromInstant(beforeOrAt)
        } yield v30.GetSafePruningOffsetRequest(Some(beforeOrAt.toProtoTimestamp), ledgerEnd)

      override protected def submitRequest(
          service: PruningServiceStub,
          request: v30.GetSafePruningOffsetRequest,
      ): Future[v30.GetSafePruningOffsetResponse] = service.getSafePruningOffset(request)

      override protected def handleResponse(
          response: v30.GetSafePruningOffsetResponse
      ): Either[String, Option[Long]] = response.response match {
        case v30.GetSafePruningOffsetResponse.Response.Empty => Left("Unexpected empty response")

        case v30.GetSafePruningOffsetResponse.Response.SafePruningOffset(offset) =>
          Right(Some(offset))

        case v30.GetSafePruningOffsetResponse.Response.NoSafePruningOffset(_) => Right(None)
      }
    }

    final case class PruneInternallyCommand(pruneUpTo: Long)
        extends Base[v30.PruneRequest, v30.PruneResponse, Unit] {
      override protected def createRequest(): Either[String, PruneRequest] =
        Right(v30.PruneRequest(pruneUpTo))

      override protected def submitRequest(
          service: PruningServiceStub,
          request: v30.PruneRequest,
      ): Future[v30.PruneResponse] =
        service.prune(request)

      override protected def handleResponse(response: v30.PruneResponse): Either[String, Unit] =
        Either.unit
    }

    final case class SetParticipantScheduleCommand(
        cron: String,
        maxDuration: config.PositiveDurationSeconds,
        retention: config.PositiveDurationSeconds,
        pruneInternallyOnly: Boolean,
    ) extends Base[
          pruning.v30.SetParticipantScheduleRequest,
          pruning.v30.SetParticipantScheduleResponse,
          Unit,
        ] {
      override protected def createRequest()
          : Right[String, pruning.v30.SetParticipantScheduleRequest] =
        Right(
          pruning.v30.SetParticipantScheduleRequest(
            Some(
              pruning.v30.ParticipantPruningSchedule(
                Some(
                  pruning.v30.PruningSchedule(
                    cron,
                    Some(maxDuration.toProtoPrimitive),
                    Some(retention.toProtoPrimitive),
                  )
                ),
                pruneInternallyOnly,
              )
            )
          )
        )

      override protected def submitRequest(
          service: Svc,
          request: pruning.v30.SetParticipantScheduleRequest,
      ): Future[pruning.v30.SetParticipantScheduleResponse] =
        service.setParticipantSchedule(request)

      override protected def handleResponse(
          response: pruning.v30.SetParticipantScheduleResponse
      ): Either[String, Unit] =
        response match {
          case pruning.v30.SetParticipantScheduleResponse() => Either.unit
        }
    }

    final case class SetNoWaitCommitmentsFrom(
        counterParticipants: Seq[ParticipantId],
        synchronizerIds: Seq[SynchronizerId],
    ) extends Base[
          pruning.v30.SetNoWaitCommitmentsFromRequest,
          pruning.v30.SetNoWaitCommitmentsFromResponse,
          Unit,
        ] {
      override def createRequest(): Either[String, pruning.v30.SetNoWaitCommitmentsFromRequest] =
        Right(
          pruning.v30.SetNoWaitCommitmentsFromRequest(
            counterParticipants.map(_.toProtoPrimitive),
            synchronizerIds.map(_.toProtoPrimitive),
          )
        )

      override protected def submitRequest(
          service: Svc,
          request: pruning.v30.SetNoWaitCommitmentsFromRequest,
      ): Future[pruning.v30.SetNoWaitCommitmentsFromResponse] =
        service.setNoWaitCommitmentsFrom(request)

      override protected def handleResponse(
          response: pruning.v30.SetNoWaitCommitmentsFromResponse
      ): Either[String, Unit] = Right(())
    }

    final case class NoWaitCommitments(
        counterParticipant: ParticipantId,
        domains: Seq[SynchronizerId],
    )

    object NoWaitCommitments {
      def fromSetup(setup: Seq[WaitCommitmentsSetup]): Either[String, Seq[NoWaitCommitments]] = {
        val s = setup.map(setup =>
          for {
            synchronizers <- setup.synchronizers.traverse(
              _.synchronizerIds.traverse(SynchronizerId.fromString)
            )
            participantId <- ParticipantId
              .fromProtoPrimitive(setup.counterParticipantUid, "")
              .leftMap(_.toString)
          } yield NoWaitCommitments(
            participantId,
            synchronizers.getOrElse(Seq.empty),
          )
        )
        if (s.forall(_.isRight)) {
          Right(
            s.map(
              _.getOrElse(
                NoWaitCommitments(
                  ParticipantId.tryFromProtoPrimitive("PAR::participant::error"),
                  Seq.empty,
                )
              )
            )
          )
        } else
          Left("Error parsing response of getNoWaitCommitmentsFrom")
      }
    }

    final case class SetWaitCommitmentsFrom(
        counterParticipants: Seq[ParticipantId],
        synchronizerIds: Seq[SynchronizerId],
    ) extends Base[
          pruning.v30.ResetNoWaitCommitmentsFromRequest,
          pruning.v30.ResetNoWaitCommitmentsFromResponse,
          Unit,
        ] {
      override protected def createRequest()
          : Right[String, pruning.v30.ResetNoWaitCommitmentsFromRequest] =
        Right(
          pruning.v30.ResetNoWaitCommitmentsFromRequest(
            counterParticipants.map(_.toProtoPrimitive),
            synchronizerIds.map(_.toProtoPrimitive),
          )
        )

      override protected def submitRequest(
          service: Svc,
          request: pruning.v30.ResetNoWaitCommitmentsFromRequest,
      ): Future[pruning.v30.ResetNoWaitCommitmentsFromResponse] =
        service.resetNoWaitCommitmentsFrom(request)

      override protected def handleResponse(
          response: pruning.v30.ResetNoWaitCommitmentsFromResponse
      ): Either[String, Unit] = Right(())
    }

    final case class WaitCommitments(
        counterParticipant: ParticipantId,
        domains: Seq[SynchronizerId],
    )

    object WaitCommitments {
      def fromSetup(setup: Seq[WaitCommitmentsSetup]): Either[String, Seq[WaitCommitments]] = {
        val s = setup.map(setup =>
          for {
            synchronizers <- setup.synchronizers.traverse(
              _.synchronizerIds.traverse(SynchronizerId.fromString)
            )
          } yield WaitCommitments(
            ParticipantId.tryFromProtoPrimitive(setup.counterParticipantUid),
            synchronizers.getOrElse(Seq.empty),
          )
        )
        if (s.forall(_.isRight)) {
          Right(
            s.map(
              _.getOrElse(
                WaitCommitments(
                  ParticipantId.tryFromProtoPrimitive("PAR::participant::error"),
                  Seq.empty,
                )
              )
            )
          )
        } else
          Left("Error parsing response of getNoWaitCommitmentsFrom")
      }
    }

    final case class GetNoWaitCommitmentsFrom(
        domains: Seq[SynchronizerId],
        counterParticipants: Seq[ParticipantId],
    ) extends Base[
          pruning.v30.GetNoWaitCommitmentsFromRequest,
          pruning.v30.GetNoWaitCommitmentsFromResponse,
          (Seq[NoWaitCommitments], Seq[WaitCommitments]),
        ] {

      override protected def createRequest()
          : Right[String, pruning.v30.GetNoWaitCommitmentsFromRequest] =
        Right(
          pruning.v30.GetNoWaitCommitmentsFromRequest(
            domains.map(_.toProtoPrimitive),
            counterParticipants.map(_.toProtoPrimitive),
          )
        )

      override protected def submitRequest(
          service: Svc,
          request: pruning.v30.GetNoWaitCommitmentsFromRequest,
      ): Future[pruning.v30.GetNoWaitCommitmentsFromResponse] =
        service.getNoWaitCommitmentsFrom(request)

      override protected def handleResponse(
          response: pruning.v30.GetNoWaitCommitmentsFromResponse
      ): Either[String, (Seq[NoWaitCommitments], Seq[WaitCommitments])] = {
        val ignoredCounterParticipants = NoWaitCommitments.fromSetup(response.ignoredParticipants)
        val nonIgnoredCounterParticipants =
          WaitCommitments.fromSetup(response.notIgnoredParticipants)
        if (ignoredCounterParticipants.isLeft || nonIgnoredCounterParticipants.isLeft) {
          Left("Error parsing response of getNoWaitCommitmentsFrom")
        } else {
          Right(
            (
              ignoredCounterParticipants.getOrElse(Seq.empty),
              nonIgnoredCounterParticipants.getOrElse(Seq.empty),
            )
          )
        }
      }
    }

    final case class GetParticipantScheduleCommand()
        extends Base[
          pruning.v30.GetParticipantScheduleRequest,
          pruning.v30.GetParticipantScheduleResponse,
          Option[ParticipantPruningSchedule],
        ] {
      override protected def createRequest()
          : Right[String, pruning.v30.GetParticipantScheduleRequest] =
        Right(
          pruning.v30.GetParticipantScheduleRequest()
        )

      override protected def submitRequest(
          service: Svc,
          request: pruning.v30.GetParticipantScheduleRequest,
      ): Future[pruning.v30.GetParticipantScheduleResponse] =
        service.getParticipantSchedule(request)

      override protected def handleResponse(
          response: pruning.v30.GetParticipantScheduleResponse
      ): Either[String, Option[ParticipantPruningSchedule]] =
        response.schedule.fold(
          Right(None): Either[String, Option[ParticipantPruningSchedule]]
        )(ParticipantPruningSchedule.fromProtoV30(_).bimap(_.message, Some(_)))
    }
  }

  object Replication {

    final case class SetPassiveCommand()
        extends GrpcAdminCommand[v30.SetPassiveRequest, v30.SetPassiveResponse, Unit] {
      override type Svc = EnterpriseParticipantReplicationServiceStub

      override def createService(
          channel: ManagedChannel
      ): EnterpriseParticipantReplicationServiceStub =
        EnterpriseParticipantReplicationServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, v30.SetPassiveRequest] =
        Right(v30.SetPassiveRequest())

      override protected def submitRequest(
          service: EnterpriseParticipantReplicationServiceStub,
          request: v30.SetPassiveRequest,
      ): Future[v30.SetPassiveResponse] =
        service.setPassive(request)

      override protected def handleResponse(
          response: v30.SetPassiveResponse
      ): Either[String, Unit] =
        response match {
          case v30.SetPassiveResponse() => Either.unit
        }
    }
  }

  object TrafficControl {
    final case class GetTrafficControlState(synchronizerId: SynchronizerId)
        extends GrpcAdminCommand[
          TrafficControlStateRequest,
          TrafficControlStateResponse,
          TrafficState,
        ] {
      override type Svc = TrafficControlServiceGrpc.TrafficControlServiceStub

      override def createService(
          channel: ManagedChannel
      ): TrafficControlServiceGrpc.TrafficControlServiceStub =
        TrafficControlServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: TrafficControlServiceGrpc.TrafficControlServiceStub,
          request: TrafficControlStateRequest,
      ): Future[TrafficControlStateResponse] =
        service.trafficControlState(request)

      override protected def createRequest(): Either[String, TrafficControlStateRequest] = Right(
        TrafficControlStateRequest(synchronizerId.toProtoPrimitive)
      )

      override protected def handleResponse(
          response: TrafficControlStateResponse
      ): Either[String, TrafficState] =
        response.trafficState
          .map { trafficStatus =>
            TrafficStateAdmin
              .fromProto(trafficStatus)
              .leftMap(_.message)
          }
          .getOrElse(Left("No traffic state available"))
    }
  }

  object Health {
    final case class ParticipantStatusCommand()
        extends GrpcAdminCommand[
          ParticipantStatusRequest,
          ParticipantStatusResponse,
          NodeStatus[ParticipantStatus],
        ] {

      override type Svc = ParticipantStatusServiceStub

      override def createService(channel: ManagedChannel): ParticipantStatusServiceStub =
        ParticipantStatusServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: ParticipantStatusServiceStub,
          request: ParticipantStatusRequest,
      ): Future[ParticipantStatusResponse] =
        service.participantStatus(request)

      override protected def createRequest(): Either[String, ParticipantStatusRequest] = Right(
        ParticipantStatusRequest()
      )

      override protected def handleResponse(
          response: ParticipantStatusResponse
      ): Either[String, NodeStatus[ParticipantStatus]] =
        ParticipantStatus.fromProtoV30(response).leftMap(_.message)
    }
  }

}
