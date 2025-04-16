// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.implicits.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  ServerEnforcedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.api.client.data.PackageDescription.PackageContents
import com.digitalasset.canton.admin.api.client.data.{
  AddPartyStatus,
  DarContents,
  DarDescription,
  InFlightCount,
  ListConnectedSynchronizersResult,
  NodeStatus,
  PackageDescription,
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
import com.digitalasset.canton.admin.pruning
import com.digitalasset.canton.admin.pruning.v30.WaitCommitmentsSetup
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.admin.ResourceLimits
import com.digitalasset.canton.participant.admin.data.ContractIdImportMode
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
import com.digitalasset.canton.util.{BinaryFileUtil, GrpcStreamingUtils, OptionUtil, PathUtils}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{ReassignmentCounter, SequencerCounter, SynchronizerAlias, config}
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver
import io.grpc.{Context, ManagedChannel}

import java.io.{File, IOException}
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
        v30.PackageServiceGrpc.stub(channel)
    }

    final case class List(filterName: String, limit: PositiveInt)
        extends PackageCommand[v30.ListPackagesRequest, v30.ListPackagesResponse, Seq[
          PackageDescription
        ]] {
      override protected def createRequest() = Right(
        v30.ListPackagesRequest(limit = limit.value, filterName = filterName)
      )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: v30.ListPackagesRequest,
      ): Future[v30.ListPackagesResponse] =
        service.listPackages(request)

      override protected def handleResponse(
          response: v30.ListPackagesResponse
      ): Either[String, Seq[PackageDescription]] =
        response.packageDescriptions.traverse(PackageDescription.fromProto).leftMap(_.toString)
    }

    final case class GetContents(packageId: String)
        extends PackageCommand[
          v30.GetPackageContentsRequest,
          v30.GetPackageContentsResponse,
          PackageContents,
        ] {
      override protected def createRequest() = Right(v30.GetPackageContentsRequest(packageId))

      override protected def submitRequest(
          service: PackageServiceStub,
          request: v30.GetPackageContentsRequest,
      ): Future[v30.GetPackageContentsResponse] =
        service.getPackageContents(request)

      override protected def handleResponse(
          response: v30.GetPackageContentsResponse
      ): Either[String, PackageContents] = {
        val v30.GetPackageContentsResponse(
          descriptionP,
          modulesP,
          isUtilityPackage,
          languageVersion,
        ) =
          response
        (for {
          desc <- ProtoConverter.parseRequired(
            PackageDescription.fromProto,
            "description",
            descriptionP,
          )
        } yield PackageContents(
          description = desc,
          modules = modulesP.map(_.name).toSet,
          isUtilityPackage = isUtilityPackage,
          languageVersion = languageVersion,
        )).leftMap(_.toString)

      }
    }

    final case class GetReferences(packageId: String)
        extends PackageCommand[
          v30.GetPackageReferencesRequest,
          v30.GetPackageReferencesResponse,
          Seq[DarDescription],
        ] {
      override protected def createRequest() = Right(v30.GetPackageReferencesRequest(packageId))

      override protected def submitRequest(
          service: PackageServiceStub,
          request: v30.GetPackageReferencesRequest,
      ): Future[v30.GetPackageReferencesResponse] =
        service.getPackageReferences(request)

      override protected def handleResponse(
          response: v30.GetPackageReferencesResponse
      ): Either[String, Seq[DarDescription]] = {
        val v30.GetPackageReferencesResponse(darsP) =
          response
        (for {
          dars <- darsP.traverse(DarDescription.fromProtoV30)
        } yield dars).leftMap(_.toString)
      }
    }

    final case class DarData(darPath: String, description: String, expectedMainPackageId: String)
    final case class UploadDar(
        dars: Seq[DarData],
        vetAllPackages: Boolean,
        synchronizeVetting: Boolean,
        requestHeaders: Map[String, String],
        logger: TracedLogger,
    ) extends PackageCommand[v30.UploadDarRequest, v30.UploadDarResponse, Seq[String]] {
      override protected def createRequest(): Either[String, v30.UploadDarRequest] =
        dars
          .traverse(dar =>
            for {
              _ <- Either.cond(dar.darPath.nonEmpty, (), "Provided DAR path is empty")
              filenameAndDarData <- loadDarData(dar.darPath)
              (filename, darData) = filenameAndDarData
              descriptionOrFilename =
                if (dar.description.isEmpty)
                  PathUtils.getFilenameWithoutExtension(Path.of(filename))
                else dar.description
            } yield v30.UploadDarRequest.UploadDarData(
              darData,
              Some(descriptionOrFilename),
              OptionUtil.emptyStringAsNone(dar.expectedMainPackageId.trim),
            )
          )
          .map(
            v30.UploadDarRequest(
              _,
              synchronizeVetting = synchronizeVetting,
              vetAllPackages = vetAllPackages,
            )
          )

      /** Reads the dar data from a path:
        *   - If the path is a URL (`darPath` starts with http), it downloads the file to a
        *     temporary file and reads the data from there.
        *   - Otherwise, it reads the data from the file at the given path.
        *
        * @return
        *   Name of the local file and the dar data.
        */
      private def loadDarData(darPath: String): Either[String, (String, ByteString)] = if (
        darPath.startsWith("http")
      ) {
        val file = Files.createTempFile("cantonTempDar", ".dar").toFile
        file.deleteOnExit()
        for {
          _ <- BinaryFileUtil.downloadFile(darPath, file.toString, requestHeaders)
          darData <- BinaryFileUtil.readByteStringFromFile(file.toString)
        } yield (darPath, darData)
      } else {
        val filename = Paths.get(darPath).getFileName.toString
        for {
          darData <- BinaryFileUtil.readByteStringFromFile(darPath)
        } yield (filename, darData)
      }

      override protected def submitRequest(
          service: PackageServiceStub,
          request: v30.UploadDarRequest,
      ): Future[v30.UploadDarResponse] =
        service.uploadDar(request)

      override protected def handleResponse(
          response: v30.UploadDarResponse
      ): Either[String, Seq[String]] = Right(response.darIds)

      // file can be big. checking & vetting might take a while
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    object UploadDar {
      def apply(
          darPath: String,
          vetAllPackages: Boolean,
          synchronizeVetting: Boolean,
          description: String,
          expectedMainPackageId: String,
          requestHeaders: Map[String, String],
          logger: TracedLogger,
      ): UploadDar = UploadDar(
        Seq(DarData(darPath, description, expectedMainPackageId)),
        vetAllPackages,
        synchronizeVetting,
        requestHeaders,
        logger,
      )

    }

    final case class ValidateDar(
        darPath: Option[String],
        logger: TracedLogger,
    ) extends PackageCommand[v30.ValidateDarRequest, v30.ValidateDarResponse, String] {

      override protected def createRequest(): Either[String, v30.ValidateDarRequest] =
        for {
          pathValue <- darPath.toRight("DAR path not provided")
          nonEmptyPathValue <- Either.cond(
            pathValue.nonEmpty,
            pathValue,
            "Provided DAR path is empty",
          )
          filename = Paths.get(nonEmptyPathValue).getFileName.toString
          darData <- BinaryFileUtil.readByteStringFromFile(nonEmptyPathValue)
        } yield v30.ValidateDarRequest(
          darData,
          filename,
        )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: v30.ValidateDarRequest,
      ): Future[v30.ValidateDarResponse] =
        service.validateDar(request)

      override protected def handleResponse(
          response: v30.ValidateDarResponse
      ): Either[String, String] =
        response match {
          case v30.ValidateDarResponse(hash) => Right(hash)
        }

      // file can be big. checking & vetting might take a while
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class RemovePackage(
        packageId: String,
        force: Boolean,
    ) extends PackageCommand[v30.RemovePackageRequest, v30.RemovePackageResponse, Unit] {

      override protected def createRequest() = Right(v30.RemovePackageRequest(packageId, force))

      override protected def submitRequest(
          service: PackageServiceStub,
          request: v30.RemovePackageRequest,
      ): Future[v30.RemovePackageResponse] =
        service.removePackage(request)

      override protected def handleResponse(
          response: v30.RemovePackageResponse
      ): Either[String, Unit] =
        response.success match {
          case None => Left("unexpected empty response")
          case Some(_success) => Right(())
        }

    }

    final case class GetDar(
        mainPackageId: String,
        destinationDirectory: String,
        logger: TracedLogger,
    ) extends PackageCommand[v30.GetDarRequest, v30.GetDarResponse, Path] {
      override protected def createRequest(): Either[String, v30.GetDarRequest] =
        for {
          _ <- Either.cond(
            destinationDirectory.nonEmpty,
            destinationDirectory,
            "DAR destination directory not provided",
          )
          dest = (new File(destinationDirectory))
          _ <- Either.cond(
            dest.exists() && dest.isDirectory && dest.canWrite,
            (),
            s"$destinationDirectory is not a writable directory",
          )
          hash <- Either.cond(
            mainPackageId.nonEmpty,
            mainPackageId,
            "The main package-id not provided",
          )
        } yield v30.GetDarRequest(hash)

      override protected def submitRequest(
          service: PackageServiceStub,
          request: v30.GetDarRequest,
      ): Future[v30.GetDarResponse] =
        service.getDar(request)

      override protected def handleResponse(response: v30.GetDarResponse): Either[String, Path] = {
        val v30.GetDarResponse(payloadP, dataP) = response
        for {
          payload <- if (payloadP.isEmpty) Left("DAR was not found") else Right(payloadP)
          response <- dataP.toRight("Response is missing context information")
          path <-
            try {
              val path =
                Paths.get(destinationDirectory, s"${response.name}-${response.version}.dar")
              Files.write(path, payload.toByteArray)
              Right(path)
            } catch {
              case ex: IOException =>
                // the trace context for admin commands is started by the submit-request call
                // however we can't get at it here
                logger.debug(s"Error saving DAR to $destinationDirectory: $ex")(TraceContext.empty)
                Left(s"Error saving DAR to $destinationDirectory")
            }
        } yield path
      }

      // might be a big file to download
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class GetDarContents(mainPackageId: String)
        extends PackageCommand[v30.GetDarContentsRequest, v30.GetDarContentsResponse, DarContents] {
      override protected def createRequest() = Right(v30.GetDarContentsRequest(mainPackageId))

      override protected def submitRequest(
          service: PackageServiceStub,
          request: v30.GetDarContentsRequest,
      ): Future[v30.GetDarContentsResponse] =
        service.getDarContents(request)

      override protected def handleResponse(
          response: v30.GetDarContentsResponse
      ): Either[String, DarContents] =
        DarContents.fromProtoV30(response).leftMap(_.toString)

    }

    final case class RemoveDar(
        darHash: String
    ) extends PackageCommand[v30.RemoveDarRequest, v30.RemoveDarResponse, Unit] {

      override protected def createRequest(): Either[String, v30.RemoveDarRequest] = Right(
        v30.RemoveDarRequest(darHash)
      )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: v30.RemoveDarRequest,
      ): Future[v30.RemoveDarResponse] =
        service.removeDar(request)

      override protected def handleResponse(
          response: v30.RemoveDarResponse
      ): Either[String, Unit] = Right(())

    }

    final case class VetDar(darDash: String, synchronize: Boolean)
        extends PackageCommand[v30.VetDarRequest, v30.VetDarResponse, Unit] {
      override protected def createRequest(): Either[String, v30.VetDarRequest] = Right(
        v30.VetDarRequest(darDash, synchronize)
      )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: v30.VetDarRequest,
      ): Future[v30.VetDarResponse] = service.vetDar(request)

      override protected def handleResponse(response: v30.VetDarResponse): Either[String, Unit] =
        Either.unit
    }

    // TODO(#14432): Add `synchronize` flag which makes the call block until the unvetting operation
    //               is observed by the participant on all connected synchronizers.
    final case class UnvetDar(mainPackageId: String)
        extends PackageCommand[v30.UnvetDarRequest, v30.UnvetDarResponse, Unit] {

      override protected def createRequest(): Either[String, v30.UnvetDarRequest] = Right(
        v30.UnvetDarRequest(mainPackageId)
      )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: v30.UnvetDarRequest,
      ): Future[v30.UnvetDarResponse] = service.unvetDar(request)

      override protected def handleResponse(response: v30.UnvetDarResponse): Either[String, Unit] =
        Either.unit
    }

    final case class ListDars(filterName: String, limit: PositiveInt)
        extends PackageCommand[v30.ListDarsRequest, v30.ListDarsResponse, Seq[DarDescription]] {
      override protected def createRequest(): Either[String, v30.ListDarsRequest] = Right(
        v30.ListDarsRequest(limit = limit.value, filterName = filterName)
      )

      override protected def submitRequest(
          service: PackageServiceStub,
          request: v30.ListDarsRequest,
      ): Future[v30.ListDarsResponse] =
        service.listDars(request)

      override protected def handleResponse(
          response: v30.ListDarsResponse
      ): Either[String, Seq[DarDescription]] =
        response.dars.traverse(DarDescription.fromProtoV30).leftMap(_.toString)
    }

  }

  object PartyManagement {

    final case class AddPartyAsync(
        party: PartyId,
        synchronizerId: SynchronizerId,
        sourceParticipant: Option[ParticipantId],
        serial: Option[PositiveInt],
    ) extends GrpcAdminCommand[
          v30.AddPartyAsyncRequest,
          v30.AddPartyAsyncResponse,
          String,
        ] {
      override type Svc = PartyManagementServiceStub

      override def createService(channel: ManagedChannel): PartyManagementServiceStub =
        v30.PartyManagementServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, v30.AddPartyAsyncRequest] =
        Right(
          v30.AddPartyAsyncRequest(
            partyId = party.toProtoPrimitive,
            synchronizerId = synchronizerId.toProtoPrimitive,
            sourceParticipantUid = sourceParticipant.fold("")(_.uid.toProtoPrimitive),
            serial = serial.fold(0)(_.value),
          )
        )

      override protected def submitRequest(
          service: PartyManagementServiceStub,
          request: v30.AddPartyAsyncRequest,
      ): Future[v30.AddPartyAsyncResponse] = service.addPartyAsync(request)

      override protected def handleResponse(
          response: v30.AddPartyAsyncResponse
      ): Either[String, String] = Right(response.addPartyRequestId)
    }

    final case class GetAddPartyStatus(requestId: String)
        extends GrpcAdminCommand[
          v30.GetAddPartyStatusRequest,
          v30.GetAddPartyStatusResponse,
          AddPartyStatus,
        ] {
      override type Svc = PartyManagementServiceStub

      override def createService(channel: ManagedChannel): PartyManagementServiceStub =
        v30.PartyManagementServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, v30.GetAddPartyStatusRequest] =
        Right(v30.GetAddPartyStatusRequest(requestId))

      override protected def submitRequest(
          service: PartyManagementServiceStub,
          request: v30.GetAddPartyStatusRequest,
      ): Future[v30.GetAddPartyStatusResponse] = service.getAddPartyStatus(request)

      override protected def handleResponse(
          response: v30.GetAddPartyStatusResponse
      ): Either[String, AddPartyStatus] = AddPartyStatus.fromProtoV30(response).leftMap(_.toString)
    }

    final case class ExportAcs(
        parties: Set[PartyId],
        filterSynchronizerId: Option[SynchronizerId],
        offset: Long,
        observer: StreamObserver[v30.ExportAcsResponse],
        contractSynchronizerRenames: Map[SynchronizerId, SynchronizerId],
    ) extends GrpcAdminCommand[
          v30.ExportAcsRequest,
          CancellableContext,
          CancellableContext,
        ] {

      override type Svc = PartyManagementServiceStub

      override def createService(channel: ManagedChannel): PartyManagementServiceStub =
        v30.PartyManagementServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, v30.ExportAcsRequest] =
        Right(
          v30.ExportAcsRequest(
            parties.map(_.toLf).toSeq,
            filterSynchronizerId.map(_.toProtoPrimitive).getOrElse(""),
            offset,
            contractSynchronizerRenames.map { case (source, targetSynchronizerId) =>
              val target = v30.ExportAcsTargetSynchronizer(
                targetSynchronizerId.toProtoPrimitive
              )

              (source.toProtoPrimitive, target)
            },
          )
        )

      override protected def submitRequest(
          service: PartyManagementServiceStub,
          request: v30.ExportAcsRequest,
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

    final case class ExportAcsAtTimestamp(
        parties: Set[PartyId],
        filterSynchronizerId: SynchronizerId,
        topologyTransactionEffectiveTime: Instant,
        observer: StreamObserver[v30.ExportAcsAtTimestampResponse],
    ) extends GrpcAdminCommand[
          v30.ExportAcsAtTimestampRequest,
          CancellableContext,
          CancellableContext,
        ] {

      override type Svc = PartyManagementServiceStub

      override def createService(channel: ManagedChannel): PartyManagementServiceStub =
        v30.PartyManagementServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, v30.ExportAcsAtTimestampRequest] =
        Right(
          v30.ExportAcsAtTimestampRequest(
            parties.map(_.toLf).toSeq,
            filterSynchronizerId.toProtoPrimitive,
            Some(Timestamp(topologyTransactionEffectiveTime)),
          )
        )

      override protected def submitRequest(
          service: PartyManagementServiceStub,
          request: v30.ExportAcsAtTimestampRequest,
      ): Future[CancellableContext] = {
        val context = Context.current().withCancellation()
        context.run(() => service.exportAcsAtTimestamp(request, observer))
        Future.successful(context)
      }

      override protected def handleResponse(
          response: CancellableContext
      ): Either[String, CancellableContext] = Right(response)

      override def timeoutType: GrpcAdminCommand.TimeoutType =
        GrpcAdminCommand.DefaultUnboundedTimeout
    }

  }

  object ParticipantRepairManagement {

    // TODO(#24610) – Remove, replaced by ExportAcs
    final case class ExportAcsOld(
        parties: Set[PartyId],
        partiesOffboarding: Boolean,
        filterSynchronizerId: Option[SynchronizerId],
        timestamp: Option[Instant],
        observer: StreamObserver[v30.ExportAcsOldResponse],
        contractSynchronizerRenames: Map[SynchronizerId, (SynchronizerId, ProtocolVersion)],
        force: Boolean,
    ) extends GrpcAdminCommand[
          v30.ExportAcsOldRequest,
          CancellableContext,
          CancellableContext,
        ] {

      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        v30.ParticipantRepairServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, v30.ExportAcsOldRequest] =
        Right(
          v30.ExportAcsOldRequest(
            parties.map(_.toLf).toSeq,
            filterSynchronizerId.map(_.toProtoPrimitive).getOrElse(""),
            timestamp.map(Timestamp.apply),
            contractSynchronizerRenames.map {
              case (source, (targetSynchronizerId, targetProtocolVersion)) =>
                val targetSynchronizer = v30.ExportAcsOldRequest.TargetSynchronizer(
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
          request: v30.ExportAcsOldRequest,
      ): Future[CancellableContext] = {
        val context = Context.current().withCancellation()
        context.run(() => service.exportAcsOld(request, observer))
        Future.successful(context)
      }

      override protected def handleResponse(
          response: CancellableContext
      ): Either[String, CancellableContext] = Right(response)

      override def timeoutType: GrpcAdminCommand.TimeoutType =
        GrpcAdminCommand.DefaultUnboundedTimeout
    }

    // TODO(#24610) - Remove, replaced by ImportAcs
    final case class ImportAcsOld(
        acsChunk: ByteString,
        workflowIdPrefix: String,
        allowContractIdSuffixRecomputation: Boolean,
    ) extends GrpcAdminCommand[
          v30.ImportAcsOldRequest,
          v30.ImportAcsOldResponse,
          Map[LfContractId, LfContractId],
        ] {

      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        v30.ParticipantRepairServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, v30.ImportAcsOldRequest] =
        Right(
          v30.ImportAcsOldRequest(
            acsChunk,
            workflowIdPrefix,
            allowContractIdSuffixRecomputation,
          )
        )

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: v30.ImportAcsOldRequest,
      ): Future[v30.ImportAcsOldResponse] =
        GrpcStreamingUtils.streamToServer(
          service.importAcsOld,
          (bytes: Array[Byte]) =>
            v30.ImportAcsOldRequest(
              ByteString.copyFrom(bytes),
              workflowIdPrefix,
              allowContractIdSuffixRecomputation,
            ),
          request.acsSnapshot,
        )

      override protected def handleResponse(
          response: v30.ImportAcsOldResponse
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

    final case class ImportAcs(
        acsChunk: ByteString,
        workflowIdPrefix: String,
        contractIdImportMode: ContractIdImportMode,
    ) extends GrpcAdminCommand[
          v30.ImportAcsRequest,
          v30.ImportAcsResponse,
          Map[LfContractId, LfContractId],
        ] {

      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        v30.ParticipantRepairServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, v30.ImportAcsRequest] =
        Right(
          v30.ImportAcsRequest(
            acsChunk,
            workflowIdPrefix,
            contractIdImportMode.toProtoV30,
          )
        )

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: v30.ImportAcsRequest,
      ): Future[v30.ImportAcsResponse] =
        GrpcStreamingUtils.streamToServer(
          service.importAcs,
          (bytes: Array[Byte]) =>
            v30.ImportAcsRequest(
              ByteString.copyFrom(bytes),
              workflowIdPrefix,
              contractIdImportMode.toProtoV30,
            ),
          request.acsSnapshot,
        )

      override protected def handleResponse(
          response: v30.ImportAcsResponse
      ): Either[String, Map[LfContractId, LfContractId]] =
        response.contractIdMappings.toSeq
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
    ) extends GrpcAdminCommand[v30.PurgeContractsRequest, v30.PurgeContractsResponse, Unit] {

      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        v30.ParticipantRepairServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, v30.PurgeContractsRequest] =
        Right(
          v30.PurgeContractsRequest(
            synchronizerAlias = synchronizerAlias.toProtoPrimitive,
            contractIds = contracts.map(_.coid),
            ignoreAlreadyPurged = ignoreAlreadyPurged,
          )
        )

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: v30.PurgeContractsRequest,
      ): Future[v30.PurgeContractsResponse] = service.purgeContracts(request)

      override protected def handleResponse(
          response: v30.PurgeContractsResponse
      ): Either[String, Unit] =
        Either.unit
    }

    final case class MigrateSynchronizer(
        sourceSynchronizerAlias: SynchronizerAlias,
        targetSynchronizerConfig: SynchronizerConnectionConfig,
        force: Boolean,
    ) extends GrpcAdminCommand[
          v30.MigrateSynchronizerRequest,
          v30.MigrateSynchronizerResponse,
          Unit,
        ] {
      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        v30.ParticipantRepairServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: v30.MigrateSynchronizerRequest,
      ): Future[v30.MigrateSynchronizerResponse] = service.migrateSynchronizer(request)

      override protected def createRequest(): Either[String, v30.MigrateSynchronizerRequest] =
        Right(
          v30.MigrateSynchronizerRequest(
            sourceSynchronizerAlias.toProtoPrimitive,
            Some(targetSynchronizerConfig.toProtoV30),
            force = force,
          )
        )

      override protected def handleResponse(
          response: v30.MigrateSynchronizerResponse
      ): Either[String, Unit] =
        Either.unit

      // migration command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    final case class ChangeAssignation(
        sourceSynchronizerAlias: SynchronizerAlias,
        targetSynchronizerAlias: SynchronizerAlias,
        skipInactive: Boolean,
        contracts: Seq[(LfContractId, Option[ReassignmentCounter])],
    ) extends GrpcAdminCommand[v30.ChangeAssignationRequest, v30.ChangeAssignationResponse, Unit] {
      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        v30.ParticipantRepairServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: v30.ChangeAssignationRequest,
      ): Future[v30.ChangeAssignationResponse] = service.changeAssignation(request)

      override protected def createRequest(): Either[String, v30.ChangeAssignationRequest] =
        Right(
          v30.ChangeAssignationRequest(
            sourceSynchronizerAlias = sourceSynchronizerAlias.toProtoPrimitive,
            targetSynchronizerAlias = targetSynchronizerAlias.toProtoPrimitive,
            skipInactive = skipInactive,
            contracts = contracts.map { case (cid, reassignmentCounter) =>
              v30.ChangeAssignationRequest.Contract(
                cid.coid,
                reassignmentCounter.map(_.toProtoPrimitive),
              )
            },
          )
        )

      override protected def handleResponse(
          response: v30.ChangeAssignationResponse
      ): Either[String, Unit] =
        Either.unit

      // migration command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    final case class PurgeDeactivatedSynchronizer(synchronizerAlias: SynchronizerAlias)
        extends GrpcAdminCommand[
          v30.PurgeDeactivatedSynchronizerRequest,
          v30.PurgeDeactivatedSynchronizerResponse,
          Unit,
        ] {
      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        v30.ParticipantRepairServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: v30.PurgeDeactivatedSynchronizerRequest,
      ): Future[v30.PurgeDeactivatedSynchronizerResponse] =
        service.purgeDeactivatedSynchronizer(request)

      override protected def createRequest()
          : Either[String, v30.PurgeDeactivatedSynchronizerRequest] =
        Right(v30.PurgeDeactivatedSynchronizerRequest(synchronizerAlias.toProtoPrimitive))

      override protected def handleResponse(
          response: v30.PurgeDeactivatedSynchronizerResponse
      ): Either[String, Unit] = Either.unit
    }

    final case class IgnoreEvents(
        synchronizerId: SynchronizerId,
        fromInclusive: SequencerCounter,
        toInclusive: SequencerCounter,
        force: Boolean,
    ) extends GrpcAdminCommand[
          v30.IgnoreEventsRequest,
          v30.IgnoreEventsResponse,
          Unit,
        ] {
      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        v30.ParticipantRepairServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: v30.IgnoreEventsRequest,
      ): Future[v30.IgnoreEventsResponse] =
        service.ignoreEvents(request)

      override protected def createRequest(): Either[String, v30.IgnoreEventsRequest] =
        Right(
          v30.IgnoreEventsRequest(
            synchronizerId = synchronizerId.toProtoPrimitive,
            fromInclusive = fromInclusive.toProtoPrimitive,
            toInclusive = toInclusive.toProtoPrimitive,
            force = force,
          )
        )

      override protected def handleResponse(
          response: v30.IgnoreEventsResponse
      ): Either[String, Unit] =
        Either.unit
    }

    final case class UnignoreEvents(
        synchronizerId: SynchronizerId,
        fromInclusive: SequencerCounter,
        toInclusive: SequencerCounter,
        force: Boolean,
    ) extends GrpcAdminCommand[
          v30.UnignoreEventsRequest,
          v30.UnignoreEventsResponse,
          Unit,
        ] {
      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        v30.ParticipantRepairServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: v30.UnignoreEventsRequest,
      ): Future[v30.UnignoreEventsResponse] =
        service.unignoreEvents(request)

      override protected def createRequest(): Either[String, v30.UnignoreEventsRequest] =
        Right(
          v30.UnignoreEventsRequest(
            synchronizerId = synchronizerId.toProtoPrimitive,
            fromInclusive = fromInclusive.toProtoPrimitive,
            toInclusive = toInclusive.toProtoPrimitive,
            force = force,
          )
        )

      override protected def handleResponse(
          response: v30.UnignoreEventsResponse
      ): Either[String, Unit] = Either.unit
    }

    final case class RollbackUnassignment(
        unassignId: String,
        source: SynchronizerId,
        target: SynchronizerId,
    ) extends GrpcAdminCommand[
          v30.RollbackUnassignmentRequest,
          v30.RollbackUnassignmentResponse,
          Unit,
        ] {
      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        v30.ParticipantRepairServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, v30.RollbackUnassignmentRequest] =
        Right(
          v30.RollbackUnassignmentRequest(
            unassignId = unassignId,
            sourceSynchronizerId = source.toProtoPrimitive,
            targetSynchronizerId = target.toProtoPrimitive,
          )
        )

      override protected def submitRequest(
          service: ParticipantRepairServiceStub,
          request: v30.RollbackUnassignmentRequest,
      ): Future[v30.RollbackUnassignmentResponse] =
        service.rollbackUnassignment(request)

      override protected def handleResponse(
          response: v30.RollbackUnassignmentResponse
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
    ) extends GrpcAdminCommand[v30.PingRequest, v30.PingResponse, Either[String, Duration]] {
      override type Svc = PingServiceStub

      override def createService(channel: ManagedChannel): PingServiceStub =
        v30.PingServiceGrpc.stub(channel)

      override protected def createRequest(): Either[String, v30.PingRequest] =
        Right(
          v30.PingRequest(
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
          request: v30.PingRequest,
      ): Future[v30.PingResponse] =
        service.ping(request)

      override protected def handleResponse(
          response: v30.PingResponse
      ): Either[String, Either[String, Duration]] =
        response.response match {
          case v30.PingResponse.Response.Success(v30.PingSuccess(pingTime, responder)) =>
            Right(Right(Duration(pingTime, MILLISECONDS)))
          case v30.PingResponse.Response.Failure(failure) => Right(Left(failure.reason))
          case v30.PingResponse.Response.Empty => Left("Ping client: unexpected empty response")
        }

      override def timeoutType: TimeoutType = ServerEnforcedTimeout
    }

  }

  object SynchronizerConnectivity {

    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = SynchronizerConnectivityServiceStub

      override def createService(channel: ManagedChannel): SynchronizerConnectivityServiceStub =
        v30.SynchronizerConnectivityServiceGrpc.stub(channel)
    }

    final case class ReconnectSynchronizers(ignoreFailures: Boolean)
        extends Base[v30.ReconnectSynchronizersRequest, v30.ReconnectSynchronizersResponse, Unit] {

      override protected def createRequest(): Either[String, v30.ReconnectSynchronizersRequest] =
        Right(v30.ReconnectSynchronizersRequest(ignoreFailures = ignoreFailures))

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: v30.ReconnectSynchronizersRequest,
      ): Future[v30.ReconnectSynchronizersResponse] =
        service.reconnectSynchronizers(request)

      override protected def handleResponse(
          response: v30.ReconnectSynchronizersResponse
      ): Either[String, Unit] = Either.unit

      // depending on the confirmation timeout and the load, this might take a bit longer
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ReconnectSynchronizer(synchronizerAlias: SynchronizerAlias, retry: Boolean)
        extends Base[v30.ReconnectSynchronizerRequest, v30.ReconnectSynchronizerResponse, Boolean] {

      override protected def createRequest(): Either[String, v30.ReconnectSynchronizerRequest] =
        Right(v30.ReconnectSynchronizerRequest(synchronizerAlias.toProtoPrimitive, retry))

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: v30.ReconnectSynchronizerRequest,
      ): Future[v30.ReconnectSynchronizerResponse] =
        service.reconnectSynchronizer(request)

      override protected def handleResponse(
          response: v30.ReconnectSynchronizerResponse
      ): Either[String, Boolean] =
        Right(response.connectedSuccessfully)

      // can take long if we need to wait to become active
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class GetSynchronizerId(synchronizerAlias: SynchronizerAlias)
        extends Base[v30.GetSynchronizerIdRequest, v30.GetSynchronizerIdResponse, SynchronizerId] {

      override protected def createRequest(): Either[String, v30.GetSynchronizerIdRequest] =
        Right(v30.GetSynchronizerIdRequest(synchronizerAlias.toProtoPrimitive))

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: v30.GetSynchronizerIdRequest,
      ): Future[v30.GetSynchronizerIdResponse] =
        service.getSynchronizerId(request)

      override protected def handleResponse(
          response: v30.GetSynchronizerIdResponse
      ): Either[String, SynchronizerId] =
        SynchronizerId
          .fromProtoPrimitive(response.synchronizerId, "synchronizer_id")
          .leftMap(_.toString)
    }

    final case class DisconnectSynchronizer(synchronizerAlias: SynchronizerAlias)
        extends Base[v30.DisconnectSynchronizerRequest, v30.DisconnectSynchronizerResponse, Unit] {

      override protected def createRequest(): Either[String, v30.DisconnectSynchronizerRequest] =
        Right(v30.DisconnectSynchronizerRequest(synchronizerAlias.toProtoPrimitive))

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: v30.DisconnectSynchronizerRequest,
      ): Future[v30.DisconnectSynchronizerResponse] =
        service.disconnectSynchronizer(request)

      override protected def handleResponse(
          response: v30.DisconnectSynchronizerResponse
      ): Either[String, Unit] = Either.unit
    }

    final case class DisconnectAllSynchronizers()
        extends Base[
          v30.DisconnectAllSynchronizersRequest,
          v30.DisconnectAllSynchronizersResponse,
          Unit,
        ] {

      override protected def createRequest()
          : Either[String, v30.DisconnectAllSynchronizersRequest] =
        Right(v30.DisconnectAllSynchronizersRequest())

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: v30.DisconnectAllSynchronizersRequest,
      ): Future[v30.DisconnectAllSynchronizersResponse] =
        service.disconnectAllSynchronizers(request)

      override protected def handleResponse(
          response: v30.DisconnectAllSynchronizersResponse
      ): Either[String, Unit] = Either.unit
    }

    final case class ListConnectedSynchronizers()
        extends Base[
          v30.ListConnectedSynchronizersRequest,
          v30.ListConnectedSynchronizersResponse,
          Seq[
            ListConnectedSynchronizersResult
          ],
        ] {

      override protected def createRequest()
          : Either[String, v30.ListConnectedSynchronizersRequest] =
        Right(
          v30.ListConnectedSynchronizersRequest()
        )

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: v30.ListConnectedSynchronizersRequest,
      ): Future[v30.ListConnectedSynchronizersResponse] =
        service.listConnectedSynchronizers(request)

      override protected def handleResponse(
          response: v30.ListConnectedSynchronizersResponse
      ): Either[String, Seq[ListConnectedSynchronizersResult]] =
        response.connectedSynchronizers.traverse(
          ListConnectedSynchronizersResult.fromProtoV30(_).leftMap(_.toString)
        )

    }

    final case object ListRegisteredSynchronizers
        extends Base[
          v30.ListRegisteredSynchronizersRequest,
          v30.ListRegisteredSynchronizersResponse,
          Seq[
            (SynchronizerConnectionConfig, Boolean)
          ],
        ] {

      override protected def createRequest()
          : Either[String, v30.ListRegisteredSynchronizersRequest] =
        Right(
          v30.ListRegisteredSynchronizersRequest()
        )

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: v30.ListRegisteredSynchronizersRequest,
      ): Future[v30.ListRegisteredSynchronizersResponse] =
        service.listRegisteredSynchronizers(request)

      override protected def handleResponse(
          response: v30.ListRegisteredSynchronizersResponse
      ): Either[String, Seq[(SynchronizerConnectionConfig, Boolean)]] = {

        def mapRes(
            result: v30.ListRegisteredSynchronizersResponse.Result
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
    ) extends Base[v30.ConnectSynchronizerRequest, v30.ConnectSynchronizerResponse, Unit] {

      override protected def createRequest(): Either[String, v30.ConnectSynchronizerRequest] =
        Right(
          v30.ConnectSynchronizerRequest(
            config = Some(config.toProtoV30),
            sequencerConnectionValidation = sequencerConnectionValidation.toProtoV30,
          )
        )

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: v30.ConnectSynchronizerRequest,
      ): Future[v30.ConnectSynchronizerResponse] =
        service.connectSynchronizer(request)

      override protected def handleResponse(
          response: v30.ConnectSynchronizerResponse
      ): Either[String, Unit] = Either.unit

      // can take long if we need to wait to become active
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class RegisterSynchronizer(
        config: SynchronizerConnectionConfig,
        performHandshake: Boolean,
        sequencerConnectionValidation: SequencerConnectionValidation,
    ) extends Base[v30.RegisterSynchronizerRequest, v30.RegisterSynchronizerResponse, Unit] {

      override protected def createRequest(): Either[String, v30.RegisterSynchronizerRequest] = {
        val synchronizerConnection =
          if (performHandshake)
            v30.RegisterSynchronizerRequest.SynchronizerConnection.SYNCHRONIZER_CONNECTION_HANDSHAKE
          else v30.RegisterSynchronizerRequest.SynchronizerConnection.SYNCHRONIZER_CONNECTION_NONE

        Right(
          v30.RegisterSynchronizerRequest(
            config = Some(config.toProtoV30),
            synchronizerConnection = synchronizerConnection,
            sequencerConnectionValidation = sequencerConnectionValidation.toProtoV30,
          )
        )
      }

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: v30.RegisterSynchronizerRequest,
      ): Future[v30.RegisterSynchronizerResponse] =
        service.registerSynchronizer(request)

      override protected def handleResponse(
          response: v30.RegisterSynchronizerResponse
      ): Either[String, Unit] = Either.unit

      // can take long if we need to wait to become active
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ModifySynchronizerConnection(
        config: SynchronizerConnectionConfig,
        sequencerConnectionValidation: SequencerConnectionValidation,
    ) extends Base[v30.ModifySynchronizerRequest, v30.ModifySynchronizerResponse, Unit] {

      override protected def createRequest(): Either[String, v30.ModifySynchronizerRequest] =
        Right(
          v30.ModifySynchronizerRequest(
            newConfig = Some(config.toProtoV30),
            sequencerConnectionValidation = sequencerConnectionValidation.toProtoV30,
          )
        )

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: v30.ModifySynchronizerRequest,
      ): Future[v30.ModifySynchronizerResponse] =
        service.modifySynchronizer(request)

      override protected def handleResponse(
          response: v30.ModifySynchronizerResponse
      ): Either[String, Unit] =
        Either.unit
    }

    final case class Logout(synchronizerAlias: SynchronizerAlias)
        extends Base[v30.LogoutRequest, v30.LogoutResponse, Unit] {

      override protected def createRequest(): Either[String, v30.LogoutRequest] =
        Right(v30.LogoutRequest(synchronizerAlias.toProtoPrimitive))

      override protected def submitRequest(
          service: SynchronizerConnectivityServiceStub,
          request: v30.LogoutRequest,
      ): Future[v30.LogoutResponse] =
        service.logout(request)

      override protected def handleResponse(response: v30.LogoutResponse): Either[String, Unit] =
        Either.unit
    }
  }

  object Resources {
    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = ResourceManagementServiceStub

      override def createService(channel: ManagedChannel): ResourceManagementServiceStub =
        v30.ResourceManagementServiceGrpc.stub(channel)
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
        v30.InspectionServiceGrpc.stub(channel)
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
        commitment: AcsCommitment.HashedCommitmentType,
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
          AcsCommitment.hashedCommitmentTypeToProto(commitment),
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
        expectedSynchronizerId: SynchronizerId,
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
          expectedSynchronizerId.toProtoPrimitive,
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
            .traverse(receivedCmtPerSynchronizer =>
              for {
                synchronizerId <- SynchronizerId.fromString(
                  receivedCmtPerSynchronizer.synchronizerId
                )
                receivedCmts <- receivedCmtPerSynchronizer.received
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
        receivedCommitment: Option[AcsCommitment.HashedCommitmentType],
        localCommitment: Option[AcsCommitment.HashedCommitmentType],
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
        receivedCommitmentO <- cmt.receivedCommitment.traverse(
          AcsCommitment.hashedCommitmentTypeFromByteString(_).leftMap(_.toString)
        )
        ownCommitmentO <- cmt.ownCommitment.traverse(
          AcsCommitment.hashedCommitmentTypeFromByteString(_).leftMap(_.toString)
        )
      } yield ReceivedAcsCmt(
        period,
        participantId,
        receivedCommitmentO,
        ownCommitmentO,
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
            .traverse(sentCmtPerSynchronizer =>
              for {
                synchronizerId <- SynchronizerId.fromString(sentCmtPerSynchronizer.synchronizerId)
                sentCmts <- sentCmtPerSynchronizer.sent.map(fromProtoToSentAcsCmt).sequence
              } yield synchronizerId -> sentCmts
            )
            .map(_.toMap)
    }

    final case class SentAcsCmt(
        receivedCmtPeriod: CommitmentPeriod,
        destCounterParticipant: ParticipantId,
        sentCommitment: Option[AcsCommitment.HashedCommitmentType],
        receivedCommitment: Option[AcsCommitment.HashedCommitmentType],
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
        ownCommitmentO <- cmt.ownCommitment.traverse(
          AcsCommitment.hashedCommitmentTypeFromByteString(_).leftMap(_.toString)
        )
        receivedCommitmentO <- cmt.receivedCommitment.traverse(
          AcsCommitment.hashedCommitmentTypeFromByteString(_).leftMap(_.toString)
        )
      } yield SentAcsCmt(
        period,
        participantId,
        ownCommitmentO,
        receivedCommitmentO,
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
        v30.PruningServiceGrpc.stub(channel)
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
      override protected def createRequest(): Either[String, v30.PruneRequest] =
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
        synchronizers: Seq[SynchronizerId],
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
        synchronizers: Seq[SynchronizerId],
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
        synchronizers: Seq[SynchronizerId],
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
            synchronizers.map(_.toProtoPrimitive),
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
        v30.EnterpriseParticipantReplicationServiceGrpc.stub(channel)

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
          v30.TrafficControlStateRequest,
          v30.TrafficControlStateResponse,
          TrafficState,
        ] {
      override type Svc = v30.TrafficControlServiceGrpc.TrafficControlServiceStub

      override def createService(
          channel: ManagedChannel
      ): v30.TrafficControlServiceGrpc.TrafficControlServiceStub =
        v30.TrafficControlServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: v30.TrafficControlServiceGrpc.TrafficControlServiceStub,
          request: v30.TrafficControlStateRequest,
      ): Future[v30.TrafficControlStateResponse] =
        service.trafficControlState(request)

      override protected def createRequest(): Either[String, v30.TrafficControlStateRequest] =
        Right(
          v30.TrafficControlStateRequest(synchronizerId.toProtoPrimitive)
        )

      override protected def handleResponse(
          response: v30.TrafficControlStateResponse
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
          v30.ParticipantStatusRequest,
          v30.ParticipantStatusResponse,
          NodeStatus[ParticipantStatus],
        ] {

      override type Svc = ParticipantStatusServiceStub

      override def createService(channel: ManagedChannel): ParticipantStatusServiceStub =
        v30.ParticipantStatusServiceGrpc.stub(channel)

      override protected def submitRequest(
          service: ParticipantStatusServiceStub,
          request: v30.ParticipantStatusRequest,
      ): Future[v30.ParticipantStatusResponse] =
        service.participantStatus(request)

      override protected def createRequest(): Either[String, v30.ParticipantStatusRequest] = Right(
        v30.ParticipantStatusRequest()
      )

      override protected def handleResponse(
          response: v30.ParticipantStatusResponse
      ): Either[String, NodeStatus[ParticipantStatus]] =
        ParticipantStatus.fromProtoV30(response).leftMap(_.message)
    }
  }

}
