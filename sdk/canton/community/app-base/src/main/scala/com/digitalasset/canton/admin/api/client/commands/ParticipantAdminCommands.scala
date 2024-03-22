// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.implicits.*
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.Value
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  ServerEnforcedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.api.client.data.{
  DarMetadata,
  ListConnectedDomainsResult,
  ParticipantPruningSchedule,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.admin.grpc.{
  GrpcParticipantRepairService,
  TransferSearchResult,
}
import com.digitalasset.canton.participant.admin.v0.DomainConnectivityServiceGrpc.DomainConnectivityServiceStub
import com.digitalasset.canton.participant.admin.v0.EnterpriseParticipantReplicationServiceGrpc.EnterpriseParticipantReplicationServiceStub
import com.digitalasset.canton.participant.admin.v0.InspectionServiceGrpc.InspectionServiceStub
import com.digitalasset.canton.participant.admin.v0.PackageServiceGrpc.PackageServiceStub
import com.digitalasset.canton.participant.admin.v0.ParticipantRepairServiceGrpc.ParticipantRepairServiceStub
import com.digitalasset.canton.participant.admin.v0.PartyNameManagementServiceGrpc.PartyNameManagementServiceStub
import com.digitalasset.canton.participant.admin.v0.PingServiceGrpc.PingServiceStub
import com.digitalasset.canton.participant.admin.v0.PruningServiceGrpc.PruningServiceStub
import com.digitalasset.canton.participant.admin.v0.ResourceManagementServiceGrpc.ResourceManagementServiceStub
import com.digitalasset.canton.participant.admin.v0.TransferServiceGrpc.TransferServiceStub
import com.digitalasset.canton.participant.admin.v0.{ResourceLimits as _, *}
import com.digitalasset.canton.participant.admin.{ResourceLimits, v0}
import com.digitalasset.canton.participant.domain.DomainConnectionConfig as CDomainConnectionConfig
import com.digitalasset.canton.participant.sync.UpstreamOffsetConvert
import com.digitalasset.canton.protocol.{LfContractId, TransferId, v0 as v0proto}
import com.digitalasset.canton.pruning.admin
import com.digitalasset.canton.serialization.ProtoConverter.InstantConverter
import com.digitalasset.canton.topology.{DomainId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.MemberTrafficStatus
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainAlias, LedgerApplicationId, LedgerTransactionId, config}
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import com.google.protobuf.timestamp.Timestamp
import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver
import io.grpc.{Context, ManagedChannel}

import java.io.IOException
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Future, Promise, blocking}

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
      override def createRequest() = Right(ListPackagesRequest(limit.value))

      override def submitRequest(
          service: PackageServiceStub,
          request: ListPackagesRequest,
      ): Future[ListPackagesResponse] =
        service.listPackages(request)

      override def handleResponse(
          response: ListPackagesResponse
      ): Either[String, Seq[PackageDescription]] =
        Right(response.packageDescriptions)
    }

    final case class ListContents(packageId: String)
        extends PackageCommand[ListPackageContentsRequest, ListPackageContentsResponse, Seq[
          ModuleDescription
        ]] {
      override def createRequest() = Right(ListPackageContentsRequest(packageId))

      override def submitRequest(
          service: PackageServiceStub,
          request: ListPackageContentsRequest,
      ): Future[ListPackageContentsResponse] =
        service.listPackageContents(request)

      override def handleResponse(
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

      override def createRequest(): Either[String, UploadDarRequest] =
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

      override def submitRequest(
          service: PackageServiceStub,
          request: UploadDarRequest,
      ): Future[UploadDarResponse] =
        service.uploadDar(request)

      override def handleResponse(response: UploadDarResponse): Either[String, String] =
        response.value match {
          case UploadDarResponse.Value.Success(UploadDarResponse.Success(hash)) => Right(hash)
          case UploadDarResponse.Value.Failure(UploadDarResponse.Failure(msg)) => Left(msg)
          case UploadDarResponse.Value.Empty => Left("unexpected empty response")
        }

      // file can be big. checking & vetting might take a while
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class RemovePackage(
        packageId: String,
        force: Boolean,
    ) extends PackageCommand[RemovePackageRequest, RemovePackageResponse, Unit] {

      override def createRequest() = Right(RemovePackageRequest(packageId, force))

      override def submitRequest(
          service: PackageServiceStub,
          request: RemovePackageRequest,
      ): Future[RemovePackageResponse] =
        service.removePackage(request)

      override def handleResponse(
          response: RemovePackageResponse
      ): Either[String, Unit] = {
        response.success match {
          case None => Left("unexpected empty response")
          case Some(_success) => Right(())
        }
      }

    }

    final case class GetDar(
        darHash: Option[String],
        destinationDirectory: Option[String],
        logger: TracedLogger,
    ) extends PackageCommand[GetDarRequest, GetDarResponse, Path] {
      override def createRequest(): Either[String, GetDarRequest] =
        for {
          _ <- destinationDirectory.toRight("DAR destination directory not provided")
          hash <- darHash.toRight("DAR hash not provided")
        } yield GetDarRequest(hash)

      override def submitRequest(
          service: PackageServiceStub,
          request: GetDarRequest,
      ): Future[GetDarResponse] =
        service.getDar(request)

      override def handleResponse(response: GetDarResponse): Either[String, Path] =
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
      override def createRequest() = Right(ListDarContentsRequest(darId))

      override def submitRequest(
          service: PackageServiceStub,
          request: ListDarContentsRequest,
      ): Future[ListDarContentsResponse] =
        service.listDarContents(request)

      override def handleResponse(
          response: ListDarContentsResponse
      ): Either[String, DarMetadata] =
        DarMetadata.fromProtoV0(response).leftMap(_.toString)

    }

    final case class RemoveDar(
        darHash: String
    ) extends PackageCommand[RemoveDarRequest, RemoveDarResponse, Unit] {

      override def createRequest() = Right(RemoveDarRequest(darHash))

      override def submitRequest(
          service: PackageServiceStub,
          request: RemoveDarRequest,
      ): Future[RemoveDarResponse] =
        service.removeDar(request)

      override def handleResponse(
          response: RemoveDarResponse
      ): Either[String, Unit] = {
        response.success match {
          case None => Left("unexpected empty response")
          case Some(success) => Right(())
        }
      }

    }

    final case class VetDar(darDash: String, synchronize: Boolean)
        extends PackageCommand[VetDarRequest, VetDarResponse, Unit] {
      override def createRequest(): Either[String, VetDarRequest] = Right(
        VetDarRequest(darDash, synchronize)
      )

      override def submitRequest(
          service: PackageServiceStub,
          request: VetDarRequest,
      ): Future[VetDarResponse] = service.vetDar(request)

      override def handleResponse(response: VetDarResponse): Either[String, Unit] =
        Right(())
    }

    // TODO(#14432): Add `synchronize` flag which makes the call block until the unvetting operation
    //               is observed by the participant on all connected domains.
    final case class UnvetDar(darDash: String)
        extends PackageCommand[UnvetDarRequest, UnvetDarResponse, Unit] {

      override def createRequest(): Either[String, UnvetDarRequest] = Right(
        UnvetDarRequest(darDash)
      )

      override def submitRequest(
          service: PackageServiceStub,
          request: UnvetDarRequest,
      ): Future[UnvetDarResponse] = service.unvetDar(request)

      override def handleResponse(response: UnvetDarResponse): Either[String, Unit] =
        Right(())
    }

    final case class ListDars(limit: PositiveInt)
        extends PackageCommand[ListDarsRequest, ListDarsResponse, Seq[DarDescription]] {
      override def createRequest(): Either[String, ListDarsRequest] = Right(
        ListDarsRequest(limit.value)
      )

      override def submitRequest(
          service: PackageServiceStub,
          request: ListDarsRequest,
      ): Future[ListDarsResponse] =
        service.listDars(request)

      override def handleResponse(response: ListDarsResponse): Either[String, Seq[DarDescription]] =
        Right(response.dars)
    }

  }

  object PartyNameManagement {

    final case class SetPartyDisplayName(partyId: PartyId, displayName: String)
        extends GrpcAdminCommand[SetPartyDisplayNameRequest, SetPartyDisplayNameResponse, Unit] {
      override type Svc = PartyNameManagementServiceStub

      override def createService(channel: ManagedChannel): PartyNameManagementServiceStub =
        PartyNameManagementServiceGrpc.stub(channel)

      override def createRequest(): Either[String, SetPartyDisplayNameRequest] =
        Right(
          SetPartyDisplayNameRequest(
            partyId = partyId.uid.toProtoPrimitive,
            displayName = displayName,
          )
        )

      override def submitRequest(
          service: PartyNameManagementServiceStub,
          request: SetPartyDisplayNameRequest,
      ): Future[SetPartyDisplayNameResponse] =
        service.setPartyDisplayName(request)

      override def handleResponse(response: SetPartyDisplayNameResponse): Either[String, Unit] =
        Right(())

    }

  }

  object ParticipantRepairManagement {

    // TODO(i14441): Remove deprecated ACS download / upload functionality
    @deprecated("Use ExportAcs", since = "2.8.0")
    final case class Download(
        parties: Set[PartyId],
        filterDomainId: String,
        timestamp: Option[Instant],
        protocolVersion: Option[ProtocolVersion],
        chunkSize: Option[PositiveInt],
        observer: StreamObserver[AcsSnapshotChunk],
        gzipFormat: Boolean,
        contractDomainRenames: Map[DomainId, DomainId],
    ) extends GrpcAdminCommand[
          DownloadRequest,
          CancellableContext,
          CancellableContext,
        ] {

      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override def createRequest(): Either[String, DownloadRequest] = {
        Right(
          DownloadRequest(
            parties.map(_.toLf).toSeq,
            filterDomainId,
            timestamp.map(Timestamp.apply),
            protocolVersion.map(_.toProtoPrimitiveS).getOrElse(""),
            chunkSize.map(_.value),
            gzipFormat,
            contractDomainRenames.map { case (source, target) =>
              (source.toProtoPrimitive, target.toProtoPrimitive)
            },
          )
        )
      }

      override def submitRequest(
          service: ParticipantRepairServiceStub,
          request: DownloadRequest,
      ): Future[CancellableContext] = {
        val context = Context.current().withCancellation()
        context.run(() => service.download(request, observer))
        Future.successful(context)
      }

      override def handleResponse(
          response: CancellableContext
      ): Either[String, CancellableContext] = Right(response)

    }

    sealed trait StreamingMachinery[Req, Resp] {
      def stream(
          load: StreamObserver[Resp] => StreamObserver[Req],
          requestBuilder: Array[Byte] => Req,
          snapshot: ByteString,
      ): Future[Resp] = {
        val requestComplete = Promise[Resp]()
        val ref = new AtomicReference[Option[Resp]](None)

        val responseObserver = new StreamObserver[Resp] {
          override def onNext(value: Resp): Unit = {
            ref.set(Some(value))
          }

          override def onError(t: Throwable): Unit = requestComplete.failure(t)

          override def onCompleted(): Unit = {
            ref.get() match {
              case Some(response) => requestComplete.success(response)
              case None =>
                requestComplete.failure(
                  io.grpc.Status.CANCELLED
                    .withDescription("Server completed the request before providing a response")
                    .asRuntimeException()
                )
            }

          }
        }
        val requestObserver = load(responseObserver)

        snapshot.toByteArray
          .grouped(GrpcParticipantRepairService.DefaultChunkSize.value)
          .foreach { bytes =>
            blocking {
              requestObserver.onNext(requestBuilder(bytes))
            }
          }
        requestObserver.onCompleted()
        requestComplete.future
      }
    }

    // TODO(i14441): Remove deprecated ACS download / upload functionality
    @deprecated("Use ImportAcs", since = "2.8.0")
    final case class Upload(acsChunk: ByteString, gzip: Boolean)
        extends GrpcAdminCommand[UploadRequest, UploadResponse, Unit]
        with StreamingMachinery[UploadRequest, UploadResponse] {

      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override def createRequest(): Either[String, UploadRequest] = {
        Right(UploadRequest(acsChunk, gzip))
      }

      override def submitRequest(
          service: ParticipantRepairServiceStub,
          request: UploadRequest,
      ): Future[UploadResponse] = {
        stream(
          service.upload,
          (bytes: Array[Byte]) => UploadRequest(ByteString.copyFrom(bytes), gzip),
          request.acsSnapshot,
        )
      }

      override def handleResponse(response: UploadResponse): Either[String, Unit] = {
        Right(())
      }
    }

    final case class ExportAcs(
        parties: Set[PartyId],
        filterDomainId: Option[DomainId],
        timestamp: Option[Instant],
        observer: StreamObserver[ExportAcsResponse],
        contractDomainRenames: Map[DomainId, (DomainId, ProtocolVersion)],
    ) extends GrpcAdminCommand[
          ExportAcsRequest,
          CancellableContext,
          CancellableContext,
        ] {

      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override def createRequest(): Either[String, ExportAcsRequest] = {
        Right(
          ExportAcsRequest(
            parties.map(_.toLf).toSeq,
            filterDomainId.map(_.toProtoPrimitive).getOrElse(""),
            timestamp.map(Timestamp.apply),
            contractDomainRenames.map { case (source, (targetDomainId, targetProtocolVersion)) =>
              val targetDomain = ExportAcsRequest.TargetDomain(
                domainId = targetDomainId.toProtoPrimitive,
                protocolVersion = targetProtocolVersion.toProtoPrimitive,
              )

              (source.toProtoPrimitive, targetDomain)
            },
          )
        )
      }

      override def submitRequest(
          service: ParticipantRepairServiceStub,
          request: ExportAcsRequest,
      ): Future[CancellableContext] = {
        val context = Context.current().withCancellation()
        context.run(() => service.exportAcs(request, observer))
        Future.successful(context)
      }

      override def handleResponse(
          response: CancellableContext
      ): Either[String, CancellableContext] = Right(response)

    }

    final case class ImportAcs(acsChunk: ByteString, workflowIdPrefix: String)
        extends GrpcAdminCommand[ImportAcsRequest, ImportAcsResponse, Unit]
        with StreamingMachinery[ImportAcsRequest, ImportAcsResponse] {

      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override def createRequest(): Either[String, ImportAcsRequest] = {
        Right(ImportAcsRequest(acsChunk, workflowIdPrefix))
      }

      override def submitRequest(
          service: ParticipantRepairServiceStub,
          request: ImportAcsRequest,
      ): Future[ImportAcsResponse] = {
        stream(
          service.importAcs,
          (bytes: Array[Byte]) => ImportAcsRequest(ByteString.copyFrom(bytes), workflowIdPrefix),
          request.acsSnapshot,
        )
      }

      override def handleResponse(response: ImportAcsResponse): Either[String, Unit] = {
        Right(())
      }
    }

    final case class PurgeContracts(
        domain: DomainAlias,
        contracts: Seq[LfContractId],
        ignoreAlreadyPurged: Boolean,
    ) extends GrpcAdminCommand[PurgeContractsRequest, PurgeContractsResponse, Unit] {

      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override def createRequest(): Either[String, PurgeContractsRequest] = {
        Right(
          PurgeContractsRequest(
            domain = domain.toProtoPrimitive,
            contractIds = contracts.map(_.coid),
            ignoreAlreadyPurged = ignoreAlreadyPurged,
          )
        )
      }

      override def submitRequest(
          service: ParticipantRepairServiceStub,
          request: PurgeContractsRequest,
      ): Future[PurgeContractsResponse] = service.purgeContracts(request)

      override def handleResponse(response: PurgeContractsResponse): Either[String, Unit] =
        Right(())
    }

    final case class MigrateDomain(
        sourceDomainAlias: DomainAlias,
        targetDomainConfig: CDomainConnectionConfig,
    ) extends GrpcAdminCommand[MigrateDomainRequest, MigrateDomainResponse, Unit] {
      override type Svc = ParticipantRepairServiceStub

      override def createService(channel: ManagedChannel): ParticipantRepairServiceStub =
        ParticipantRepairServiceGrpc.stub(channel)

      override def submitRequest(
          service: ParticipantRepairServiceStub,
          request: MigrateDomainRequest,
      ): Future[MigrateDomainResponse] = service.migrateDomain(request)

      override def createRequest(): Either[String, MigrateDomainRequest] =
        Right(
          MigrateDomainRequest(
            sourceDomainAlias.toProtoPrimitive,
            Some(targetDomainConfig.toProtoV0),
          )
        )

      override def handleResponse(response: MigrateDomainResponse): Either[String, Unit] = Right(())

      // migration command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }
  }

  object Ping {

    final case class Ping(
        targets: Set[String],
        validators: Set[String],
        timeoutMillis: Long,
        levels: Long,
        gracePeriodMillis: Long,
        workflowId: String,
        id: String,
    ) extends GrpcAdminCommand[PingRequest, PingResponse, Option[Duration]] {
      override type Svc = PingServiceStub

      override def createService(channel: ManagedChannel): PingServiceStub =
        PingServiceGrpc.stub(channel)

      override def createRequest(): Either[String, PingRequest] =
        Right(
          PingRequest(
            targets.toSeq,
            validators.toSeq,
            timeoutMillis,
            levels,
            gracePeriodMillis,
            workflowId,
            id,
          )
        )

      override def submitRequest(
          service: PingServiceStub,
          request: PingRequest,
      ): Future[PingResponse] =
        service.ping(request)

      override def handleResponse(response: PingResponse): Either[String, Option[Duration]] =
        response.response match {
          case PingResponse.Response.Success(PingSuccess(pingTime, responder)) =>
            Right(Some(Duration(pingTime, MILLISECONDS)))
          case PingResponse.Response.Failure(_ex) => Right(None)
          case PingResponse.Response.Empty => Left("Ping client: unexpected empty response")
        }

      override def timeoutType: TimeoutType = ServerEnforcedTimeout
    }

  }

  object DomainConnectivity {

    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = DomainConnectivityServiceStub

      override def createService(channel: ManagedChannel): DomainConnectivityServiceStub =
        DomainConnectivityServiceGrpc.stub(channel)
    }

    final case class ReconnectDomains(ignoreFailures: Boolean)
        extends Base[ReconnectDomainsRequest, ReconnectDomainsResponse, Unit] {

      override def createRequest(): Either[String, ReconnectDomainsRequest] =
        Right(ReconnectDomainsRequest(ignoreFailures = ignoreFailures))

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: ReconnectDomainsRequest,
      ): Future[ReconnectDomainsResponse] =
        service.reconnectDomains(request)

      override def handleResponse(response: ReconnectDomainsResponse): Either[String, Unit] = Right(
        ()
      )

      // depending on the confirmation timeout and the load, this might take a bit longer
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ConnectDomain(domainAlias: DomainAlias, retry: Boolean)
        extends Base[ConnectDomainRequest, ConnectDomainResponse, Boolean] {

      override def createRequest(): Either[String, ConnectDomainRequest] =
        Right(ConnectDomainRequest(domainAlias.toProtoPrimitive, retry))

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: ConnectDomainRequest,
      ): Future[ConnectDomainResponse] =
        service.connectDomain(request)

      override def handleResponse(response: ConnectDomainResponse): Either[String, Boolean] =
        Right(response.connectedSuccessfully)

      // can take long if we need to wait to become active
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class GetDomainId(domainAlias: DomainAlias)
        extends Base[GetDomainIdRequest, GetDomainIdResponse, DomainId] {

      override def createRequest(): Either[String, GetDomainIdRequest] =
        Right(GetDomainIdRequest(domainAlias.toProtoPrimitive))

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: GetDomainIdRequest,
      ): Future[GetDomainIdResponse] =
        service.getDomainId(request)

      override def handleResponse(response: GetDomainIdResponse): Either[String, DomainId] =
        DomainId.fromProtoPrimitive(response.domainId, "domain_id").leftMap(_.toString)
    }

    final case class DisconnectDomain(domainAlias: DomainAlias)
        extends Base[DisconnectDomainRequest, DisconnectDomainResponse, Unit] {

      override def createRequest(): Either[String, DisconnectDomainRequest] =
        Right(DisconnectDomainRequest(domainAlias.toProtoPrimitive))

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: DisconnectDomainRequest,
      ): Future[DisconnectDomainResponse] =
        service.disconnectDomain(request)

      override def handleResponse(response: DisconnectDomainResponse): Either[String, Unit] = Right(
        ()
      )
    }

    final case class ListConnectedDomains()
        extends Base[ListConnectedDomainsRequest, ListConnectedDomainsResponse, Seq[
          ListConnectedDomainsResult
        ]] {

      override def createRequest(): Either[String, ListConnectedDomainsRequest] = Right(
        ListConnectedDomainsRequest()
      )

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: ListConnectedDomainsRequest,
      ): Future[ListConnectedDomainsResponse] =
        service.listConnectedDomains(request)

      override def handleResponse(
          response: ListConnectedDomainsResponse
      ): Either[String, Seq[ListConnectedDomainsResult]] =
        response.connectedDomains.traverse(
          ListConnectedDomainsResult.fromProtoV0(_).leftMap(_.toString)
        )

    }

    final case object ListConfiguredDomains
        extends Base[ListConfiguredDomainsRequest, ListConfiguredDomainsResponse, Seq[
          (CDomainConnectionConfig, Boolean)
        ]] {

      override def createRequest(): Either[String, ListConfiguredDomainsRequest] = Right(
        ListConfiguredDomainsRequest()
      )

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: ListConfiguredDomainsRequest,
      ): Future[ListConfiguredDomainsResponse] =
        service.listConfiguredDomains(request)

      override def handleResponse(
          response: ListConfiguredDomainsResponse
      ): Either[String, Seq[(CDomainConnectionConfig, Boolean)]] = {

        def mapRes(
            result: ListConfiguredDomainsResponse.Result
        ): Either[String, (CDomainConnectionConfig, Boolean)] =
          for {
            configP <- result.config.toRight("Server has sent empty config")
            config <- CDomainConnectionConfig.fromProtoV0(configP).leftMap(_.toString)
          } yield (config, result.connected)

        response.results.traverse(mapRes)
      }
    }

    final case class RegisterDomain(config: CDomainConnectionConfig)
        extends Base[RegisterDomainRequest, RegisterDomainResponse, Unit] {

      override def createRequest(): Either[String, RegisterDomainRequest] =
        Right(RegisterDomainRequest(add = Some(config.toProtoV0)))

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: RegisterDomainRequest,
      ): Future[RegisterDomainResponse] =
        service.registerDomain(request)

      override def handleResponse(response: RegisterDomainResponse): Either[String, Unit] = Right(
        ()
      )

      // can take long if we need to wait to become active
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ModifyDomainConnection(config: CDomainConnectionConfig)
        extends Base[ModifyDomainRequest, ModifyDomainResponse, Unit] {

      override def createRequest(): Either[String, ModifyDomainRequest] =
        Right(ModifyDomainRequest(modify = Some(config.toProtoV0)))

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: ModifyDomainRequest,
      ): Future[ModifyDomainResponse] =
        service.modifyDomain(request)

      override def handleResponse(response: ModifyDomainResponse): Either[String, Unit] = Right(())

    }

    final case class GetAgreement(domainAlias: DomainAlias)
        extends Base[GetAgreementRequest, GetAgreementResponse, Option[(Agreement, Boolean)]] {

      override def createRequest(): Either[String, GetAgreementRequest] = Right(
        GetAgreementRequest(domainAlias.unwrap)
      )

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: GetAgreementRequest,
      ): Future[GetAgreementResponse] =
        service.getAgreement(request)

      override def handleResponse(
          response: GetAgreementResponse
      ): Either[String, Option[(Agreement, Boolean)]] =
        Right(response.agreement.map(ag => (ag, response.accepted)))
    }

    final case class AcceptAgreement(domainAlias: DomainAlias, agreementId: String)
        extends Base[AcceptAgreementRequest, AcceptAgreementResponse, Unit] {

      override def createRequest(): Either[String, AcceptAgreementRequest] =
        Right(AcceptAgreementRequest(domainAlias.unwrap, agreementId))

      override def submitRequest(
          service: DomainConnectivityServiceStub,
          request: AcceptAgreementRequest,
      ): Future[AcceptAgreementResponse] =
        service.acceptAgreement(request)

      override def handleResponse(response: AcceptAgreementResponse): Either[String, Unit] = Right(
        ()
      )
    }

  }

  object Transfer {

    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = TransferServiceStub

      override def createService(channel: ManagedChannel): TransferServiceStub =
        TransferServiceGrpc.stub(channel)
    }

    final case class TransferOut(
        submittingParty: PartyId,
        contractId: LfContractId,
        sourceDomain: DomainAlias,
        targetDomain: DomainAlias,
        applicationId: LedgerApplicationId,
        submissionId: String,
        workflowId: String,
        commandId: String,
    ) extends Base[AdminTransferOutRequest, AdminTransferOutResponse, TransferId] {
      override def createRequest(): Either[String, AdminTransferOutRequest] =
        Right(
          AdminTransferOutRequest(
            submittingParty = submittingParty.toLf,
            originDomain = sourceDomain.toProtoPrimitive,
            targetDomain = targetDomain.toProtoPrimitive,
            contractId = contractId.coid,
            applicationId = applicationId,
            submissionId = submissionId,
            workflowId = workflowId,
            commandId = commandId,
          )
        )

      override def submitRequest(
          service: TransferServiceStub,
          request: AdminTransferOutRequest,
      ): Future[AdminTransferOutResponse] =
        service.transferOut(request)

      override def handleResponse(response: AdminTransferOutResponse): Either[String, TransferId] =
        response match {
          case AdminTransferOutResponse(Some(transferIdP)) =>
            TransferId.fromProtoV0(transferIdP).leftMap(_.toString)
          case AdminTransferOutResponse(None) => Left("Empty TransferOutResponse")
        }
    }

    final case class TransferIn(
        submittingParty: PartyId,
        transferId: v0proto.TransferId,
        targetDomain: DomainAlias,
        applicationId: LedgerApplicationId,
        submissionId: String,
        workflowId: String,
        commandId: String,
    ) extends Base[AdminTransferInRequest, AdminTransferInResponse, Unit] {

      override def createRequest(): Either[String, AdminTransferInRequest] =
        Right(
          AdminTransferInRequest(
            submittingPartyId = submittingParty.toLf,
            transferId = Some(transferId),
            targetDomain = targetDomain.toProtoPrimitive,
            applicationId = applicationId,
            submissionId = submissionId,
            workflowId = workflowId,
            commandId = commandId,
          )
        )

      override def submitRequest(
          service: TransferServiceStub,
          request: AdminTransferInRequest,
      ): Future[AdminTransferInResponse] =
        service.transferIn(request)

      override def handleResponse(response: AdminTransferInResponse): Either[String, Unit] = Right(
        ()
      )

    }

    final case class TransferSearch(
        targetDomain: DomainAlias,
        sourceDomainFilter: Option[DomainAlias],
        timestampFilter: Option[Instant],
        submittingPartyFilter: Option[PartyId],
        limit0: Int,
    ) extends Base[
          AdminTransferSearchQuery,
          AdminTransferSearchResponse,
          Seq[TransferSearchResult],
        ] {

      override def createRequest(): Either[String, AdminTransferSearchQuery] =
        Right(
          AdminTransferSearchQuery(
            searchDomain = targetDomain.toProtoPrimitive,
            filterOriginDomain = sourceDomainFilter.map(_.toProtoPrimitive).getOrElse(""),
            filterTimestamp =
              timestampFilter.map((value: Instant) => InstantConverter.toProtoPrimitive(value)),
            filterSubmittingParty = submittingPartyFilter.fold("")(_.toLf),
            limit = limit0.toLong,
          )
        )

      override def submitRequest(
          service: TransferServiceStub,
          request: AdminTransferSearchQuery,
      ): Future[AdminTransferSearchResponse] =
        service.transferSearch(request)

      override def handleResponse(
          response: AdminTransferSearchResponse
      ): Either[String, Seq[TransferSearchResult]] =
        response match {
          case AdminTransferSearchResponse(results) =>
            results.traverse(TransferSearchResult.fromProtoV0).leftMap(_.toString)
        }

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }
  }

  object Resources {
    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = ResourceManagementServiceStub

      override def createService(channel: ManagedChannel): ResourceManagementServiceStub =
        ResourceManagementServiceGrpc.stub(channel)
    }

    final case class GetResourceLimits() extends Base[Empty, v0.ResourceLimits, ResourceLimits] {
      override def createRequest(): Either[String, Empty] = Right(Empty())

      override def submitRequest(
          service: ResourceManagementServiceStub,
          request: Empty,
      ): Future[v0.ResourceLimits] =
        service.getResourceLimits(request)

      override def handleResponse(response: v0.ResourceLimits): Either[String, ResourceLimits] = {
        Right(ResourceLimits.fromProtoV0(response))
      }
    }

    final case class SetResourceLimits(limits: ResourceLimits)
        extends Base[v0.ResourceLimits, Empty, Unit] {
      override def createRequest(): Either[String, v0.ResourceLimits] = Right(limits.toProtoV0)

      override def submitRequest(
          service: ResourceManagementServiceStub,
          request: v0.ResourceLimits,
      ): Future[Empty] =
        service.updateResourceLimits(request)

      override def handleResponse(response: Empty): Either[String, Unit] = Right(())
    }
  }

  object Inspection {

    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = InspectionServiceStub

      override def createService(channel: ManagedChannel): InspectionServiceStub =
        InspectionServiceGrpc.stub(channel)
    }

    final case class LookupContractDomain(contractIds: Set[LfContractId])
        extends Base[
          v0.LookupContractDomain.Request,
          v0.LookupContractDomain.Response,
          Map[LfContractId, String],
        ] {
      override def createRequest() = Right(
        v0.LookupContractDomain.Request(contractIds.toSeq.map(_.coid))
      )

      override def submitRequest(
          service: InspectionServiceStub,
          request: v0.LookupContractDomain.Request,
      ): Future[v0.LookupContractDomain.Response] =
        service.lookupContractDomain(request)

      override def handleResponse(
          response: v0.LookupContractDomain.Response
      ): Either[String, Map[LfContractId, String]] = Right(
        response.results.map { case (id, domain) =>
          LfContractId.assertFromString(id) -> domain
        }
      )

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class LookupTransactionDomain(transactionId: LedgerTransactionId)
        extends Base[
          v0.LookupTransactionDomain.Request,
          v0.LookupTransactionDomain.Response,
          DomainId,
        ] {
      override def createRequest() = Right(v0.LookupTransactionDomain.Request(transactionId))

      override def submitRequest(
          service: InspectionServiceStub,
          request: v0.LookupTransactionDomain.Request,
      ): Future[v0.LookupTransactionDomain.Response] =
        service.lookupTransactionDomain(request)

      override def handleResponse(
          response: v0.LookupTransactionDomain.Response
      ): Either[String, DomainId] =
        DomainId.fromString(response.domainId)

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class LookupOffsetByTime(ts: Timestamp)
        extends Base[v0.LookupOffsetByTime.Request, v0.LookupOffsetByTime.Response, String] {
      override def createRequest() = Right(v0.LookupOffsetByTime.Request(Some(ts)))

      override def submitRequest(
          service: InspectionServiceStub,
          request: v0.LookupOffsetByTime.Request,
      ): Future[v0.LookupOffsetByTime.Response] =
        service.lookupOffsetByTime(request)

      override def handleResponse(
          response: v0.LookupOffsetByTime.Response
      ): Either[String, String] =
        Right(response.offset)
    }

    final case class LookupOffsetByIndex(index: Long)
        extends Base[v0.LookupOffsetByIndex.Request, v0.LookupOffsetByIndex.Response, String] {
      override def createRequest() = Right(v0.LookupOffsetByIndex.Request(index))

      override def submitRequest(
          service: InspectionServiceStub,
          request: v0.LookupOffsetByIndex.Request,
      ): Future[v0.LookupOffsetByIndex.Response] =
        service.lookupOffsetByIndex(request)

      override def handleResponse(
          response: v0.LookupOffsetByIndex.Response
      ): Either[String, String] =
        Right(response.offset)
    }

  }

  object Pruning {
    abstract class Base[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = PruningServiceStub

      override def createService(channel: ManagedChannel): PruningServiceStub =
        PruningServiceGrpc.stub(channel)
    }

    final case class GetSafePruningOffsetCommand(beforeOrAt: Instant, ledgerEnd: LedgerOffset)
        extends Base[v0.GetSafePruningOffsetRequest, v0.GetSafePruningOffsetResponse, Option[
          LedgerOffset
        ]] {

      override def createRequest(): Either[String, v0.GetSafePruningOffsetRequest] =
        for {
          beforeOrAt <- CantonTimestamp.fromInstant(beforeOrAt)
          ledgerEnd <- ledgerEnd.value match {
            case Value.Absolute(value) => Right(value)
            case other => Left(s"Unable to convert ledger_end `$other` to absolute value")
          }
        } yield v0.GetSafePruningOffsetRequest(Some(beforeOrAt.toProtoPrimitive), ledgerEnd)

      override def submitRequest(
          service: PruningServiceStub,
          request: v0.GetSafePruningOffsetRequest,
      ): Future[v0.GetSafePruningOffsetResponse] = service.getSafePruningOffset(request)

      override def handleResponse(
          response: v0.GetSafePruningOffsetResponse
      ): Either[String, Option[LedgerOffset]] = response.response match {
        case v0.GetSafePruningOffsetResponse.Response.Empty => Left("Unexpected empty response")

        case v0.GetSafePruningOffsetResponse.Response.SafePruningOffset(offset) =>
          Right(Some(UpstreamOffsetConvert.toLedgerOffset(offset)))

        case v0.GetSafePruningOffsetResponse.Response.NoSafePruningOffset(_) => Right(None)
      }
    }

    final case class PruneInternallyCommand(pruneUpTo: LedgerOffset)
        extends Base[v0.PruneRequest, v0.PruneResponse, Unit] {
      override def createRequest() =
        pruneUpTo.value.absolute
          .toRight("The pruneUpTo ledger offset needs to be absolute")
          .map(v0.PruneRequest(_))

      override def submitRequest(
          service: PruningServiceStub,
          request: v0.PruneRequest,
      ): Future[v0.PruneResponse] =
        service.prune(request)

      override def handleResponse(response: v0.PruneResponse): Either[String, Unit] = Right(())
    }

    final case class SetParticipantScheduleCommand(
        cron: String,
        maxDuration: config.PositiveDurationSeconds,
        retention: config.PositiveDurationSeconds,
        pruneInternallyOnly: Boolean,
    ) extends Base[
          admin.v0.SetParticipantSchedule.Request,
          admin.v0.SetParticipantSchedule.Response,
          Unit,
        ] {
      override def createRequest(): Right[String, admin.v0.SetParticipantSchedule.Request] =
        Right(
          admin.v0.SetParticipantSchedule.Request(
            Some(
              admin.v0.ParticipantPruningSchedule(
                Some(
                  admin.v0.PruningSchedule(
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

      override def submitRequest(
          service: Svc,
          request: admin.v0.SetParticipantSchedule.Request,
      ): Future[admin.v0.SetParticipantSchedule.Response] = service.setParticipantSchedule(request)

      override def handleResponse(
          response: admin.v0.SetParticipantSchedule.Response
      ): Either[String, Unit] =
        response match {
          case admin.v0.SetParticipantSchedule.Response() => Right(())
        }
    }

    final case class GetParticipantScheduleCommand()
        extends Base[
          admin.v0.GetParticipantSchedule.Request,
          admin.v0.GetParticipantSchedule.Response,
          Option[ParticipantPruningSchedule],
        ] {
      override def createRequest(): Right[String, admin.v0.GetParticipantSchedule.Request] =
        Right(
          admin.v0.GetParticipantSchedule.Request()
        )

      override def submitRequest(
          service: Svc,
          request: admin.v0.GetParticipantSchedule.Request,
      ): Future[admin.v0.GetParticipantSchedule.Response] = service.getParticipantSchedule(request)

      override def handleResponse(
          response: admin.v0.GetParticipantSchedule.Response
      ): Either[String, Option[ParticipantPruningSchedule]] =
        response.schedule.fold(
          Right(None): Either[String, Option[ParticipantPruningSchedule]]
        )(ParticipantPruningSchedule.fromProtoV0(_).bimap(_.message, Some(_)))
    }
  }

  object Replication {

    final case class SetPassiveCommand()
        extends GrpcAdminCommand[SetPassive.Request, SetPassive.Response, Unit] {
      override type Svc = EnterpriseParticipantReplicationServiceStub

      override def createService(
          channel: ManagedChannel
      ): EnterpriseParticipantReplicationServiceStub =
        EnterpriseParticipantReplicationServiceGrpc.stub(channel)

      override def createRequest(): Either[String, SetPassive.Request] =
        Right(SetPassive.Request())

      override def submitRequest(
          service: EnterpriseParticipantReplicationServiceStub,
          request: SetPassive.Request,
      ): Future[SetPassive.Response] =
        service.setPassive(request)

      override def handleResponse(response: SetPassive.Response): Either[String, Unit] =
        response match {
          case SetPassive.Response() => Right(())
        }
    }
  }

  object TrafficControl {
    final case class GetTrafficControlState(domainId: DomainId)
        extends GrpcAdminCommand[
          TrafficControlStateRequest,
          TrafficControlStateResponse,
          MemberTrafficStatus,
        ] {
      override type Svc = TrafficControlServiceGrpc.TrafficControlServiceStub

      override def createService(
          channel: ManagedChannel
      ): TrafficControlServiceGrpc.TrafficControlServiceStub =
        TrafficControlServiceGrpc.stub(channel)

      override def submitRequest(
          service: TrafficControlServiceGrpc.TrafficControlServiceStub,
          request: TrafficControlStateRequest,
      ): Future[TrafficControlStateResponse] =
        service.trafficControlState(request)

      override def createRequest(): Either[String, TrafficControlStateRequest] = Right(
        TrafficControlStateRequest(domainId.toProtoPrimitive)
      )

      override def handleResponse(
          response: TrafficControlStateResponse
      ): Either[String, MemberTrafficStatus] = {
        response.trafficState
          .map { trafficStatus =>
            MemberTrafficStatus
              .fromProtoV0(trafficStatus)
              .leftMap(_.message)
          }
          .getOrElse(Left("No traffic state available"))
      }
    }
  }

}
