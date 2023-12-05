// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import cats.syntax.option.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.api.client.data.StaticDomainParameters
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v2.SequencerInitializationServiceGrpc
import com.digitalasset.canton.domain.admin.{v0, v2}
import com.digitalasset.canton.domain.sequencing.admin.grpc.{
  InitializeSequencerRequest,
  InitializeSequencerRequestX,
  InitializeSequencerResponse,
  InitializeSequencerResponseX,
}
import com.digitalasset.canton.domain.sequencing.sequencer.{LedgerIdentity, SequencerSnapshot}
import com.digitalasset.canton.pruning.admin.v0.LocatePruningTimestamp
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import com.digitalasset.canton.topology.{DomainId, Member}
import com.google.protobuf.empty.Empty
import io.grpc.ManagedChannel

import scala.concurrent.Future

object EnterpriseSequencerAdminCommands {
  abstract class BaseSequencerInitializationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc = v0.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v0.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub =
      v0.SequencerInitializationServiceGrpc.stub(channel)
  }

  abstract class BaseSequencerAdministrationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc =
      v0.EnterpriseSequencerAdministrationServiceGrpc.EnterpriseSequencerAdministrationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v0.EnterpriseSequencerAdministrationServiceGrpc.EnterpriseSequencerAdministrationServiceStub =
      v0.EnterpriseSequencerAdministrationServiceGrpc.stub(channel)
  }

  abstract class BaseSequencerTopologyBootstrapCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc = v0.TopologyBootstrapServiceGrpc.TopologyBootstrapServiceStub
    override def createService(
        channel: ManagedChannel
    ): v0.TopologyBootstrapServiceGrpc.TopologyBootstrapServiceStub =
      v0.TopologyBootstrapServiceGrpc.stub(channel)
  }

  sealed trait Initialize[ProtoRequest]
      extends BaseSequencerInitializationCommand[
        ProtoRequest,
        v0.InitResponse,
        InitializeSequencerResponse,
      ] {
    protected def domainId: DomainId
    protected def topologySnapshot: StoredTopologyTransactions[TopologyChangeOp.Positive]

    protected def domainParameters: StaticDomainParameters

    protected def snapshotO: Option[SequencerSnapshot]

    protected def serializer: InitializeSequencerRequest => ProtoRequest

    override def createRequest(): Either[String, ProtoRequest] = {
      val request = InitializeSequencerRequest(
        domainId,
        topologySnapshot,
        domainParameters.toInternal,
        snapshotO,
      )
      Right(serializer(request))
    }

    override def handleResponse(
        response: v0.InitResponse
    ): Either[String, InitializeSequencerResponse] =
      InitializeSequencerResponse
        .fromProtoV0(response)
        .leftMap(err => s"Failed to deserialize response: $err")

    override def timeoutType: TimeoutType = DefaultUnboundedTimeout
  }

  object Initialize {
    final case class V2(
        domainId: DomainId,
        topologySnapshot: StoredTopologyTransactions[TopologyChangeOp.Positive],
        domainParameters: StaticDomainParameters,
        snapshotO: Option[SequencerSnapshot],
    ) extends Initialize[v2.InitRequest] {

      override protected def serializer: InitializeSequencerRequest => v2.InitRequest = _.toProtoV2

      override def submitRequest(
          service: v0.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub,
          request: v2.InitRequest,
      ): Future[v0.InitResponse] =
        service.initV2(request)
    }

    def apply(
        domainId: DomainId,
        topologySnapshot: StoredTopologyTransactions[TopologyChangeOp.Positive],
        domainParameters: StaticDomainParameters,
        snapshotO: Option[SequencerSnapshot] = None,
    ): Initialize[_] =
      V2(domainId, topologySnapshot, domainParameters, snapshotO)
  }

  final case class InitializeX(
      topologySnapshot: GenericStoredTopologyTransactionsX,
      domainParameters: com.digitalasset.canton.protocol.StaticDomainParameters,
      sequencerSnapshot: Option[SequencerSnapshot],
  ) extends GrpcAdminCommand[
        v2.InitializeSequencerRequest,
        v2.InitializeSequencerResponse,
        InitializeSequencerResponseX,
      ] {
    override type Svc = v2.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub

    override def createService(
        channel: ManagedChannel
    ): SequencerInitializationServiceGrpc.SequencerInitializationServiceStub =
      v2.SequencerInitializationServiceGrpc.stub(channel)

    override def submitRequest(
        service: SequencerInitializationServiceGrpc.SequencerInitializationServiceStub,
        request: v2.InitializeSequencerRequest,
    ): Future[v2.InitializeSequencerResponse] =
      service.initialize(request)

    override def createRequest(): Either[String, v2.InitializeSequencerRequest] =
      Right(
        InitializeSequencerRequestX(
          topologySnapshot,
          domainParameters,
          sequencerSnapshot,
        ).toProtoV2
      )

    override def handleResponse(
        response: v2.InitializeSequencerResponse
    ): Either[String, InitializeSequencerResponseX] =
      InitializeSequencerResponseX.fromProtoV2(response).leftMap(_.toString)
  }

  final case class Snapshot(timestamp: CantonTimestamp)
      extends BaseSequencerAdministrationCommand[
        v0.Snapshot.Request,
        v0.Snapshot.Response,
        SequencerSnapshot,
      ] {
    override def createRequest(): Either[String, v0.Snapshot.Request] = {
      Right(v0.Snapshot.Request(Some(timestamp.toProtoPrimitive)))
    }

    override def submitRequest(
        service: v0.EnterpriseSequencerAdministrationServiceGrpc.EnterpriseSequencerAdministrationServiceStub,
        request: v0.Snapshot.Request,
    ): Future[v0.Snapshot.Response] = service.snapshot(request)

    override def handleResponse(response: v0.Snapshot.Response): Either[String, SequencerSnapshot] =
      response.value match {
        case v0.Snapshot.Response.Value.Failure(v0.Snapshot.Failure(reason)) => Left(reason)
        case v0.Snapshot.Response.Value.Success(v0.Snapshot.Success(Some(result))) =>
          SequencerSnapshot.fromProtoV1(result).leftMap(_.toString)
        case v0.Snapshot.Response.Value.VersionedSuccess(v0.Snapshot.VersionedSuccess(snapshot)) =>
          SequencerSnapshot.fromByteString(snapshot).leftMap(_.toString)
        case _ => Left("response is empty")
      }

    //  command will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout

  }

  final case class Prune(timestamp: CantonTimestamp)
      extends BaseSequencerAdministrationCommand[v0.Pruning.Request, v0.Pruning.Response, String] {
    override def createRequest(): Either[String, v0.Pruning.Request] =
      Right(v0.Pruning.Request(timestamp.toProtoPrimitive.some))

    override def submitRequest(
        service: v0.EnterpriseSequencerAdministrationServiceGrpc.EnterpriseSequencerAdministrationServiceStub,
        request: v0.Pruning.Request,
    ): Future[v0.Pruning.Response] =
      service.prune(request)
    override def handleResponse(response: v0.Pruning.Response): Either[String, String] =
      Either.cond(
        response.details.nonEmpty,
        response.details,
        "Pruning response did not contain details",
      )

    //  command will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout

  }

  final case class LocatePruningTimestampCommand(index: PositiveInt)
      extends BaseSequencerAdministrationCommand[
        LocatePruningTimestamp.Request,
        LocatePruningTimestamp.Response,
        Option[CantonTimestamp],
      ] {
    override def createRequest(): Either[String, LocatePruningTimestamp.Request] = Right(
      LocatePruningTimestamp.Request(index.value)
    )

    override def submitRequest(
        service: v0.EnterpriseSequencerAdministrationServiceGrpc.EnterpriseSequencerAdministrationServiceStub,
        request: LocatePruningTimestamp.Request,
    ): Future[LocatePruningTimestamp.Response] =
      service.locatePruningTimestamp(request)

    override def handleResponse(
        response: LocatePruningTimestamp.Response
    ): Either[String, Option[CantonTimestamp]] =
      response.timestamp.fold(Right(None): Either[String, Option[CantonTimestamp]])(
        CantonTimestamp.fromProtoPrimitive(_).bimap(_.message, Some(_))
      )
  }

  final case class DisableMember(member: Member)
      extends BaseSequencerAdministrationCommand[v0.DisableMemberRequest, Empty, Unit] {
    override def createRequest(): Either[String, v0.DisableMemberRequest] =
      Right(v0.DisableMemberRequest(member.toProtoPrimitive))
    override def submitRequest(
        service: v0.EnterpriseSequencerAdministrationServiceGrpc.EnterpriseSequencerAdministrationServiceStub,
        request: v0.DisableMemberRequest,
    ): Future[Empty] = service.disableMember(request)
    override def handleResponse(response: Empty): Either[String, Unit] = Right(())
  }

  final case class AuthorizeLedgerIdentity(ledgerIdentity: LedgerIdentity)
      extends BaseSequencerAdministrationCommand[
        v0.LedgerIdentity.AuthorizeRequest,
        v0.LedgerIdentity.AuthorizeResponse,
        Unit,
      ] {
    override def createRequest(): Either[String, v0.LedgerIdentity.AuthorizeRequest] =
      Right(v0.LedgerIdentity.AuthorizeRequest(Some(ledgerIdentity.toProtoV0)))
    override def submitRequest(
        service: v0.EnterpriseSequencerAdministrationServiceGrpc.EnterpriseSequencerAdministrationServiceStub,
        request: v0.LedgerIdentity.AuthorizeRequest,
    ): Future[v0.LedgerIdentity.AuthorizeResponse] = service.authorizeLedgerIdentity(request)
    override def handleResponse(
        response: v0.LedgerIdentity.AuthorizeResponse
    ): Either[String, Unit] = response.value match {
      case v0.LedgerIdentity.AuthorizeResponse.Value.Failure(v0.LedgerIdentity.Failure(reason)) =>
        Left(reason)
      case v0.LedgerIdentity.AuthorizeResponse.Value.Success(v0.LedgerIdentity.Success()) =>
        Right(())
      case other => Left(s"Empty response: $other")
    }
  }

  final case class BootstrapTopology(
      topologySnapshot: StoredTopologyTransactions[TopologyChangeOp.Positive]
  ) extends BaseSequencerTopologyBootstrapCommand[v0.TopologyBootstrapRequest, Empty, Unit] {
    override def createRequest(): Either[String, v0.TopologyBootstrapRequest] =
      Right(v0.TopologyBootstrapRequest(Some(topologySnapshot.toProtoV0)))

    override def submitRequest(
        service: v0.TopologyBootstrapServiceGrpc.TopologyBootstrapServiceStub,
        request: v0.TopologyBootstrapRequest,
    ): Future[Empty] =
      service.bootstrap(request)

    override def handleResponse(response: Empty): Either[String, Unit] = Right(())

    //  command will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout

  }
}
