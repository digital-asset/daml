// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import cats.syntax.option.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.api.client.data.StaticDomainParameters
import com.digitalasset.canton.admin.pruning.v30.LocatePruningTimestamp
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v30.SequencerInitializationServiceGrpc
import com.digitalasset.canton.domain.admin.{v30, v30old}
import com.digitalasset.canton.domain.sequencing.admin.grpc.{
  InitializeSequencerRequest,
  InitializeSequencerRequestX,
  InitializeSequencerResponse,
  InitializeSequencerResponseX,
}
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot
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
    override type Svc = v30old.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v30old.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub =
      v30old.SequencerInitializationServiceGrpc.stub(channel)
  }

  abstract class BaseSequencerAdministrationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc =
      v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub =
      v30.SequencerAdministrationServiceGrpc.stub(channel)
  }

  abstract class BaseSequencerPruningAdministrationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc =
      v30.SequencerPruningAdministrationServiceGrpc.SequencerPruningAdministrationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v30.SequencerPruningAdministrationServiceGrpc.SequencerPruningAdministrationServiceStub =
      v30.SequencerPruningAdministrationServiceGrpc.stub(channel)
  }

  abstract class BaseSequencerTopologyBootstrapCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc = v30old.TopologyBootstrapServiceGrpc.TopologyBootstrapServiceStub
    override def createService(
        channel: ManagedChannel
    ): v30old.TopologyBootstrapServiceGrpc.TopologyBootstrapServiceStub =
      v30old.TopologyBootstrapServiceGrpc.stub(channel)
  }

  sealed trait Initialize[ProtoRequest]
      extends BaseSequencerInitializationCommand[
        ProtoRequest,
        v30old.InitResponse,
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
        response: v30old.InitResponse
    ): Either[String, InitializeSequencerResponse] =
      InitializeSequencerResponse
        .fromProtoV30Old(response)
        .leftMap(err => s"Failed to deserialize response: $err")

    override def timeoutType: TimeoutType = DefaultUnboundedTimeout
  }

  object Initialize {
    private final case class V2(
        domainId: DomainId,
        topologySnapshot: StoredTopologyTransactions[TopologyChangeOp.Positive],
        domainParameters: StaticDomainParameters,
        snapshotO: Option[SequencerSnapshot],
    ) extends Initialize[v30old.InitRequest] {

      override protected def serializer: InitializeSequencerRequest => v30old.InitRequest =
        _.toProtoV30Old

      override def submitRequest(
          service: v30old.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub,
          request: v30old.InitRequest,
      ): Future[v30old.InitResponse] =
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
        v30.InitializeSequencerRequest,
        v30.InitializeSequencerResponse,
        InitializeSequencerResponseX,
      ] {
    override type Svc = v30.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub

    override def createService(
        channel: ManagedChannel
    ): SequencerInitializationServiceGrpc.SequencerInitializationServiceStub =
      v30.SequencerInitializationServiceGrpc.stub(channel)

    override def submitRequest(
        service: SequencerInitializationServiceGrpc.SequencerInitializationServiceStub,
        request: v30.InitializeSequencerRequest,
    ): Future[v30.InitializeSequencerResponse] =
      service.initialize(request)

    override def createRequest(): Either[String, v30.InitializeSequencerRequest] =
      Right(
        InitializeSequencerRequestX(
          topologySnapshot,
          domainParameters,
          sequencerSnapshot,
        ).toProtoV30
      )

    override def handleResponse(
        response: v30.InitializeSequencerResponse
    ): Either[String, InitializeSequencerResponseX] =
      InitializeSequencerResponseX.fromProtoV30(response).leftMap(_.toString)
  }

  final case class Snapshot(timestamp: CantonTimestamp)
      extends BaseSequencerAdministrationCommand[
        v30.Snapshot.Request,
        v30.Snapshot.Response,
        SequencerSnapshot,
      ] {
    override def createRequest(): Either[String, v30.Snapshot.Request] = {
      Right(v30.Snapshot.Request(Some(timestamp.toProtoPrimitive)))
    }

    override def submitRequest(
        service: v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: v30.Snapshot.Request,
    ): Future[v30.Snapshot.Response] = service.snapshot(request)

    override def handleResponse(
        response: v30.Snapshot.Response
    ): Either[String, SequencerSnapshot] =
      response.value match {
        case v30.Snapshot.Response.Value.Failure(v30.Snapshot.Failure(reason)) => Left(reason)
        case v30.Snapshot.Response.Value.Success(v30.Snapshot.Success(Some(result))) =>
          SequencerSnapshot.fromProtoV30(result).leftMap(_.toString)
        case v30.Snapshot.Response.Value
              .VersionedSuccess(v30.Snapshot.VersionedSuccess(snapshot)) =>
          SequencerSnapshot
            .fromByteStringUnsafe(snapshot)
            .leftMap(_.toString)
        case _ => Left("response is empty")
      }

    //  command will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout
  }

  final case class Prune(timestamp: CantonTimestamp)
      extends BaseSequencerPruningAdministrationCommand[
        v30.Pruning.Request,
        v30.Pruning.Response,
        String,
      ] {
    override def createRequest(): Either[String, v30.Pruning.Request] =
      Right(v30.Pruning.Request(timestamp.toProtoPrimitive.some))

    override def submitRequest(
        service: v30.SequencerPruningAdministrationServiceGrpc.SequencerPruningAdministrationServiceStub,
        request: v30.Pruning.Request,
    ): Future[v30.Pruning.Response] =
      service.prune(request)
    override def handleResponse(response: v30.Pruning.Response): Either[String, String] =
      Either.cond(
        response.details.nonEmpty,
        response.details,
        "Pruning response did not contain details",
      )

    //  command will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout

  }

  final case class LocatePruningTimestampCommand(index: PositiveInt)
      extends BaseSequencerPruningAdministrationCommand[
        LocatePruningTimestamp.Request,
        LocatePruningTimestamp.Response,
        Option[CantonTimestamp],
      ] {
    override def createRequest(): Either[String, LocatePruningTimestamp.Request] = Right(
      LocatePruningTimestamp.Request(index.value)
    )

    override def submitRequest(
        service: v30.SequencerPruningAdministrationServiceGrpc.SequencerPruningAdministrationServiceStub,
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
      extends BaseSequencerAdministrationCommand[v30.DisableMemberRequest, Empty, Unit] {
    override def createRequest(): Either[String, v30.DisableMemberRequest] =
      Right(v30.DisableMemberRequest(member.toProtoPrimitive))
    override def submitRequest(
        service: v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: v30.DisableMemberRequest,
    ): Future[Empty] = service.disableMember(request)
    override def handleResponse(response: Empty): Either[String, Unit] = Right(())
  }

  final case class BootstrapTopology(
      topologySnapshot: StoredTopologyTransactions[TopologyChangeOp.Positive]
  ) extends BaseSequencerTopologyBootstrapCommand[v30old.TopologyBootstrapRequest, Empty, Unit] {
    override def createRequest(): Either[String, v30old.TopologyBootstrapRequest] =
      Right(v30old.TopologyBootstrapRequest(Some(topologySnapshot.toProtoV30)))

    override def submitRequest(
        service: v30old.TopologyBootstrapServiceGrpc.TopologyBootstrapServiceStub,
        request: v30old.TopologyBootstrapRequest,
    ): Future[Empty] =
      service.bootstrap(request)

    override def handleResponse(response: Empty): Either[String, Unit] = Right(())

    //  command will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout
  }
}
