// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import cats.syntax.option.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.pruning.v30.LocatePruningTimestamp
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Fingerprint, PublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v30.MediatorAdministrationServiceGrpc
import com.digitalasset.canton.domain.admin.{v30, v30old}
import com.digitalasset.canton.domain.mediator.admin.gprc.{
  InitializeMediatorRequest,
  InitializeMediatorRequestX,
  InitializeMediatorResponse,
  InitializeMediatorResponseX,
}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import com.digitalasset.canton.topology.{DomainId, MediatorId}
import com.google.protobuf.empty.Empty
import io.grpc.ManagedChannel

import scala.concurrent.Future

object EnterpriseMediatorAdministrationCommands {
  // TODO(#15161): Remove old mediator commands and administration endpoints
  abstract class BaseMediatorInitializationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc = v30old.MediatorInitializationServiceGrpc.MediatorInitializationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v30old.MediatorInitializationServiceGrpc.MediatorInitializationServiceStub =
      v30old.MediatorInitializationServiceGrpc.stub(channel)
  }
  abstract class BaseMediatorXInitializationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc = v30.MediatorInitializationServiceGrpc.MediatorInitializationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v30.MediatorInitializationServiceGrpc.MediatorInitializationServiceStub =
      v30.MediatorInitializationServiceGrpc.stub(channel)
  }
  abstract class BaseMediatorAdministrationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc =
      v30.MediatorAdministrationServiceGrpc.MediatorAdministrationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v30.MediatorAdministrationServiceGrpc.MediatorAdministrationServiceStub =
      v30.MediatorAdministrationServiceGrpc.stub(channel)
  }

  final case class Initialize(
      domainId: DomainId,
      mediatorId: MediatorId,
      topologyState: Option[StoredTopologyTransactions[TopologyChangeOp.Positive]],
      domainParameters: StaticDomainParameters,
      sequencerConnections: SequencerConnections,
      signingKeyFingerprint: Option[Fingerprint],
  ) extends BaseMediatorInitializationCommand[
        v30old.InitializeMediatorRequest,
        v30old.InitializeMediatorResponse,
        PublicKey,
      ] {
    override def createRequest(): Either[String, v30old.InitializeMediatorRequest] =
      Right(
        InitializeMediatorRequest(
          domainId,
          mediatorId,
          topologyState,
          domainParameters,
          sequencerConnections,
          signingKeyFingerprint,
        ).toProtoV30Old
      )

    override def submitRequest(
        service: v30old.MediatorInitializationServiceGrpc.MediatorInitializationServiceStub,
        request: v30old.InitializeMediatorRequest,
    ): Future[v30old.InitializeMediatorResponse] =
      service.initialize(request)
    override def handleResponse(
        response: v30old.InitializeMediatorResponse
    ): Either[String, PublicKey] =
      InitializeMediatorResponse
        .fromProtoV30old(response)
        .leftMap(err => s"Failed to deserialize response: $err")
        .flatMap(_.toEither)
  }

  final case class InitializeX(
      domainId: DomainId,
      domainParameters: StaticDomainParameters,
      sequencerConnections: SequencerConnections,
  ) extends BaseMediatorXInitializationCommand[
        v30.InitializeMediatorRequest,
        v30.InitializeMediatorResponse,
        Unit,
      ] {
    override def createRequest(): Either[String, v30.InitializeMediatorRequest] =
      Right(
        InitializeMediatorRequestX(
          domainId,
          domainParameters,
          sequencerConnections,
        ).toProtoV30
      )

    override def submitRequest(
        service: v30.MediatorInitializationServiceGrpc.MediatorInitializationServiceStub,
        request: v30.InitializeMediatorRequest,
    ): Future[v30.InitializeMediatorResponse] =
      service.initialize(request)
    override def handleResponse(
        response: v30.InitializeMediatorResponse
    ): Either[String, Unit] =
      InitializeMediatorResponseX
        .fromProtoV30(response)
        .leftMap(err => s"Failed to deserialize response: $err")
        .map(_ => ())

  }

  final case class Prune(timestamp: CantonTimestamp)
      extends GrpcAdminCommand[v30.MediatorPruningRequest, Empty, Unit] {
    override type Svc =
      v30.MediatorAdministrationServiceGrpc.MediatorAdministrationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v30.MediatorAdministrationServiceGrpc.MediatorAdministrationServiceStub =
      v30.MediatorAdministrationServiceGrpc.stub(channel)
    override def createRequest(): Either[String, v30.MediatorPruningRequest] =
      Right(v30.MediatorPruningRequest(timestamp.toProtoPrimitive.some))
    override def submitRequest(
        service: v30.MediatorAdministrationServiceGrpc.MediatorAdministrationServiceStub,
        request: v30.MediatorPruningRequest,
    ): Future[Empty] = service.prune(request)
    override def handleResponse(response: Empty): Either[String, Unit] = Right(())

    // all pruning commands will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout
  }

  final case class LocatePruningTimestampCommand(index: PositiveInt)
      extends BaseMediatorAdministrationCommand[
        LocatePruningTimestamp.Request,
        LocatePruningTimestamp.Response,
        Option[CantonTimestamp],
      ] {
    override def createRequest(): Either[String, LocatePruningTimestamp.Request] = Right(
      LocatePruningTimestamp.Request(index.value)
    )

    override def submitRequest(
        service: MediatorAdministrationServiceGrpc.MediatorAdministrationServiceStub,
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
}
