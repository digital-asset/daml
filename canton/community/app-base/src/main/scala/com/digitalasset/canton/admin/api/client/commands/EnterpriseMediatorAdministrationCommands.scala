// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import cats.syntax.option.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Fingerprint, PublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.domain.admin.v0.EnterpriseMediatorAdministrationServiceGrpc
import com.digitalasset.canton.domain.mediator.admin.gprc.{
  InitializeMediatorRequest,
  InitializeMediatorResponse,
}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.pruning.admin.v0.LocatePruningTimestamp
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import com.digitalasset.canton.topology.{DomainId, MediatorId}
import com.google.protobuf.empty.Empty
import io.grpc.ManagedChannel

import scala.concurrent.Future

object EnterpriseMediatorAdministrationCommands {
  abstract class BaseMediatorInitializationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc = v0.MediatorInitializationServiceGrpc.MediatorInitializationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v0.MediatorInitializationServiceGrpc.MediatorInitializationServiceStub =
      v0.MediatorInitializationServiceGrpc.stub(channel)
  }
  abstract class BaseMediatorAdministrationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc =
      v0.EnterpriseMediatorAdministrationServiceGrpc.EnterpriseMediatorAdministrationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v0.EnterpriseMediatorAdministrationServiceGrpc.EnterpriseMediatorAdministrationServiceStub =
      v0.EnterpriseMediatorAdministrationServiceGrpc.stub(channel)
  }

  final case class Initialize(
      domainId: DomainId,
      mediatorId: MediatorId,
      topologyState: Option[StoredTopologyTransactions[TopologyChangeOp.Positive]],
      domainParameters: StaticDomainParameters,
      sequencerConnections: SequencerConnections,
      signingKeyFingerprint: Option[Fingerprint],
  ) extends BaseMediatorInitializationCommand[
        v0.InitializeMediatorRequest,
        v0.InitializeMediatorResponse,
        PublicKey,
      ] {
    override def createRequest(): Either[String, v0.InitializeMediatorRequest] =
      Right(
        InitializeMediatorRequest(
          domainId,
          mediatorId,
          topologyState,
          domainParameters,
          sequencerConnections,
          signingKeyFingerprint,
        ).toProtoV0
      )

    override def submitRequest(
        service: v0.MediatorInitializationServiceGrpc.MediatorInitializationServiceStub,
        request: v0.InitializeMediatorRequest,
    ): Future[v0.InitializeMediatorResponse] =
      service.initialize(request)
    override def handleResponse(
        response: v0.InitializeMediatorResponse
    ): Either[String, PublicKey] =
      InitializeMediatorResponse
        .fromProtoV0(response)
        .leftMap(err => s"Failed to deserialize response: $err")
        .flatMap(_.toEither)
  }

  final case class Prune(timestamp: CantonTimestamp)
      extends GrpcAdminCommand[v0.MediatorPruningRequest, Empty, Unit] {
    override type Svc =
      v0.EnterpriseMediatorAdministrationServiceGrpc.EnterpriseMediatorAdministrationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v0.EnterpriseMediatorAdministrationServiceGrpc.EnterpriseMediatorAdministrationServiceStub =
      v0.EnterpriseMediatorAdministrationServiceGrpc.stub(channel)
    override def createRequest(): Either[String, v0.MediatorPruningRequest] =
      Right(v0.MediatorPruningRequest(timestamp.toProtoPrimitive.some))
    override def submitRequest(
        service: v0.EnterpriseMediatorAdministrationServiceGrpc.EnterpriseMediatorAdministrationServiceStub,
        request: v0.MediatorPruningRequest,
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
        service: EnterpriseMediatorAdministrationServiceGrpc.EnterpriseMediatorAdministrationServiceStub,
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
