// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import cats.syntax.option.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.pruning.v30 as pruningProto
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.mediator.admin.v30
import com.digitalasset.canton.sequencing.{SequencerConnectionValidation, SequencerConnections}
import com.digitalasset.canton.synchronizer.mediator.admin.gprc.{
  InitializeMediatorRequest,
  InitializeMediatorResponse,
}
import com.digitalasset.canton.topology.SynchronizerId
import io.grpc.ManagedChannel

import scala.concurrent.Future

object MediatorAdministrationCommands {
  abstract class BaseMediatorInitializationCommand[Req, Rep, Res]
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
      synchronizerId: SynchronizerId,
      sequencerConnections: SequencerConnections,
      validation: SequencerConnectionValidation,
  ) extends BaseMediatorInitializationCommand[
        v30.InitializeMediatorRequest,
        v30.InitializeMediatorResponse,
        Unit,
      ] {
    override protected def createRequest(): Either[String, v30.InitializeMediatorRequest] =
      Right(
        InitializeMediatorRequest(
          synchronizerId,
          sequencerConnections,
          validation,
        ).toProtoV30
      )

    override protected def submitRequest(
        service: v30.MediatorInitializationServiceGrpc.MediatorInitializationServiceStub,
        request: v30.InitializeMediatorRequest,
    ): Future[v30.InitializeMediatorResponse] =
      service.initializeMediator(request)
    override protected def handleResponse(
        response: v30.InitializeMediatorResponse
    ): Either[String, Unit] =
      InitializeMediatorResponse
        .fromProtoV30(response)
        .leftMap(err => s"Failed to deserialize response: $err")
        .map(_ => ())

  }

  final case class Prune(timestamp: CantonTimestamp)
      extends GrpcAdminCommand[
        v30.MediatorPruning.PruneRequest,
        v30.MediatorPruning.PruneResponse,
        Unit,
      ] {
    override type Svc =
      v30.MediatorAdministrationServiceGrpc.MediatorAdministrationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v30.MediatorAdministrationServiceGrpc.MediatorAdministrationServiceStub =
      v30.MediatorAdministrationServiceGrpc.stub(channel)
    override protected def createRequest(): Either[String, v30.MediatorPruning.PruneRequest] =
      Right(v30.MediatorPruning.PruneRequest(timestamp.toProtoTimestamp.some))
    override protected def submitRequest(
        service: v30.MediatorAdministrationServiceGrpc.MediatorAdministrationServiceStub,
        request: v30.MediatorPruning.PruneRequest,
    ): Future[v30.MediatorPruning.PruneResponse] = service.prune(request)
    override protected def handleResponse(
        response: v30.MediatorPruning.PruneResponse
    ): Either[String, Unit] = Either.unit

    // all pruning commands will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout
  }

  final case class LocatePruningTimestampCommand(index: PositiveInt)
      extends BaseMediatorAdministrationCommand[
        pruningProto.LocatePruningTimestampRequest,
        pruningProto.LocatePruningTimestampResponse,
        Option[CantonTimestamp],
      ] {
    override protected def createRequest()
        : Either[String, pruningProto.LocatePruningTimestampRequest] = Right(
      pruningProto.LocatePruningTimestampRequest(index.value)
    )

    override protected def submitRequest(
        service: v30.MediatorAdministrationServiceGrpc.MediatorAdministrationServiceStub,
        request: pruningProto.LocatePruningTimestampRequest,
    ): Future[pruningProto.LocatePruningTimestampResponse] =
      service.locatePruningTimestamp(request)

    override protected def handleResponse(
        response: pruningProto.LocatePruningTimestampResponse
    ): Either[String, Option[CantonTimestamp]] =
      response.timestamp.fold(Right(None): Either[String, Option[CantonTimestamp]])(
        CantonTimestamp.fromProtoTimestamp(_).bimap(_.message, Some(_))
      )
  }
}
