// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.implicits.toTraverseOps
import cats.syntax.either.*
import com.digitalasset.canton.mediator.admin.v30
import com.digitalasset.canton.sequencing.{SequencerConnectionValidation, SequencerConnections}
import io.grpc.ManagedChannel

import scala.concurrent.Future

object SequencerConnectionAdminCommands {

  abstract class BaseSequencerConnectionAdminCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc =
      v30.SequencerConnectionServiceGrpc.SequencerConnectionServiceStub

    override def createService(
        channel: ManagedChannel
    ): v30.SequencerConnectionServiceGrpc.SequencerConnectionServiceStub =
      v30.SequencerConnectionServiceGrpc.stub(channel)
  }

  final case class GetConnection()
      extends BaseSequencerConnectionAdminCommand[
        v30.GetConnectionRequest,
        v30.GetConnectionResponse,
        Option[SequencerConnections],
      ] {
    override protected def submitRequest(
        service: v30.SequencerConnectionServiceGrpc.SequencerConnectionServiceStub,
        request: v30.GetConnectionRequest,
    ): Future[v30.GetConnectionResponse] = service.getConnection(request)

    override protected def createRequest(): Either[String, v30.GetConnectionRequest] = Right(
      v30.GetConnectionRequest()
    )

    override protected def handleResponse(
        response: v30.GetConnectionResponse
    ): Either[String, Option[SequencerConnections]] = {
      val v30.GetConnectionResponse(sequencerConnectionsPO) = response
      sequencerConnectionsPO.traverse(SequencerConnections.fromProtoV30).leftMap(_.message)
    }
  }

  final case class SetConnection(
      connections: SequencerConnections,
      validation: SequencerConnectionValidation,
  ) extends BaseSequencerConnectionAdminCommand[
        v30.SetConnectionRequest,
        v30.SetConnectionResponse,
        Unit,
      ] {
    override protected def submitRequest(
        service: v30.SequencerConnectionServiceGrpc.SequencerConnectionServiceStub,
        request: v30.SetConnectionRequest,
    ): Future[v30.SetConnectionResponse] = service.setConnection(request)

    override protected def createRequest(): Either[String, v30.SetConnectionRequest] = Right(
      v30.SetConnectionRequest(
        Some(connections.toProtoV30),
        sequencerConnectionValidation = validation.toProtoV30,
      )
    )

    override protected def handleResponse(
        response: v30.SetConnectionResponse
    ): Either[String, Unit] =
      Either.unit
  }

  final case class Logout()
      extends BaseSequencerConnectionAdminCommand[v30.LogoutRequest, v30.LogoutResponse, Unit] {

    override protected def createRequest(): Either[String, v30.LogoutRequest] =
      Right(v30.LogoutRequest())

    override protected def submitRequest(
        service: v30.SequencerConnectionServiceGrpc.SequencerConnectionServiceStub,
        request: v30.LogoutRequest,
    ): Future[v30.LogoutResponse] =
      service.logout(request)

    override protected def handleResponse(response: v30.LogoutResponse): Either[String, Unit] =
      Either.unit
  }
}
