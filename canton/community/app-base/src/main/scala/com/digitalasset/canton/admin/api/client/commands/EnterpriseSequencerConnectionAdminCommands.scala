// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.implicits.toTraverseOps
import cats.syntax.either.*
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.sequencing.SequencerConnections
import com.google.protobuf.empty.Empty
import io.grpc.ManagedChannel

import scala.concurrent.Future

object EnterpriseSequencerConnectionAdminCommands {
  abstract class BaseSequencerConnectionAdminCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc =
      v0.SequencerConnectionServiceGrpc.SequencerConnectionServiceStub

    override def createService(
        channel: ManagedChannel
    ): v0.SequencerConnectionServiceGrpc.SequencerConnectionServiceStub =
      v0.SequencerConnectionServiceGrpc.stub(channel)
  }

  final case class GetConnection()
      extends BaseSequencerConnectionAdminCommand[
        v0.GetConnectionRequest,
        v0.GetConnectionResponse,
        Option[SequencerConnections],
      ] {
    override def submitRequest(
        service: v0.SequencerConnectionServiceGrpc.SequencerConnectionServiceStub,
        request: v0.GetConnectionRequest,
    ): Future[v0.GetConnectionResponse] = service.getConnection(request)

    override def createRequest(): Either[String, v0.GetConnectionRequest] = Right(
      v0.GetConnectionRequest()
    )

    override def handleResponse(
        response: v0.GetConnectionResponse
    ): Either[String, Option[SequencerConnections]] = {
      val v0.GetConnectionResponse(sequencerConnectionsPO) = response
      sequencerConnectionsPO.traverse(SequencerConnections.fromProtoV30).leftMap(_.message)
    }
  }

  final case class SetConnection(connections: SequencerConnections)
      extends BaseSequencerConnectionAdminCommand[
        v0.SetConnectionRequest,
        Empty,
        Unit,
      ] {
    override def submitRequest(
        service: v0.SequencerConnectionServiceGrpc.SequencerConnectionServiceStub,
        request: v0.SetConnectionRequest,
    ): Future[Empty] = service.setConnection(request)

    override def createRequest(): Either[String, v0.SetConnectionRequest] = Right(
      v0.SetConnectionRequest(Some(connections.toProtoV30))
    )

    override def handleResponse(response: Empty): Either[String, Unit] = Right(())
  }
}
