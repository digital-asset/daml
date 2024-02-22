// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.implicits.toTraverseOps
import cats.syntax.either.*
import com.digitalasset.canton.domain.admin.v30
import com.digitalasset.canton.sequencing.SequencerConnections
import io.grpc.ManagedChannel

import scala.concurrent.Future

object EnterpriseSequencerConnectionAdminCommands {

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
    override def submitRequest(
        service: v30.SequencerConnectionServiceGrpc.SequencerConnectionServiceStub,
        request: v30.GetConnectionRequest,
    ): Future[v30.GetConnectionResponse] = service.getConnection(request)

    override def createRequest(): Either[String, v30.GetConnectionRequest] = Right(
      v30.GetConnectionRequest()
    )

    override def handleResponse(
        response: v30.GetConnectionResponse
    ): Either[String, Option[SequencerConnections]] = {
      val v30.GetConnectionResponse(sequencerConnectionsPO) = response
      sequencerConnectionsPO.traverse(SequencerConnections.fromProtoV30).leftMap(_.message)
    }
  }

  final case class SetConnection(connections: SequencerConnections)
      extends BaseSequencerConnectionAdminCommand[
        v30.SetConnectionRequest,
        v30.SetConnectionResponse,
        Unit,
      ] {
    override def submitRequest(
        service: v30.SequencerConnectionServiceGrpc.SequencerConnectionServiceStub,
        request: v30.SetConnectionRequest,
    ): Future[v30.SetConnectionResponse] = service.setConnection(request)

    override def createRequest(): Either[String, v30.SetConnectionRequest] = Right(
      v30.SetConnectionRequest(Some(connections.toProtoV30))
    )

    override def handleResponse(response: v30.SetConnectionResponse): Either[String, Unit] =
      Either.unit
  }
}
