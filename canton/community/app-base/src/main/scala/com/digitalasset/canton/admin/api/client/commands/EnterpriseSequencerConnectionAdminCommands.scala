// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.implicits.toTraverseOps
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.sequencing.SequencerConnections
import com.google.protobuf.empty.Empty
import io.grpc.ManagedChannel

import scala.concurrent.Future

object EnterpriseSequencerConnectionAdminCommands {
  abstract class BaseSequencerConnectionAdminCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc =
      v0.EnterpriseSequencerConnectionServiceGrpc.EnterpriseSequencerConnectionServiceStub

    override def createService(
        channel: ManagedChannel
    ): v0.EnterpriseSequencerConnectionServiceGrpc.EnterpriseSequencerConnectionServiceStub =
      v0.EnterpriseSequencerConnectionServiceGrpc.stub(channel)
  }

  final case class GetConnection()
      extends BaseSequencerConnectionAdminCommand[
        v0.GetConnectionRequest,
        v0.GetConnectionResponse,
        Option[SequencerConnections],
      ] {
    override def submitRequest(
        service: v0.EnterpriseSequencerConnectionServiceGrpc.EnterpriseSequencerConnectionServiceStub,
        request: v0.GetConnectionRequest,
    ): Future[v0.GetConnectionResponse] = service.getConnection(request)

    override def createRequest(): Either[String, v0.GetConnectionRequest] = Right(
      v0.GetConnectionRequest()
    )

    override def handleResponse(
        response: v0.GetConnectionResponse
    ): Either[String, Option[SequencerConnections]] =
      NonEmpty.from(response.sequencerConnections).traverse { connections =>
        SequencerConnections
          .fromProtoV0(
            connections,
            response.sequencerTrustThreshold,
          )
          .leftMap(_.message)
      }
  }

  final case class SetConnection(connections: SequencerConnections)
      extends BaseSequencerConnectionAdminCommand[
        v0.SetConnectionRequest,
        Empty,
        Unit,
      ] {
    override def submitRequest(
        service: v0.EnterpriseSequencerConnectionServiceGrpc.EnterpriseSequencerConnectionServiceStub,
        request: v0.SetConnectionRequest,
    ): Future[Empty] = service.setConnection(request)

    override def createRequest(): Either[String, v0.SetConnectionRequest] = Right(
      v0.SetConnectionRequest(
        connections.connections.map(_.toProtoV0),
        connections.sequencerTrustThreshold.unwrap,
      )
    )

    override def handleResponse(response: Empty): Either[String, Unit] = Right(())
  }
}
