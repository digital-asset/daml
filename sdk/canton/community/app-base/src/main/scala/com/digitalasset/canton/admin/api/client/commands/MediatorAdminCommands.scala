// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.implicits.toBifunctorOps
import com.digitalasset.canton.admin.api.client.commands.StatusAdminCommands.NodeStatusCommand
import com.digitalasset.canton.admin.api.client.data.{MediatorStatus, NodeStatus}
import com.digitalasset.canton.admin.domain.v30.MediatorStatusServiceGrpc.MediatorStatusServiceStub
import com.digitalasset.canton.admin.domain.v30.{
  MediatorStatusRequest,
  MediatorStatusResponse,
  MediatorStatusServiceGrpc,
}
import io.grpc.ManagedChannel

import scala.concurrent.Future

object MediatorAdminCommands {

  object Health {
    final case class MediatorStatusCommand()
        extends NodeStatusCommand[
          MediatorStatus,
          MediatorStatusRequest,
          MediatorStatusResponse,
        ] {

      override type Svc = MediatorStatusServiceStub

      override def createService(channel: ManagedChannel): MediatorStatusServiceStub =
        MediatorStatusServiceGrpc.stub(channel)

      override def getStatus(
          service: MediatorStatusServiceStub,
          request: MediatorStatusRequest,
      ): Future[MediatorStatusResponse] = service.mediatorStatus(request)

      override def submitRequest(
          service: MediatorStatusServiceStub,
          request: MediatorStatusRequest,
      ): Future[MediatorStatusResponse] =
        submitReq(service, request)

      override def createRequest(): Either[String, MediatorStatusRequest] = Right(
        MediatorStatusRequest()
      )

      override def handleResponse(
          response: MediatorStatusResponse
      ): Either[String, NodeStatus[MediatorStatus]] =
        MediatorStatus.fromProtoV30(response).leftMap(_.message)
    }
  }

}
