// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.implicits.{toBifunctorOps, toTraverseOps}
import com.digitalasset.canton.admin.api.client.commands.StatusAdminCommands.NodeStatusCommand
import com.digitalasset.canton.admin.api.client.data.{MediatorStatus, NodeStatus}
import com.digitalasset.canton.domain.admin.v0.MediatorStatusServiceGrpc.MediatorStatusServiceStub
import com.digitalasset.canton.domain.admin.v0.{
  MediatorStatusRequest,
  MediatorStatusResponse,
  MediatorStatusServiceGrpc,
}
import io.grpc.{ManagedChannel, Status}

import scala.concurrent.{ExecutionContext, Future}

object MediatorAdminCommands {

  object Health {
    /*
      Response and Result types are an Either of the gRPC Status Code to enable backward compatibility.
      Implicitly, the Left only represents the unavailability of the participant specific status command
      endpoint because it is an older version that has not yet implemented it.
     */
    final case class MediatorStatusCommand()(implicit ec: ExecutionContext)
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
      ): Future[Either[Status.Code.UNIMPLEMENTED.type, MediatorStatusResponse]] =
        submitReq(service, request)

      override def createRequest(): Either[String, MediatorStatusRequest] = Right(
        MediatorStatusRequest()
      )

      override def handleResponse(
          response: Either[Status.Code.UNIMPLEMENTED.type, MediatorStatusResponse]
      ): Either[String, Either[Status.Code.UNIMPLEMENTED.type, NodeStatus[MediatorStatus]]] =
        response.traverse(MediatorStatus.fromProtoV1).leftMap(_.message)
    }
  }

}
