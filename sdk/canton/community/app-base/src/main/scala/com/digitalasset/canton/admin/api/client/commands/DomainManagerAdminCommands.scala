// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.implicits.{toBifunctorOps, toTraverseOps}
import com.digitalasset.canton.admin.api.client.commands.StatusAdminCommands.NodeStatusCommand
import com.digitalasset.canton.domain.admin.data.DomainManagerNodeStatus
import com.digitalasset.canton.domain.admin.v0.DomainManagerStatusServiceGrpc.DomainManagerStatusServiceStub
import com.digitalasset.canton.domain.admin.v0.{
  DomainManagerStatusRequest,
  DomainManagerStatusResponse,
  DomainManagerStatusServiceGrpc,
}
import com.digitalasset.canton.health.admin.data.NodeStatus
import io.grpc.{ManagedChannel, Status}

import scala.concurrent.{ExecutionContext, Future}

object DomainManagerAdminCommands {

  object Health {
    /*
      Response and Result types are an Either of the gRPC Status Code to enable backward compatibility.
      Implicitly, the Left only represents the unavailability of the participant specific status command
      endpoint because it is an older version that has not yet implemented it.
     */
    final case class DomainManagerStatusCommand()(implicit ec: ExecutionContext)
        extends NodeStatusCommand[
          DomainManagerNodeStatus,
          DomainManagerStatusRequest,
          DomainManagerStatusResponse,
        ] {

      override type Svc = DomainManagerStatusServiceStub

      override def createService(channel: ManagedChannel): DomainManagerStatusServiceStub =
        DomainManagerStatusServiceGrpc.stub(channel)

      override def getStatus(
          service: DomainManagerStatusServiceStub,
          request: DomainManagerStatusRequest,
      ): Future[DomainManagerStatusResponse] = service.domainManagerStatus(request)

      override def submitRequest(
          service: DomainManagerStatusServiceStub,
          request: DomainManagerStatusRequest,
      ): Future[Either[Status.Code.UNIMPLEMENTED.type, DomainManagerStatusResponse]] =
        submitReq(service, request)

      override def createRequest(): Either[String, DomainManagerStatusRequest] = Right(
        DomainManagerStatusRequest()
      )

      override def handleResponse(
          response: Either[Status.Code.UNIMPLEMENTED.type, DomainManagerStatusResponse]
      ): Either[String, Either[Status.Code.UNIMPLEMENTED.type, NodeStatus[
        DomainManagerNodeStatus
      ]]] =
        response.traverse(DomainManagerNodeStatus.fromProtoV1).leftMap(_.message)
    }
  }

}
