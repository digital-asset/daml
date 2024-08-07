// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import com.digitalasset.canton.admin.api.client.commands.StatusAdminCommands.NodeStatusCommand
import com.digitalasset.canton.admin.api.client.data.{NodeStatus, SequencerStatus}
import com.digitalasset.canton.domain.admin.v0.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub
import com.digitalasset.canton.domain.admin.v0.SequencerStatusServiceGrpc.SequencerStatusServiceStub
import com.digitalasset.canton.domain.admin.v0.{
  SequencerStatusRequest,
  SequencerStatusResponse,
  SequencerStatusServiceGrpc,
}
import com.digitalasset.canton.domain.admin.v0 as adminproto
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerPruningStatus
import com.google.protobuf.empty.Empty
import io.grpc.{ManagedChannel, Status}

import scala.concurrent.{ExecutionContext, Future}

object SequencerAdminCommands {

  abstract class BaseSequencerAdministrationCommands[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc = SequencerAdministrationServiceStub

    override def createService(channel: ManagedChannel): SequencerAdministrationServiceStub =
      adminproto.SequencerAdministrationServiceGrpc.stub(channel)
  }

  final case object GetPruningStatus
      extends BaseSequencerAdministrationCommands[
        Empty,
        adminproto.SequencerPruningStatus,
        SequencerPruningStatus,
      ] {
    override def createRequest(): Either[String, Empty] = Right(Empty())
    override def submitRequest(
        service: SequencerAdministrationServiceStub,
        request: Empty,
    ): Future[adminproto.SequencerPruningStatus] =
      service.pruningStatus(request)
    override def handleResponse(
        response: adminproto.SequencerPruningStatus
    ): Either[String, SequencerPruningStatus] =
      SequencerPruningStatus.fromProtoV0(response).leftMap(_.toString)
  }

  object Health {
    /*
      Response and Result types are an Either of the gRPC Status Code to enable backward compatibility.
      Implicitly, the Left only represents the unavailability of the participant specific status command
      endpoint because it is an older version that has not yet implemented it.
     */
    final case class SequencerStatusCommand()(implicit ec: ExecutionContext)
        extends NodeStatusCommand[
          SequencerStatus,
          SequencerStatusRequest,
          SequencerStatusResponse,
        ] {

      override type Svc = SequencerStatusServiceStub

      override def createService(channel: ManagedChannel): SequencerStatusServiceStub =
        SequencerStatusServiceGrpc.stub(channel)

      override def getStatus(
          service: SequencerStatusServiceStub,
          request: SequencerStatusRequest,
      ): Future[SequencerStatusResponse] = service.sequencerStatus(request)

      override def submitRequest(
          service: SequencerStatusServiceStub,
          request: SequencerStatusRequest,
      ): Future[Either[Status.Code.UNIMPLEMENTED.type, SequencerStatusResponse]] =
        submitReq(service, request)

      override def createRequest(): Either[String, SequencerStatusRequest] = Right(
        SequencerStatusRequest()
      )

      override def handleResponse(
          response: Either[Status.Code.UNIMPLEMENTED.type, SequencerStatusResponse]
      ): Either[String, Either[Status.Code.UNIMPLEMENTED.type, NodeStatus[SequencerStatus]]] =
        response.traverse(SequencerStatus.fromProtoV1).leftMap(_.message)
    }
  }
}
