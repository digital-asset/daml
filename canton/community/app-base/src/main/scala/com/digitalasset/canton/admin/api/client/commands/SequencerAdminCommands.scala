// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.domain.admin.v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub
import com.digitalasset.canton.domain.admin.v30 as adminproto
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerPruningStatus
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerTrafficStatus
import com.digitalasset.canton.topology.Member
import com.google.protobuf.empty.Empty
import io.grpc.ManagedChannel

import scala.concurrent.Future

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
      SequencerPruningStatus.fromProtoV30(response).leftMap(_.toString)
  }

  final case class GetTrafficControlState(members: Seq[Member])
      extends BaseSequencerAdministrationCommands[
        adminproto.TrafficControlStateRequest,
        adminproto.TrafficControlStateResponse,
        SequencerTrafficStatus,
      ] {
    override def createRequest(): Either[String, adminproto.TrafficControlStateRequest] = Right(
      adminproto.TrafficControlStateRequest(members.map(_.toProtoPrimitive))
    )
    override def submitRequest(
        service: SequencerAdministrationServiceStub,
        request: adminproto.TrafficControlStateRequest,
    ): Future[adminproto.TrafficControlStateResponse] =
      service.trafficControlState(request)
    override def handleResponse(
        response: adminproto.TrafficControlStateResponse
    ): Either[String, SequencerTrafficStatus] =
      response.trafficStates
        .traverse(com.digitalasset.canton.traffic.MemberTrafficStatus.fromProtoV0)
        .leftMap(_.toString)
        .map(SequencerTrafficStatus)
  }
}
