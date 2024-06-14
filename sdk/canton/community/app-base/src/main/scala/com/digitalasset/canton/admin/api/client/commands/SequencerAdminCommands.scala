// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.apply.*
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerPruningStatus
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.TimestampSelector.TimestampSelector
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.{
  SequencerTrafficStatus,
  TimestampSelector,
}
import com.digitalasset.canton.sequencer.admin
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.Member
import io.grpc.ManagedChannel

import scala.concurrent.Future

object SequencerAdminCommands {

  abstract class BaseSequencerAdministrationCommands[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc =
      admin.v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub

    override def createService(
        channel: ManagedChannel
    ): admin.v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub =
      admin.v30.SequencerAdministrationServiceGrpc.stub(channel)
  }

  final case object GetPruningStatus
      extends BaseSequencerAdministrationCommands[
        admin.v30.PruningStatusRequest,
        admin.v30.PruningStatusResponse,
        SequencerPruningStatus,
      ] {
    override def createRequest(): Either[String, admin.v30.PruningStatusRequest] = Right(
      admin.v30.PruningStatusRequest()
    )
    override def submitRequest(
        service: admin.v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: admin.v30.PruningStatusRequest,
    ): Future[admin.v30.PruningStatusResponse] =
      service.pruningStatus(request)
    override def handleResponse(
        response: admin.v30.PruningStatusResponse
    ): Either[String, SequencerPruningStatus] =
      SequencerPruningStatus.fromProtoV30(response.getPruningStatus).leftMap(_.toString)
  }

  final case class GetTrafficControlState(
      members: Seq[Member],
      timestampSelector: TimestampSelector = TimestampSelector.LatestSafe,
  ) extends BaseSequencerAdministrationCommands[
        admin.v30.TrafficControlStateRequest,
        admin.v30.TrafficControlStateResponse,
        SequencerTrafficStatus,
      ] {
    override def createRequest(): Either[String, admin.v30.TrafficControlStateRequest] = Right(
      admin.v30
        .TrafficControlStateRequest(members.map(_.toProtoPrimitive), timestampSelector.toProtoV30)
    )
    override def submitRequest(
        service: admin.v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: admin.v30.TrafficControlStateRequest,
    ): Future[admin.v30.TrafficControlStateResponse] =
      service.trafficControlState(request)
    override def handleResponse(
        response: admin.v30.TrafficControlStateResponse
    ): Either[String, SequencerTrafficStatus] =
      response.trafficStates.toList
        .traverse { case (member, state) =>
          (
            Member.fromProtoPrimitive(member, "member").leftMap(_.toString),
            TrafficState.fromProtoV30(state).leftMap(_.toString).map(Right(_)),
          ).tupled
        }
        .map(_.toMap)
        .map(SequencerTrafficStatus)
  }

  final case class SetTrafficPurchased(
      member: Member,
      serial: PositiveInt,
      balance: NonNegativeLong,
  ) extends BaseSequencerAdministrationCommands[
        admin.v30.SetTrafficPurchasedRequest,
        admin.v30.SetTrafficPurchasedResponse,
        Option[CantonTimestamp],
      ] {
    override def createRequest(): Either[String, admin.v30.SetTrafficPurchasedRequest] = Right(
      admin.v30.SetTrafficPurchasedRequest(member.toProtoPrimitive, serial.value, balance.value)
    )
    override def submitRequest(
        service: admin.v30.SequencerAdministrationServiceGrpc.SequencerAdministrationServiceStub,
        request: admin.v30.SetTrafficPurchasedRequest,
    ): Future[admin.v30.SetTrafficPurchasedResponse] =
      service.setTrafficPurchased(request)
    override def handleResponse(
        response: admin.v30.SetTrafficPurchasedResponse
    ): Either[String, Option[CantonTimestamp]] = {
      response.maxSequencingTimestamp
        .traverse(CantonTimestamp.fromProtoTimestamp)
        .leftMap(_.message)
    }
  }
}
