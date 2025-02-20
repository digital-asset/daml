// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import com.digitalasset.canton.sequencer.admin.v30.SequencerBftAdministrationServiceGrpc.SequencerBftAdministrationServiceStub
import com.digitalasset.canton.sequencer.admin.v30.{
  AddPeerEndpointRequest,
  AddPeerEndpointResponse,
  GetOrderingTopologyRequest,
  GetOrderingTopologyResponse,
  GetPeerNetworkStatusRequest,
  GetPeerNetworkStatusResponse,
  RemovePeerEndpointRequest,
  RemovePeerEndpointResponse,
  SequencerBftAdministrationServiceGrpc,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.{
  OrderingTopology,
  PeerNetworkStatus,
  endpointIdToProto,
  endpointToProto,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.P2PEndpoint
import io.grpc.ManagedChannel

import scala.concurrent.Future

object SequencerBftAdminCommands {

  abstract class BaseSequencerBftAdministrationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {

    override type Svc =
      SequencerBftAdministrationServiceStub

    override def createService(
        channel: ManagedChannel
    ): SequencerBftAdministrationServiceStub =
      SequencerBftAdministrationServiceGrpc.stub(channel)
  }

  final case class AddPeerEndpoint(endpoint: P2PEndpoint)
      extends BaseSequencerBftAdministrationCommand[
        AddPeerEndpointRequest,
        AddPeerEndpointResponse,
        Unit,
      ] {

    override protected def createRequest(): Either[String, AddPeerEndpointRequest] = Right(
      AddPeerEndpointRequest.of(Some(endpointToProto(endpoint)))
    )

    override protected def submitRequest(
        service: SequencerBftAdministrationServiceStub,
        request: AddPeerEndpointRequest,
    ): Future[AddPeerEndpointResponse] =
      service.addPeerEndpoint(request)

    override protected def handleResponse(
        response: AddPeerEndpointResponse
    ): Either[String, Unit] =
      Either.unit
  }

  final case class RemovePeerEndpoint(endpointId: P2PEndpoint.Id)
      extends BaseSequencerBftAdministrationCommand[
        RemovePeerEndpointRequest,
        RemovePeerEndpointResponse,
        Unit,
      ] {

    override protected def createRequest(): Either[String, RemovePeerEndpointRequest] = Right(
      RemovePeerEndpointRequest(Some(endpointIdToProto(endpointId)))
    )

    override protected def submitRequest(
        service: SequencerBftAdministrationServiceStub,
        request: RemovePeerEndpointRequest,
    ): Future[RemovePeerEndpointResponse] =
      service.removePeerEndpoint(request)

    override protected def handleResponse(
        response: RemovePeerEndpointResponse
    ): Either[String, Unit] =
      Either.unit
  }

  final case class GetPeerNetworkStatus(endpoints: Option[Iterable[P2PEndpoint.Id]])
      extends BaseSequencerBftAdministrationCommand[
        GetPeerNetworkStatusRequest,
        GetPeerNetworkStatusResponse,
        PeerNetworkStatus,
      ] {

    override protected def createRequest(): Either[String, GetPeerNetworkStatusRequest] = Right(
      GetPeerNetworkStatusRequest.of(
        endpoints.getOrElse(Iterable.empty).map(endpointIdToProto).toSeq
      )
    )

    override protected def submitRequest(
        service: SequencerBftAdministrationServiceStub,
        request: GetPeerNetworkStatusRequest,
    ): Future[GetPeerNetworkStatusResponse] =
      service.getPeerNetworkStatus(request)

    override protected def handleResponse(
        response: GetPeerNetworkStatusResponse
    ): Either[String, PeerNetworkStatus] =
      PeerNetworkStatus.fromProto(response)
  }

  final case class GetOrderingTopology()
      extends BaseSequencerBftAdministrationCommand[
        GetOrderingTopologyRequest,
        GetOrderingTopologyResponse,
        OrderingTopology,
      ] {

    override protected def createRequest(): Either[String, GetOrderingTopologyRequest] = Right(
      GetOrderingTopologyRequest.of()
    )

    override protected def submitRequest(
        service: SequencerBftAdministrationServiceStub,
        request: GetOrderingTopologyRequest,
    ): Future[GetOrderingTopologyResponse] =
      service.getOrderingTopology(request)

    override protected def handleResponse(
        response: GetOrderingTopologyResponse
    ): Either[String, OrderingTopology] =
      OrderingTopology.fromProto(response)
  }
}
