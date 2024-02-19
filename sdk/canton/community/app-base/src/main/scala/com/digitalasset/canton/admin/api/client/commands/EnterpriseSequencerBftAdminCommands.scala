// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.admin.EnterpriseSequencerBftAdminData.{
  PeerNetworkStatus,
  endpointToProto,
}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencer.admin.v30.*
import io.grpc.ManagedChannel

import scala.concurrent.Future

object EnterpriseSequencerBftAdminCommands {

  abstract class BaseSequencerBftAdministrationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc =
      v30.SequencerBftAdministrationServiceGrpc.SequencerBftAdministrationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v30.SequencerBftAdministrationServiceGrpc.SequencerBftAdministrationServiceStub =
      v30.SequencerBftAdministrationServiceGrpc.stub(channel)
  }

  final case class AddPeerEndpoint(endpoint: Endpoint)
      extends BaseSequencerBftAdministrationCommand[
        AddPeerEndpointRequest,
        AddPeerEndpointResponse,
        Unit,
      ] {

    override def createRequest(): Either[String, AddPeerEndpointRequest] = Right(
      AddPeerEndpointRequest.of(Some(endpointToProto(endpoint)))
    )

    override def submitRequest(
        service: v30.SequencerBftAdministrationServiceGrpc.SequencerBftAdministrationServiceStub,
        request: AddPeerEndpointRequest,
    ): Future[AddPeerEndpointResponse] =
      service.addPeerEndpoint(request)

    override def handleResponse(
        response: AddPeerEndpointResponse
    ): Either[String, Unit] =
      Right(())
  }

  final case class RemovePeerEndpoint(endpoint: Endpoint)
      extends BaseSequencerBftAdministrationCommand[
        RemovePeerEndpointRequest,
        RemovePeerEndpointResponse,
        Unit,
      ] {

    override def createRequest(): Either[String, RemovePeerEndpointRequest] = Right(
      RemovePeerEndpointRequest.of(Some(endpointToProto(endpoint)))
    )

    override def submitRequest(
        service: v30.SequencerBftAdministrationServiceGrpc.SequencerBftAdministrationServiceStub,
        request: RemovePeerEndpointRequest,
    ): Future[RemovePeerEndpointResponse] =
      service.removePeerEndpoint(request)

    override def handleResponse(
        response: RemovePeerEndpointResponse
    ): Either[String, Unit] =
      Right(())
  }

  final case class GetPeerNetworkStatus(endpoints: Option[Iterable[Endpoint]])
      extends BaseSequencerBftAdministrationCommand[
        GetPeerNetworkStatusRequest,
        GetPeerNetworkStatusResponse,
        PeerNetworkStatus,
      ] {

    override def createRequest(): Either[String, GetPeerNetworkStatusRequest] = Right(
      GetPeerNetworkStatusRequest.of(endpoints.getOrElse(Iterable.empty).map(endpointToProto).toSeq)
    )

    override def submitRequest(
        service: v30.SequencerBftAdministrationServiceGrpc.SequencerBftAdministrationServiceStub,
        request: GetPeerNetworkStatusRequest,
    ): Future[GetPeerNetworkStatusResponse] =
      service.getPeerNetworkStatus(request)

    override def handleResponse(
        response: GetPeerNetworkStatusResponse
    ): Either[String, PeerNetworkStatus] =
      PeerNetworkStatus.fromProto(response)
  }
}
