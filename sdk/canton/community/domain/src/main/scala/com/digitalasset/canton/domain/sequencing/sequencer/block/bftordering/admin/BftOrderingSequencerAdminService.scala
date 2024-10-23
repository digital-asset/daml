// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.admin

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.admin.EnterpriseSequencerBftAdminData.{
  PeerNetworkStatus,
  endpointFromProto,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.{
  Consensus,
  P2PNetworkOut,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencer.admin.v30.*
import com.digitalasset.canton.sequencer.admin.v30.SequencerBftAdministrationServiceGrpc.SequencerBftAdministrationService
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future, Promise}

final class BftOrderingSequencerAdminService(
    p2pNetworkOutAdminRef: ModuleRef[P2PNetworkOut.Admin],
    issConsensusAdminRef: ModuleRef[Consensus.Admin],
    override val loggerFactory: NamedLoggerFactory,
    createBoolPromise: () => Promise[Boolean] = () => Promise(),
    createNetworkStatusPromise: () => Promise[PeerNetworkStatus] = () => Promise(),
    createOrderingTopologyPromise: () => Promise[(EpochNumber, Set[SequencerId])] = () => Promise(),
)(implicit ec: ExecutionContext)
    extends SequencerBftAdministrationService
    with NamedLogging {

  private implicit val traceContext: TraceContext = TraceContext.empty

  override def addPeerEndpoint(request: AddPeerEndpointRequest): Future[AddPeerEndpointResponse] = {
    logger.info(
      s"BFT sequencer admin service: adding peer endpoint ${request.endpoint} to the network."
    )
    adminPeerEndpoint(
      request.endpoint,
      P2PNetworkOut.Admin.AddEndpoint.apply,
      AddPeerEndpointResponse.of,
    )
  }

  override def removePeerEndpoint(
      request: RemovePeerEndpointRequest
  ): Future[RemovePeerEndpointResponse] = {
    logger.info(
      s"BFT sequencer admin service: removing peer endpoint ${request.endpoint} to the network."
    )
    adminPeerEndpoint(
      request.endpoint,
      P2PNetworkOut.Admin.RemoveEndpoint.apply,
      RemovePeerEndpointResponse.of,
    )
  }

  override def getPeerNetworkStatus(
      request: GetPeerNetworkStatusRequest
  ): Future[GetPeerNetworkStatusResponse] = {
    logger.info(
      "BFT sequencer admin service: getting peer network status for endpoints " +
        s"${if (request.endpoints.isEmpty) "<all known>" else request.endpoints.toString()} " +
        "to the network."
    )
    val resultPromise = createNetworkStatusPromise()
    val endpoints = request.endpoints.map(tryEndpointFromProto)
    p2pNetworkOutAdminRef.asyncSend(
      P2PNetworkOut.Admin.GetStatus(
        if (endpoints.isEmpty) None else Some(endpoints),
        status => resultPromise.success(status),
      )
    )
    resultPromise.future.map(_.toProto)
  }

  private def adminPeerEndpoint[R](
      endpoint: Option[PeerEndpoint],
      createMessage: (Endpoint, Boolean => Unit) => P2PNetworkOut.Admin,
      createResponse: Boolean => R,
  ): Future[R] = {
    val resultPromise = createBoolPromise()
    endpoint.fold(
      resultPromise.success(false).discard
    )(endpoint =>
      p2pNetworkOutAdminRef.asyncSend(
        createMessage(
          tryEndpointFromProto(endpoint),
          resultPromise.success,
        )
      )
    )
    resultPromise.future.map(createResponse)
  }

  private def tryEndpointFromProto(endpoint: PeerEndpoint): Endpoint =
    endpointFromProto(endpoint).fold(
      error => {
        logger.error(s"Failed to convert endpoint $endpoint from proto: $error")
        throw new RuntimeException(error) // Will produce a gRPC internal error
      },
      identity,
    )

  override def getOrderingTopology(
      request: GetOrderingTopologyRequest
  ): Future[GetOrderingTopologyResponse] = {
    val resultPromise = createOrderingTopologyPromise()
    issConsensusAdminRef.asyncSend(
      Consensus.Admin.GetOrderingTopology { (currentEpoch, sequencerIds) =>
        resultPromise.success(currentEpoch -> sequencerIds).discard
      }
    )
    resultPromise.future.map { case (currentEpoch, sequencerIds) =>
      GetOrderingTopologyResponse.of(
        currentEpoch,
        sequencerIds.map(_.toProtoPrimitive).toSeq.sorted,
      )
    }
  }
}
