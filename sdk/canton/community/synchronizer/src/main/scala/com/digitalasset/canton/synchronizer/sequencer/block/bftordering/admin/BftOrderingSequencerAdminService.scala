// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencer.admin.v30.*
import com.digitalasset.canton.sequencer.admin.v30.SequencerBftAdministrationServiceGrpc.SequencerBftAdministrationService
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Consensus,
  P2PNetworkOut,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future, Promise}

import SequencerBftAdminData.{PeerNetworkStatus, endpointFromProto, endpointIdFromProto}

final class BftOrderingSequencerAdminService(
    p2pNetworkOutAdminRef: ModuleRef[P2PNetworkOut.Admin],
    issConsensusAdminRef: ModuleRef[Consensus.Admin],
    override val loggerFactory: NamedLoggerFactory,
    createBoolPromise: () => Promise[Boolean] = () => Promise(),
    createNetworkStatusPromise: () => Promise[PeerNetworkStatus] = () => Promise(),
    createOrderingTopologyPromise: () => Promise[(EpochNumber, Set[BftNodeId])] = () => Promise(),
)(implicit executionContext: ExecutionContext, metricsContext: MetricsContext)
    extends SequencerBftAdministrationService
    with NamedLogging {

  private implicit val traceContext: TraceContext = TraceContext.empty

  override def addPeerEndpoint(request: AddPeerEndpointRequest): Future[AddPeerEndpointResponse] = {
    logger.info(
      s"BFT sequencer admin service: adding endpoint ${request.endpoint} to the network."
    )
    val resultPromise = createBoolPromise()
    request.endpoint.fold(
      resultPromise.success(false).discard
    )(endpoint =>
      p2pNetworkOutAdminRef.asyncSend(
        P2PNetworkOut.Admin.AddEndpoint(
          tryEndpointFromProto(endpoint),
          resultPromise.success,
        )
      )
    )
    resultPromise.future.map(AddPeerEndpointResponse(_))
  }

  override def removePeerEndpoint(
      request: RemovePeerEndpointRequest
  ): Future[RemovePeerEndpointResponse] = {
    logger.info(
      s"BFT sequencer admin service: removing endpoint ${request.endpointId} to the network."
    )
    val resultPromise = createBoolPromise()
    request.endpointId.fold(
      resultPromise.success(false).discard
    )(endpointId =>
      p2pNetworkOutAdminRef.asyncSend(
        P2PNetworkOut.Admin.RemoveEndpoint(
          tryEndpointIdFromProto(endpointId),
          resultPromise.success,
        )
      )
    )
    resultPromise.future.map(RemovePeerEndpointResponse(_))
  }

  override def getPeerNetworkStatus(
      request: GetPeerNetworkStatusRequest
  ): Future[GetPeerNetworkStatusResponse] = {
    logger.info(
      "BFT sequencer admin service: getting network status for endpoints " +
        s"${if (request.endpointIds.isEmpty) "<all known>" else request.endpointIds.toString()} " +
        "to the network."
    )
    val resultPromise = createNetworkStatusPromise()
    val endpointIds = request.endpointIds.map(tryEndpointIdFromProto)
    p2pNetworkOutAdminRef.asyncSend(
      P2PNetworkOut.Admin.GetStatus(
        status => resultPromise.success(status),
        if (endpointIds.isEmpty) None else Some(endpointIds),
      )
    )
    resultPromise.future.map(_.toProto)
  }

  private def tryEndpointFromProto(endpoint: PeerEndpoint): P2PEndpoint =
    endpointFromProto(endpoint).fold(
      error => {
        logger.error(s"Failed to convert endpoint $endpoint from proto: $error")
        throw new RuntimeException(error) // Will produce a gRPC internal error
      },
      identity,
    )

  private def tryEndpointIdFromProto(endpointId: PeerEndpointId): P2PEndpoint.Id =
    endpointIdFromProto(endpointId).fold(
      error => {
        logger.error(s"Failed to convert endpoint key $endpointId from proto: $error")
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
    resultPromise.future.map { case (currentEpoch, nodes) =>
      GetOrderingTopologyResponse(
        currentEpoch,
        nodes.toSeq.sorted,
      )
    }
  }
}
