// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.P2PConnectionState
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.P2PNetworkRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.mutex
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import io.grpc.stub.StreamObserver

import scala.collection.mutable

class P2PGrpcConnectionState(override val loggerFactory: NamedLoggerFactory)
    extends P2PConnectionState
    with NamedLogging {

  private val clientPeerSenders =
    mutable.Map[P2PEndpoint.Id, StreamObserver[BftOrderingMessage]]()

  private val serverPeerSenders = mutable.Set[StreamObserver[BftOrderingMessage]]()

  private val p2pEndpointIdToNetworkRef =
    mutable.Map.empty[
      P2PEndpoint.Id,
      (P2PEndpoint, P2PNetworkRef[BftOrderingMessage]),
    ]
  private val p2pEndpointIdToNodeId = mutable.Map.empty[P2PEndpoint.Id, BftNodeId]
  private val nodeIdToP2PEndpointId = mutable.Map.empty[BftNodeId, P2PEndpoint.Id]

  def getClientPeerSender(
      p2pEndpointId: P2PEndpoint.Id
  ): Option[StreamObserver[BftOrderingMessage]] =
    mutex(this)(clientPeerSenders.get(p2pEndpointId))

  def knownP2PEndpointIds: Seq[P2PEndpoint.Id] =
    mutex(this)(clientPeerSenders.keys.toSeq)

  def setClientPeerSender(
      p2pEndpointId: P2PEndpoint.Id,
      peerSender: StreamObserver[BftOrderingMessage],
  ): Option[StreamObserver[BftOrderingMessage]] =
    mutex(this)(clientPeerSenders.put(p2pEndpointId, peerSender))

  def addServerPeerSender(peerSender: StreamObserver[BftOrderingMessage]): Boolean =
    mutex(this)(serverPeerSenders.add(peerSender))

  def removeClientPeerSender(
      p2pEndpointId: P2PEndpoint.Id
  ): Option[StreamObserver[BftOrderingMessage]] =
    mutex(this)(clientPeerSenders.remove(p2pEndpointId))

  def removeServerPeerSender(peerSender: StreamObserver[BftOrderingMessage]): Boolean =
    mutex(this)(serverPeerSenders.remove(peerSender))

  def cleanupServerPeerSenders(
      cleanupServerPeerSender: StreamObserver[BftOrderingMessage] => Unit
  ): Unit =
    mutex(this) {
      serverPeerSenders.foreach(cleanupServerPeerSender)
      serverPeerSenders.clear()
    }

  override def isDefined(p2pEndpointId: P2PEndpoint.Id): Boolean =
    p2pEndpointIdToNetworkRef.contains(p2pEndpointId)

  override def actOnBftNodeId(
      bftNodeId: BftNodeId,
      ifEmpty: => Unit,
  )(
      action: P2PNetworkRef[BftOrderingMessage] => Unit
  ): Unit =
    nodeIdToP2PEndpointId
      .get(bftNodeId)
      .fold(ifEmpty)(p2pEndpointIdToNetworkRef.get(_).map(_._2).fold(ifEmpty)(action))

  override def addNetworkRef(
      p2pEndpoint: P2PEndpoint,
      ref: P2PNetworkRef[BftOrderingMessage],
  ): Unit =
    p2pEndpointIdToNetworkRef.addOne(p2pEndpoint.id -> (p2pEndpoint, ref))

  override def getBftNodeId(p2pEndpointId: P2PEndpoint.Id): Option[BftNodeId] =
    p2pEndpointIdToNodeId.get(p2pEndpointId)

  override def setBftNodeId(p2pEndpointId: P2PEndpoint.Id, node: BftNodeId): Unit = {
    p2pEndpointIdToNodeId.addOne(p2pEndpointId -> node)
    nodeIdToP2PEndpointId.addOne(node -> p2pEndpointId)
  }

  override def getP2PEndpoints: Seq[P2PEndpoint] =
    p2pEndpointIdToNetworkRef.values.map(_._1).toSeq

  override def authenticatedCount: Int =
    p2pEndpointIdToNodeId.size

  override def delete(p2pEndpointId: P2PEndpoint.Id): Unit = {
    p2pEndpointIdToNodeId.remove(p2pEndpointId).foreach(nodeIdToP2PEndpointId.remove)
    p2pEndpointIdToNetworkRef.remove(p2pEndpointId).foreach(_._2.close())
  }
}
