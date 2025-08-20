// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.P2PNetworkRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage

trait P2PConnectionState {

  def isDefined(p2pEndpointId: P2PEndpoint.Id): Boolean

  def isOutgoing(p2pEndpointId: P2PEndpoint.Id): Boolean

  def authenticatedCount: Int

  def actOnBftNodeId(
      bftNodeId: BftNodeId,
      ifEmpty: => Unit,
  )(
      action: P2PNetworkRef[BftOrderingMessage] => Unit
  ): Unit

  def addNetworkRef(
      p2pEndpoint: P2PEndpoint,
      ref: P2PNetworkRef[BftOrderingMessage],
  ): Unit

  def getBftNodeId(p2pEndpointId: P2PEndpoint.Id): Option[BftNodeId]

  def setBftNodeId(p2pEndpointId: P2PEndpoint.Id, node: BftNodeId): Unit

  def getP2PEndpoints: Seq[P2PEndpoint]

  def delete(p2pEndpointId: P2PEndpoint.Id): Unit
}
