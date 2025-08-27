// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.bidi

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  P2PAddress,
  P2PNetworkRef,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import com.digitalasset.canton.tracing.TraceContext

trait P2PBidiConnectionState {

  def isDefined(p2pEndpointId: P2PEndpoint.Id)(implicit traceContext: TraceContext): Boolean

  def isOutgoing(p2pEndpointId: P2PEndpoint.Id): Boolean

  def authenticatedCount: NonNegativeInt

  def associateP2PEndpointIdToBftNodeId(
      p2pAddress: P2PAddress
  )(implicit traceContext: TraceContext): Unit

  def addNetworkRefIfMissing(
      p2pAddressId: P2PAddress.Id
  )(
      actionIfPresent: () => Unit
  )(
      createNetworkRef: () => P2PNetworkRef[BftOrderingMessage]
  )(implicit traceContext: TraceContext): Unit

  def getBftNodeId(p2pEndpointId: P2PEndpoint.Id): Option[BftNodeId]

  def getNetworkRef(bftNodeId: BftNodeId): Option[P2PNetworkRef[BftOrderingMessage]]

  def connections(implicit
      traceContext: TraceContext
  ): Seq[(Option[P2PEndpoint.Id], Option[BftNodeId])]
}
