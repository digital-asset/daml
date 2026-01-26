// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  P2PAddress,
  P2PNetworkRef,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import com.digitalasset.canton.tracing.TraceContext

trait P2PConnectionState {

  import P2PConnectionState.*

  def isDefined(p2pEndpointId: P2PEndpoint.Id)(implicit traceContext: TraceContext): Boolean

  def isOutgoing(p2pEndpointId: P2PEndpoint.Id): Boolean

  def authenticatedCount: NonNegativeInt

  def associateP2PEndpointIdToBftNodeId(
      p2pAddress: P2PAddress
  )(implicit traceContext: TraceContext): P2PEndpointIdAssociationResult

  /** Called by the P2P network output module to ensure connectivity with a peer. It must call
    * either `createNetworkRef` to create a new network reference or `actionIfPresent` if a network
    * reference already exists for the given P2P address ID.
    */
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

object P2PConnectionState {

  type P2PEndpointIdAssociationResult = Either[Error, Unit]

  sealed trait Error extends Product with Serializable

  object Error {

    final case class CannotAssociateP2PEndpointIdsToSelf(
        p2pEndpointId: P2PEndpoint.Id,
        thisBftNodeId: BftNodeId,
    ) extends Error {
      override def toString: String =
        s"Cannot associate any P2P endpoint ID $p2pEndpointId to self ($thisBftNodeId)"
    }

    final case class P2PEndpointIdAlreadyAssociated(
        p2pEndpointId: P2PEndpoint.Id,
        previousBftNodeId: BftNodeId,
        newBftNodeId: BftNodeId,
    ) extends Error {
      override def toString: String =
        s"Cannot associate P2P endpoint ID $p2pEndpointId to $newBftNodeId " +
          s"as it is already associated with $previousBftNodeId"
    }
  }
}
