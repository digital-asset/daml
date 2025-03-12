// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId

/** A composite data class containing both the current ordering topology and crypto provider as well
  * as the previous (epoch's) ordering topology and crypto provider of a node for validating
  * canonical commit sets at epoch boundaries. Also, provides an easy way to convert to
  * [[Membership]].
  */
final case class OrderingTopologyInfo[E <: Env[E]](
    thisNode: BftNodeId,
    currentTopology: OrderingTopology,
    currentCryptoProvider: CryptoProvider[E],
    currentLeaders: Seq[BftNodeId],
    previousTopology: OrderingTopology,
    previousCryptoProvider: CryptoProvider[E],
    previousLeaders: Seq[BftNodeId],
) {
  lazy val currentMembership: Membership = Membership(thisNode, currentTopology, currentLeaders)
  lazy val previousMembership: Membership = Membership(thisNode, previousTopology, previousLeaders)

  def updateMembership(
      newMembership: Membership,
      newCryptoProvider: CryptoProvider[E],
  ): OrderingTopologyInfo[E] = OrderingTopologyInfo(
    thisNode,
    newMembership.orderingTopology,
    newCryptoProvider,
    newMembership.leaders,
    previousTopology = currentTopology,
    previousCryptoProvider = currentCryptoProvider,
    previousLeaders = currentLeaders,
  )
}
