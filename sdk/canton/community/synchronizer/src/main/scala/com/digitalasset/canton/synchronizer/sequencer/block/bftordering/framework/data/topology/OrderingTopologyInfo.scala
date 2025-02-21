// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.topology.SequencerId

/** A composite data class containing both the current ordering topology and crypto provider as well
  * as the previous (epoch's) ordering topology and crypto provider of a peer for validating
  * canonical commit sets at epoch boundaries. Also, provides an easy way to convert to
  * [[Membership]].
  */
final case class OrderingTopologyInfo[E <: Env[E]](
    thisPeer: SequencerId,
    currentTopology: OrderingTopology,
    currentCryptoProvider: CryptoProvider[E],
    previousTopology: OrderingTopology,
    previousCryptoProvider: CryptoProvider[E],
) {
  lazy val currentMembership: Membership = Membership(thisPeer, currentTopology)
  lazy val previousMembership: Membership = Membership(thisPeer, previousTopology)
}
