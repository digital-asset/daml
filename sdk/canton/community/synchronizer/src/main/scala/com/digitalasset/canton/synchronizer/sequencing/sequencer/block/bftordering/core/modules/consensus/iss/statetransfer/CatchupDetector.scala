// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.topology.SequencerId

import scala.collection.mutable

// We don't foresee multiple implementations, this trait exists only to allow mocking in tests
sealed trait CatchupDetector {
  def updateMembership(newMembership: Membership): Unit
  def updateLatestKnownPeerEpoch(
      peer: SequencerId,
      epochNumber: EpochNumber,
  ): Boolean
  def shouldCatchUp(localEpoch: EpochNumber): Boolean
}

final class DefaultCatchupDetector(
    initialMembership: Membership
) extends CatchupDetector {

  import DefaultCatchupDetector.*

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var membership: Membership = initialMembership

  // Transient bookkeeping of the highest epoch number received from each peer to determine when catch up should occur
  private val latestKnownPeerEpochs: mutable.Map[SequencerId, EpochNumber] = mutable.Map.empty

  override def updateMembership(newMembership: Membership): Unit = {
    membership = newMembership

    // Cleanup outdated entries
    latestKnownPeerEpochs.keys.toSeq.foreach { p =>
      if (!membership.otherPeers.contains(p)) {
        latestKnownPeerEpochs.remove(p).discard
      }
    }
  }

  /** Updates the highest known epoch number for the specified peer.
    * Returns `true` only if the epoch number was updated for the specified peer.
    */
  override def updateLatestKnownPeerEpoch(
      peer: SequencerId,
      epochNumber: EpochNumber,
  ): Boolean = {

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var updated = false

    if (membership.otherPeers.contains(peer)) {
      latestKnownPeerEpochs
        .updateWith(peer) {
          case Some(epoch) =>
            if (epochNumber > epoch) {
              updated = true
              Some(epochNumber)
            } else {
              Some(epoch)
            }
          case None =>
            updated = true
            Some(epochNumber)
        }
        .discard
    }

    updated
  }

  override def shouldCatchUp(localEpoch: EpochNumber): Boolean = {
    val behindEnoughPeers =
      latestKnownPeerEpochs.count { case (_, peerEpoch) =>
        peerEpoch >= localEpoch + MinimumEpochDeltaToTriggerCatchUp
      } >= membership.orderingTopology.weakQuorum

    behindEnoughPeers
  }
}

private object DefaultCatchupDetector {

  private val MinimumEpochDeltaToTriggerCatchUp = 2
}
