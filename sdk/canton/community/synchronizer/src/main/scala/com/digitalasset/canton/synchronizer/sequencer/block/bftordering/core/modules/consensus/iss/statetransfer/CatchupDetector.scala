// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable

// We don't foresee multiple implementations, this trait exists only to allow mocking in tests
sealed trait CatchupDetector {
  def updateMembership(newMembership: Membership): Unit
  def updateLatestKnownNodeEpoch(
      node: BftNodeId,
      epochNumber: EpochNumber,
  ): Boolean

  /** Determines whether the local node at epoch number `localEpoch` should start catch up and
    * retrieve blocks from the state transfer protocol.
    *
    * @param localEpoch
    *   the active epoch number of the local node
    * @return
    *   the minimum epoch number supported by at least f+1 peers that the local node should state
    *   transfer to (without interruption), or `None` if the local node is not sufficiently behind
    *   f+1 or more peers.
    */
  def shouldCatchUpTo(localEpoch: EpochNumber)(implicit
      traceContext: TraceContext
  ): Option[EpochNumber]
}

final class DefaultCatchupDetector(
    initialMembership: Membership,
    override val loggerFactory: NamedLoggerFactory,
) extends CatchupDetector
    with NamedLogging {

  import DefaultCatchupDetector.*

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var membership: Membership = initialMembership

  // Transient bookkeeping of the highest epoch number received from each node to determine when catch up should occur
  private val latestKnownNodeEpochs: mutable.Map[BftNodeId, EpochNumber] = mutable.Map.empty

  override def updateMembership(newMembership: Membership): Unit = {
    membership = newMembership

    // Cleanup outdated entries
    latestKnownNodeEpochs.keys.toSeq.foreach { p =>
      if (!membership.otherNodes.contains(p)) {
        latestKnownNodeEpochs.remove(p).discard
      }
    }
  }

  /** Updates the highest known epoch number for the specified node. Returns `true` only if the
    * epoch number was updated for the specified node.
    */
  override def updateLatestKnownNodeEpoch(
      node: BftNodeId,
      epochNumber: EpochNumber,
  ): Boolean = {

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var updated = false

    if (membership.otherNodes.contains(node)) {
      latestKnownNodeEpochs
        .updateWith(node) {
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

  override def shouldCatchUpTo(
      localEpoch: EpochNumber
  )(implicit traceContext: TraceContext): Option[EpochNumber] = {
    val weakQuorum = membership.orderingTopology.weakQuorum

    if (latestKnownNodeEpochs.sizeIs < weakQuorum) {
      // If there are not even f+1 or more entries, there is no need for catch up
      None
    } else {
      // Sort the epoch numbers received from peers, drop all but the highest f+1 entries,
      // and then choose the lowest remaining entry -- this is the highest and safest
      // epoch number (observed by the local node so far) to target for catch up
      val latestEpochNumberThatHasWeakQuorum =
        latestKnownNodeEpochs.view.values.toSeq.sorted
          .drop(latestKnownNodeEpochs.size - weakQuorum)
          .headOption match {
          case None => EpochNumber.First
          case Some(epochNumber) => epochNumber
        }

      if (latestEpochNumberThatHasWeakQuorum >= localEpoch + MinimumEpochDeltaToTriggerCatchUp) {
        logger.debug(
          s"Detected need for catch-up state transfer while in epoch $localEpoch; epochs are $latestKnownNodeEpochs"
        )
        // Take -1 because the latest one may be in progress
        Some(EpochNumber(latestEpochNumberThatHasWeakQuorum - 1))
      } else None
    }
  }
}

private object DefaultCatchupDetector {

  private val MinimumEpochDeltaToTriggerCatchUp = 2
}
