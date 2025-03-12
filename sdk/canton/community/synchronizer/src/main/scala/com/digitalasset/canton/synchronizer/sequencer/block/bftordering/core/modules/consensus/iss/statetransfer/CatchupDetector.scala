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
  def shouldCatchUp(localEpoch: EpochNumber)(implicit traceContext: TraceContext): Boolean
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

  override def shouldCatchUp(
      localEpoch: EpochNumber
  )(implicit traceContext: TraceContext): Boolean = {
    val behindEnoughNodes =
      latestKnownNodeEpochs.count { case (_, nodeEpoch) =>
        nodeEpoch >= localEpoch + MinimumEpochDeltaToTriggerCatchUp
      } >= membership.orderingTopology.weakQuorum

    if (behindEnoughNodes) {
      logger.debug(
        s"Detected need for catch-up state transfer while in epoch $localEpoch; epochs are $latestKnownNodeEpochs"
      )
    }

    behindEnoughNodes
  }
}

private object DefaultCatchupDetector {

  private val MinimumEpochDeltaToTriggerCatchUp = 2
}
