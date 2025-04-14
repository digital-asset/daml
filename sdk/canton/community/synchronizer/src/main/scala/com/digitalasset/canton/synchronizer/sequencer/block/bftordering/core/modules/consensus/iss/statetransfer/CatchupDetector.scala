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

  /** If a node should start catching up, returns a minimum epoch number it should transfer to
    * (without interruptions). Otherwise, None.
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
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var aheadNodesNeeded = membership.orderingTopology.weakQuorum
    val latestEpochNumberThatHasWeakQuorum =
      latestKnownNodeEpochs
        .groupBy { case (_, epochNumber) => epochNumber }
        .view
        .mapValues(_.size)
        .toSeq
        .sortBy { case (epochNumber, _) => -epochNumber }
        .foldLeft(EpochNumber.First) { case (acc, (epochNumber, nodes)) =>
          aheadNodesNeeded -= nodes
          if (aheadNodesNeeded > 0) acc else epochNumber
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

private object DefaultCatchupDetector {

  private val MinimumEpochDeltaToTriggerCatchUp = 2
}
