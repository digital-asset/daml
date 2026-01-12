// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology

import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.crypto.FingerprintKeyId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftKeyId,
  BftNodeId,
}
import com.digitalasset.canton.util.MaxBytesToDecompress
import com.google.common.annotations.VisibleForTesting

import OrderingTopology.{
  NodeTopologyInfo,
  isStrongQuorumReached,
  isWeakQuorumReached,
  numToleratedFaults,
  strongQuorumSize,
  weakQuorumSize,
}

/** The current sequencer topology.
  *
  * Being unsorted, sequencer IDs must not be iterated over without sorting first, as the iteration
  * order is not deterministic and could introduce nondeterminism in the protocol and/or simulation
  * testing.
  */
final case class OrderingTopology(
    // NOTE: make sure to change `toString` when adding useful information
    nodesTopologyInfo: Map[BftNodeId, NodeTopologyInfo],
    sequencingParameters: SequencingParameters,
    maxBytesToDecompress: MaxBytesToDecompress,
    activationTime: TopologyActivationTime,
    areTherePendingCantonTopologyChanges: Boolean,
) extends MessageAuthorizer {

  lazy val size: Int = nodesTopologyInfo.size

  lazy val nodes: Set[BftNodeId] = nodesTopologyInfo.keySet

  lazy val sortedNodes: Seq[BftNodeId] = nodes.toList.sorted

  lazy val maxToleratedFaults: Int = numToleratedFaults(nodes.size)

  lazy val weakQuorum: Int = weakQuorumSize(nodes.size)

  lazy val strongQuorum: Int = strongQuorumSize(nodes.size)

  def numFaultsTolerated: Int = numToleratedFaults(nodes.size)

  def contains(id: BftNodeId): Boolean = nodes.contains(id)

  def hasWeakQuorum(validVotes: Int): Boolean =
    isWeakQuorumReached(nodes.size, validVotes)

  def hasStrongQuorum(validVotes: Int): Boolean =
    isStrongQuorumReached(nodes.size, validVotes)

  override def isAuthorized(from: BftNodeId, keyId: BftKeyId): Boolean =
    nodesTopologyInfo.get(from).exists(_.keyIds.contains(keyId))

  override def toString: String = {
    val nodesWithActivationTime =
      nodesTopologyInfo.map { case (nodeId, info) =>
        nodeId -> info.activationTime
      }
    s"""OrderingTopology(activation time = $activationTime,
     | size = $size,
     | weak quorum = $weakQuorum,
     | strong quorum = $strongQuorum,
     | nodes = $nodesWithActivationTime,
     | sequencing parameters = $sequencingParameters,
     | max request size to deserialize = $maxBytesToDecompress,
     | pending topology changes = $areTherePendingCantonTopologyChanges
     |)""".stripMargin
  }
}

object OrderingTopology {

  final case class NodeTopologyInfo(
      activationTime: TopologyActivationTime,
      keyIds: Set[BftKeyId],
  )

  /** A simple constructor for tests so that we don't have to provide timestamps. */
  @VisibleForTesting
  private[bftordering] def forTesting(
      nodes: Set[BftNodeId],
      sequencingParameters: SequencingParameters = SequencingParameters.Default,
      activationTime: TopologyActivationTime = TopologyActivationTime(CantonTimestamp.MinValue),
      areTherePendingCantonTopologyChanges: Boolean = false,
      nodesTopologyInfos: Map[BftNodeId, NodeTopologyInfo] = Map.empty,
  ): OrderingTopology =
    OrderingTopology(
      nodes.view.map { node =>
        node -> nodesTopologyInfos.getOrElse(
          node,
          NodeTopologyInfo(
            activationTime = TopologyActivationTime(CantonTimestamp.MinValue),
            keyIds = Set(FingerprintKeyId.toBftKeyId(Signature.noSignature.authorizingLongTermKey)),
          ),
        )
      }.toMap,
      sequencingParameters,
      // TODO(i10428) Move this method under BftSequencerBaseTest so we can reuse defaultMaxBytesToDecompress
      MaxBytesToDecompress.MaxValueUnsafe,
      activationTime,
      areTherePendingCantonTopologyChanges,
    )

  /** A strong quorum is strictly greater than `(numberOfNodes + numberOfFaults) / 2`.
    *
    * The idea is that faulty nodes could vote twice (once for A and once for !A), by sending
    * different votes to different nodes. Under that assumption, the total number of votes is
    * `numberOfNodes + numberOfFaults`. A node locally decides on an outcome only after receiving
    * more than half of the total number of votes and only if all these votes have the same outcome.
    * That way, two honest nodes will never decide for different outcomes.
    *
    * If `numberOfNodes = 3*numberOfFaults + 1`, then the size of a strong quorum is
    * `2*numberOfFaults + 1`.
    */
  def strongQuorumSize(nodes: Int): Int =
    if (nodes <= 3) nodes
    else {
      // We know that numberOfFaults <= (numberOfNodes - 1) / 3.
      // Hence, strongQuorumSize is the smallest integer strictly greater than 2/3*numberOfNodes - 1/6.
      // By doing a case distinction on `numberOfNodes % 3`, this can be simplified to:
      Math.ceil((nodes.toDouble * 2) / 3).toInt
    }

  /** A weak quorum contains at least one honest vote, provided faulty nodes vote only once. */
  def weakQuorumSize(nodes: Int): Int =
    numToleratedFaults(nodes) + 1

  def isStrongQuorumReached(nodes: Int, validVotes: Int): Boolean =
    validVotes >= strongQuorumSize(nodes)

  def isWeakQuorumReached(nodes: Int, validVotes: Int): Boolean =
    validVotes >= weakQuorumSize(nodes)

  // F as a function of Ns
  def numToleratedFaults(numberOfNodes: Int): Int =
    // N = 3f + 1
    // f = (N - 1) int_div 3
    (numberOfNodes - 1) / 3
}
