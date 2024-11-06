// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.OrderingTopology.{
  isStrongQuorumReached,
  isWeakQuorumReached,
  permutations,
  strongQuorumSize,
  weakQuorumSize,
}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.google.common.annotations.VisibleForTesting

/** The current sequencer topology.
  *
  * Being unsorted, sequencer IDs must not be iterated over without sorting first, as the iteration order is not
  * deterministic and could introduce nondeterminism in the protocol and/or simulation testing.
  */
final case class OrderingTopology(
    peersFirstKnownAt: Map[SequencerId, EffectiveTime],
    sequencingParameters: SequencingParameters,
    topologySnapshotEffectiveTime: EffectiveTime,
    areTherePendingCantonTopologyChanges: Boolean,
) {

  lazy val peers: Set[SequencerId] = peersFirstKnownAt.keySet

  lazy val weakQuorum: Int = weakQuorumSize(peers.size)

  lazy val strongQuorum: Int = strongQuorumSize(peers.size)

  def contains(id: SequencerId): Boolean = peers.contains(id)

  def hasWeakQuorum(numVotes: Int): Boolean = isWeakQuorumReached(peers.size, numVotes)

  def hasWeakQuorum(quorum: Set[SequencerId]): Boolean = {
    val quorumPeersNotInTopology = quorum.diff(peers)
    isWeakQuorumReached(peers.size, quorum.diff(quorumPeersNotInTopology).size)
  }

  def hasStrongQuorum(numVotes: Int): Boolean =
    isStrongQuorumReached(peers.size, numVotes)

  def successProbabilityOfStaleDissemination(
      previousTopology: OrderingTopology,
      votes: Set[SequencerId],
  ): Double =
    successProbabilityOfStaleDissemination(
      currentTopologyDesiredQuorumSize = weakQuorum,
      previousTopologyDesiredQuorumSize = previousTopology.weakQuorum,
      previousTopology,
      votes,
    )

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Return"))
  private def successProbabilityOfStaleDissemination(
      currentTopologyDesiredQuorumSize: Int,
      previousTopologyDesiredQuorumSize: Int,
      previousTopology: OrderingTopology,
      votes: Set[SequencerId],
  ): Double = {
    if (previousTopologyDesiredQuorumSize < currentTopologyDesiredQuorumSize)
      // The quorum in the current topology can't be reached because, as soon as the smaller quorum in the
      //  previous topology is reached, the dissemination is considered complete.
      return 0.0

    val sharedPeers = peers.intersect(previousTopology.peers)

    if (sharedPeers == previousTopology.peers)
      // The quorum in the current topology can and will be reached,
      //  because it includes the previous topology (and they have the same quorum size).
      return 1.0

    val votesInCurrentTopology = votes.intersect(peers)
    val numberOfVotesToGoInCurrentTopology =
      currentTopologyDesiredQuorumSize - votesInCurrentTopology.size
    val sharedPeersThatHaveNotVotedYet = sharedPeers.diff(votes)
    val numberOfVotesAvailableInSharedPeers = sharedPeersThatHaveNotVotedYet.size

    if (numberOfVotesAvailableInSharedPeers < numberOfVotesToGoInCurrentTopology) {
      // Peers that haven't voted and are in both topologies are less than the missing votes,
      //  so the quorum in the current topology can't be reached.
      return 0.0
    }

    val numberOfVotesToGoInPreviousTopology =
      previousTopologyDesiredQuorumSize - votes.size

    if (numberOfVotesToGoInPreviousTopology < numberOfVotesToGoInCurrentTopology)
      // Votes came in from peers that are not shared and there aren't enough left
      //  for a quorum in the current topology.
      return 0.0

    val votesAvailableInPreviousTopology = previousTopology.peers.diff(votes)
    val numberOfVotesAvailableInPreviousTopology =
      votesAvailableInPreviousTopology.size
    val votesAvailableInPreviousTopologyNotInSharedPeers =
      votesAvailableInPreviousTopology.diff(sharedPeers)
    val numberOfVotesAvailableInPreviousTopologyNotFromSharedPeers =
      votesAvailableInPreviousTopologyNotInSharedPeers.size

    // We want to calculate the probability that, assuming that an in-progress dissemination in a previous topology
    //  is successful (i.e., it results in a valid quorum in the previous topology), it also results in a valid
    //  quorum in the current topology.
    //
    //  For that, we need to count 1. the possible outcomes, 2. the "favorable" outcomes, i.e. the ones that
    //  yield a valid quorum in the current topology, and FP-divide the latter by the former.
    //
    //  The possible outcomes are all the disseminations that yield a valid (weak) quorum in the previous topology.
    //
    //  The favorable outcomes are all the disseminations that also yield a valid (weak) quorum in the current
    //  topology.
    //
    //  An "outcome" here behaves like an ordered sequence obtained by sampling `k` times without repetition
    //  (as nodes can vote only once) from a certain set of nodes of size `n`, i.e., according to combinatorics,
    //  `permutations(n, k)`.
    //
    //  Since in-progress dissemination is happening in the previous topology, rather than in the current one,
    //  the number of possible outcomes is `numberOfVotesToGoInPreviousTopology` ordered samples
    //  without repetition from `votesAvailableInPreviousTopology`.

    val numberOfPossibleOutcomes =
      permutations(
        n = numberOfVotesAvailableInPreviousTopology,
        k = numberOfVotesToGoInPreviousTopology,
      )

    //  When it comes to favorable outcomes, there are 2 cases:
    //
    //  1. If the previous and current topologies have the same quorum size, then no single vote can be wasted,
    //     so the favorable outcomes are thus only the ones from votes in shared peers,
    //     that are `numberOfVotesToGoInPreviousTopology` ordered samples without repetition from
    //     `sharedPeersThatHaveNotVotedYet`.

    val numberOfOutcomesOnlyFromAvailableVotesInSharedPeers =
      permutations(
        n = numberOfVotesAvailableInSharedPeers,
        k = numberOfVotesToGoInPreviousTopology,
      )

    //  2. If the previous topology has a bigger quorum, the favorable outcomes will also include
    //     further votes from the previous topology that are not needed to reach the quorum.
    //
    //     For example, if `numberOfVotesToGoInPreviousTopology - numberOfVotesToGoInCurrentTopology = 1`
    //     and 2 are missing in the previous topology, the favorable outcomes will be all samples without repetition
    //     consisting of 2 votes, that are either:
    //
    //     - `2 = numberOfVotesToGoInPreviousTopology` samples without repetition from the set of shared peers that
    //       have not voted yet, i.e., `numberOfOutcomesOnlyFromAvailableVotesInSharedPeers`
    //
    //     or
    //
    //     - Split into 2 sets of partial outcomes, such that partial outcomes in the first set are independent from
    //       partial outcomes in the second sets, as follows:
    //
    //       a. `1 = numberOfVotesToGoInCurrentTopology` samples without repetition from the set of shared peers that
    //         have not voted yet, i.e., `numberOfPartialOutcomesOnlyFromAvailableVotesInSharedPeers`.
    //       b. `1 = numberOfVotesToGoInPreviousTopology - numberOfVotesToGoInCurrentTopology`, i.e., the remaining
    //         votes samples without repetition from the set of peers in the previous topology that are not shared
    //         and haven't voted yet, i.e., `numberOfPartialOutcomesOnlyFromAvailableVotesNotInSharedPeers`
    //
    //     Sets `a` and `b` of partial outcomes have to be composed to produce a set of complete outcomes,
    //     which means that all ordered sequences of votes from set `a` must be concatenated with all ordered
    //     sequences of votes from set `b`, but also vice versa.
    //
    //     This composition has thus the cardinality of `cartesian_product(a, b) \union cartesian_product(b, a)`,
    //     i.e., twice the cardinality of `cartesian_product(a, b)`.

    val favorableOutcomesAreOnlyFromVotesInSharedPeers =
      numberOfVotesToGoInPreviousTopology == numberOfVotesToGoInCurrentTopology

    val numberOfFavorableOutcomes =
      if (favorableOutcomesAreOnlyFromVotesInSharedPeers) {
        numberOfOutcomesOnlyFromAvailableVotesInSharedPeers
      } else {
        val numberOfPartialOutcomesOnlyFromAvailableVotesInSharedPeers = permutations(
          n = numberOfVotesAvailableInSharedPeers,
          k = numberOfVotesToGoInCurrentTopology,
        )
        val numberOfPartialOutcomesOnlyFromAvailableVotesNotInSharedPeers = permutations(
          n = numberOfVotesAvailableInPreviousTopologyNotFromSharedPeers,
          k = numberOfVotesToGoInPreviousTopology - numberOfVotesToGoInCurrentTopology,
        )
        numberOfOutcomesOnlyFromAvailableVotesInSharedPeers +
          2 * numberOfPartialOutcomesOnlyFromAvailableVotesInSharedPeers *
          numberOfPartialOutcomesOnlyFromAvailableVotesNotInSharedPeers
      }

    numberOfFavorableOutcomes.toDouble / numberOfPossibleOutcomes
  }
}

object OrderingTopology {

  /** A simple constructor for tests so that we don't have to provide timestamps. */
  @VisibleForTesting
  def apply(
      peers: Set[SequencerId],
      sequencingParameters: SequencingParameters = SequencingParameters.Default,
      topologySnapshotEffectiveTime: EffectiveTime = EffectiveTime(CantonTimestamp.MinValue),
      areTherePendingCantonTopologyChanges: Boolean = false,
  ): OrderingTopology =
    OrderingTopology(
      peers.view.map(_ -> EffectiveTime(CantonTimestamp.MinValue)).toMap,
      sequencingParameters,
      topologySnapshotEffectiveTime,
      areTherePendingCantonTopologyChanges,
    )

  // Strong Quorum: >2/3
  def strongQuorumSize(numberOfNodes: Int): Int =
    if (numberOfNodes <= 3) numberOfNodes else Math.ceil((numberOfNodes.toDouble * 2) / 3).toInt

  // Weak Quorum: F+1
  def weakQuorumSize(numberOfNodes: Int): Int =
    numToleratedFaults(numberOfNodes) + 1

  def isStrongQuorumReached(numberOfNodes: Int, numberOfVotes: Int): Boolean =
    numberOfVotes >= strongQuorumSize(numberOfNodes)

  def isWeakQuorumReached(numberOfNodes: Int, numberOfVotes: Int): Boolean =
    numberOfVotes >= weakQuorumSize(numberOfNodes)

  // F as a function of Ns
  private def numToleratedFaults(numberOfNodes: Int): Int =
    // N = 3f + 1
    // f = (N - 1) int_div 3
    (numberOfNodes - 1) / 3

  private def permutations(n: Int, k: Int): Int =
    if (k > n)
      0
    else
      factorial(n) / factorial(n - k)

  private def factorial(n: Int): Int =
    (2 to n).fold(1) { case (acc, i) =>
      acc * i
    }
}
