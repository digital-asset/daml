// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.BlockTransferResponse

import scala.collection.mutable.ArrayBuffer

/** Used for building a (weak) quorum of matching block transfer response messages. Responses match if,
  * up to their highest common epoch, pre-prepares are the same and commits match (TBD).
  *
  * Uses mutable state for storing groups of matching responses and is not thread-safe.
  */
final class BlockTransferResponseQuorumBuilder(activeMembership: Membership) {

  // Groups responses by matching pre-prepares and commits.
  private val matchingResponseGroups = ArrayBuffer[ArrayBuffer[BlockTransferResponse]]()

  def addResponse(
      response: BlockTransferResponse
  ): Unit =
    // Find a group where the response matches the group representative (first response in a group).
    matchingResponseGroups.find(
      _.headOption.exists(representative => doResponsesMatch(representative, response))
    ) match {
      case Some(existingResponses) =>
        // Make sure that representatives are from the most advanced peer so that we always compare against them.
        val isRepresentativeFromTheMostAdvancedPeer =
          existingResponses.headOption.exists(representative =>
            representative.latestCompletedEpoch > response.latestCompletedEpoch
          )
        // Add to an existing group.
        if (isRepresentativeFromTheMostAdvancedPeer) {
          existingResponses.addOne(response)
        } else {
          existingResponses.prepend(response)
        }
      case None =>
        // Create a new group.
        matchingResponseGroups += ArrayBuffer(response)
    }

  /** Finds a group that has at least a quorum of matching responses. Returns `None` if there's no quorum. */
  def build: Option[Set[BlockTransferResponse]] =
    matchingResponseGroups
      .map(_.toSet)
      .find(_.sizeIs >= activeMembership.orderingTopology.weakQuorum)

  private def doResponsesMatch(
      representative: BlockTransferResponse,
      otherResponse: BlockTransferResponse,
  ) = {
    // Compare up to the highest common epoch.
    val highestCommonEpoch =
      otherResponse.latestCompletedEpoch.min(representative.latestCompletedEpoch)

    def takeCertsUpToHighestCommonEpochAndSort(resp: BlockTransferResponse) =
      resp.commitCertificates
        .takeWhile(_.prePrepare.message.blockMetadata.epochNumber <= highestCommonEpoch)
        .sortBy(_.prePrepare.message.blockMetadata.blockNumber)

    val certs = takeCertsUpToHighestCommonEpochAndSort(representative)
    val otherCerts = takeCertsUpToHighestCommonEpochAndSort(otherResponse)

    // Commits can differ between nodes, so check only based on pre-prepares. Certificates are validated separately.
    certs.map(_.prePrepare) == otherCerts.map(_.prePrepare)
  }
}
