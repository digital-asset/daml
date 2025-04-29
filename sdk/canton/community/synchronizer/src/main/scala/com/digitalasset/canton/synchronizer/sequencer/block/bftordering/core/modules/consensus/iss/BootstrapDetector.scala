// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisEpoch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.SequencerSnapshotAdditionalInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership

/** Detects whether a node is onboarded or should start as usual. */
object BootstrapDetector {

  /** Onboarding is currently assumed if a sequencer snapshot with additional info is provided, the
    * node hasn't completed any epoch, and the node is not the only node in the ordering topology.
    * The latter is queried based on a topology activation timestamp from the sequencer snapshot
    * additional info when a snapshot is provided.
    *
    * A sequencer snapshot can effectively be used for initialization only once. Subsequent
    * initialization calls are ignored (see the implementation in
    * [[com.digitalasset.canton.synchronizer.sequencer.SequencerNode]]). Therefore, if a sequencer
    * becomes initialized before state transfer for onboarding is finished, it will most likely
    * become stuck. In such a case, at the very least, clearing the database would be required
    * before starting the node again. However, synchronizer recovery is currently unsupported in
    * case of onboarding failures or crashes.
    */
  def detect(
      epochLength: EpochLength,
      snapshotAdditionalInfo: Option[SequencerSnapshotAdditionalInfo],
      membership: Membership,
      latestCompletedEpoch: EpochStore.Epoch,
  )(abort: String => Nothing): BootstrapKind =
    snapshotAdditionalInfo match {
      case Some(additionalInfo)
          if latestCompletedEpoch == GenesisEpoch && membership.otherNodes.sizeIs > 0 =>
        val activeAt = additionalInfo.nodeActiveAt
          .getOrElse(
            membership.myId,
            abort(s"New node ${membership.myId} not found in sequencer snapshot additional info"),
          )

        val startEpochInfo = EpochInfo(
          activeAt.startEpochNumber.getOrElse(
            abort("No starting epoch number found for new node onboarding")
          ),
          activeAt.firstBlockNumberInStartEpoch.getOrElse(
            abort("No starting epoch's first block number found for new node onboarding")
          ),
          epochLength,
          activeAt.startEpochTopologyQueryTimestamp.getOrElse(
            abort("No starting epoch's topology query timestamp found for new node onboarding")
          ),
        )

        BootstrapKind.Onboarding(startEpochInfo)

      case _ =>
        BootstrapKind.RegularStartup
    }

  sealed trait BootstrapKind extends Product with Serializable
  object BootstrapKind {
    case object RegularStartup extends BootstrapKind
    final case class Onboarding(startEpochInfo: EpochInfo) extends BootstrapKind
  }
}
