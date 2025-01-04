// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisEpoch
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.snapshot.SequencerSnapshotAdditionalInfo
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.topology.Membership

/** Detects whether a node is onboarded or should start as usual. */
object BootstrapDetector {

  /** Onboarding is currently assumed if a sequencer snapshot with additional info is provided, the node
    * hasn't completed any epoch, and the node is not the only peer in the ordering topology. The latter is queried
    * based on a topology activation timestamp from the sequencer snapshot additional info when a snapshot is provided.
    *
    * A sequencer snapshot can effectively be used for initialization only once. Subsequent initialization calls
    * are ignored (see the implementation in [[SequencerNode]]).
    * Therefore, if a sequencer becomes initialized before state transfer for onboarding is finished, it will
    * most likely become stuck. In such a case, at the very least, clearing the database would be required before
    * starting the node again. However, domain recovery is currently unsupported in case of onboarding failures
    * or crashes.
    */
  def detect(
      snapshotAdditionalInfo: Option[SequencerSnapshotAdditionalInfo],
      membership: Membership,
      latestCompletedEpoch: EpochStore.Epoch,
  )(abort: String => Nothing): BootstrapKind =
    snapshotAdditionalInfo match {
      case Some(additionalInfo)
          if latestCompletedEpoch == GenesisEpoch && membership.otherPeers.sizeIs > 0 =>
        val startEpoch = additionalInfo.peerActiveAt
          .get(membership.myId)
          .flatMap(_.epochNumber)
          .getOrElse(
            abort("No starting epoch found for new node onboarding")
          )
        BootstrapKind.Onboarding(startEpoch)

      case _ =>
        BootstrapKind.RegularStartup
    }

  sealed trait BootstrapKind extends Product with Serializable
  object BootstrapKind {
    case object RegularStartup extends BootstrapKind
    final case class Onboarding(startEpoch: EpochNumber) extends BootstrapKind
  }
}
