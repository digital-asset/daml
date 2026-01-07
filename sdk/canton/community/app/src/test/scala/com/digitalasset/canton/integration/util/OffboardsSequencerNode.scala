// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.{
  InstanceReference,
  LocalSequencerReference,
  SequencerReference,
}
import com.digitalasset.canton.crypto.SigningKeyUsage.{Protocol, SequencerAuthentication}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import org.scalatest.Inspectors.forAll

trait OffboardsSequencerNode {
  import org.scalatest.LoneElement.*
  import org.scalatest.matchers.should.Matchers.*

  // TODO(#22509) Introduce a single off-boarding command
  protected def offboardSequencer(
      synchronizerId: SynchronizerId,
      sequencerToOffboard: LocalSequencerReference,
      sequencersOnSynchronizer: NonEmpty[Seq[SequencerReference]],
      synchronizerOwners: Set[InstanceReference],
      isBftOrderer: Boolean,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val synchronizerOwnersNE = NonEmpty
      .from(synchronizerOwners)
      .getOrElse(throw new IllegalArgumentException("synchronizerOwners must not be empty"))

    // user-manual-entry-begin: SequencerOffboardingRemoveExclusiveKeys
    // Remove all non-namespace exclusive keys
    val sequencerToOffboardPubKeys =
      sequencerToOffboard.keys.public
        .list(filterUsage = Set(SequencerAuthentication, Protocol))
        .map(_.publicKey)
    val sequencerToOffboardExclusivePubKeys =
      sequencerToOffboardPubKeys
        .diff(
          sequencers.all
            .filter(_.id != sequencerToOffboard.id)
            .flatMap(_.keys.public.list())
            .map(_.publicKey)
        )
    sequencerToOffboard.topology.owner_to_key_mappings
      .propose(
        sequencerToOffboard.id,
        sequencerToOffboardExclusivePubKeys,
        ops = TopologyChangeOp.Remove,
      )
    // user-manual-entry-end: SequencerOffboardingRemoveExclusiveKeys

    // fetch the latest SequencerSynchronizerState mapping
    val seqState1 = sequencersOnSynchronizer.head1.topology.sequencers
      .list(store = synchronizerId)
      .headOption
      .getOrElse(fail("No sequencer state found"))
      .item

    // propose the SequencerSynchronizerState that removes the sequencer
    synchronizerOwnersNE
      .foreach(
        _.topology.sequencers.propose(
          synchronizerId,
          threshold = seqState1.threshold,
          active = seqState1.active.filterNot(_ == sequencerToOffboard.id),
        )
      )

    BaseTest.eventually() {
      sequencersOnSynchronizer.head1.topology.sequencers
        .list(store = synchronizerId)
        .loneElement
        .item
        .active
        .forgetNE should not contain sequencerToOffboard.id

      // If the synchronizer is running on BFT sequencers, wait for the ordering topology to be updated as well
      //  before stopping the decommissioned sequencer node, else a 3-strong or less BFT ordering network
      //  may become stuck due to insufficient quorum.
      if (isBftOrderer)
        forAll(sequencersOnSynchronizer.forgetNE) { s =>
          s.bft
            .get_ordering_topology()
            .sequencerIds should not contain sequencerToOffboard.id
        }
    }

    sequencerToOffboard.stop()
  }
}
