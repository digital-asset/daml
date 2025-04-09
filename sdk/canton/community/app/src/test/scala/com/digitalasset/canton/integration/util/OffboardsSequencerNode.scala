// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.{InstanceReference, SequencerReference}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.topology.SynchronizerId

trait OffboardsSequencerNode {
  import org.scalatest.LoneElement.*
  import org.scalatest.matchers.should.Matchers.*

  protected def offboardSequencer(
      synchronizerId: SynchronizerId,
      sequencerToOffboard: SequencerReference,
      sequencerOnSynchronizer: SequencerReference,
      synchronizerOwners: Set[InstanceReference],
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val synchronizerOwnersNE = NonEmpty
      .from(synchronizerOwners)
      .getOrElse(throw new IllegalArgumentException("synchronizerOwners must not be empty"))

    // fetch the latest SequencerSynchronizerState mapping
    val seqState1 = sequencerOnSynchronizer.topology.sequencers
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
      sequencerOnSynchronizer.topology.sequencers
        .list(store = synchronizerId)
        .loneElement
        .item
        .active
        .forgetNE should not contain sequencerToOffboard.id
    }
  }
}
