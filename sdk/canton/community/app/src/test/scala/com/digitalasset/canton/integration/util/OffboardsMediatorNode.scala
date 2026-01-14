// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.{
  InstanceReference,
  LocalMediatorReference,
  SequencerReference,
}
import com.digitalasset.canton.topology.SynchronizerId

trait OffboardsMediatorNode {
  import org.scalatest.LoneElement.*
  import org.scalatest.matchers.should.Matchers.*

  protected def offboardMediator(
      synchronizerId: SynchronizerId,
      mediatorToOffboard: LocalMediatorReference,
      sequencerOnSynchronizer: SequencerReference,
      synchronizerOwners: Set[InstanceReference],
  ): Unit = {

    val synchronizerOwnersNE = NonEmpty
      .from(synchronizerOwners)
      .getOrElse(throw new IllegalArgumentException("synchronizerOwners must not be empty"))

    // fetch the latest MediatorSynchronizerState mapping
    val medState1 = sequencerOnSynchronizer.topology.mediators
      .list(Some(synchronizerId))
      .headOption
      .getOrElse(fail("No sequencer state found"))
      .item

    // propose the SequencerSynchronizerState that removes the sequencer
    synchronizerOwnersNE
      .foreach(
        _.topology.mediators.propose(
          synchronizerId,
          threshold = medState1.threshold,
          active = medState1.active.filterNot(_ == mediatorToOffboard.id),
          group = medState1.group,
        )
      )

    BaseTest.eventually() {
      sequencerOnSynchronizer.topology.mediators
        .list(Some(synchronizerId))
        .loneElement
        .item
        .active
        .forgetNE should not contain mediatorToOffboard.id
    }

    mediatorToOffboard.stop()
  }
}
