// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.{InstanceReference, SequencerReference}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.{ForceFlag, SequencerId, SynchronizerId}
import com.typesafe.scalalogging.Logger
import org.scalatest.LoneElement.*

trait OnboardsNewSequencerNode {
  import org.scalatest.matchers.should.Matchers.*

  protected val isBftSequencer: Boolean = false

  private val logger: Logger = NamedLoggerFactory.root.getLogger(getClass)

  protected def synchronizeTopologyAfterAddingNewSequencer(
      existingSequencer: SequencerReference,
      newSequencerId: SequencerId,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    BaseTest.eventually() {
      existingSequencer.topology.sequencers
        .list(existingSequencer.synchronizer_id.logical)
        .loneElement
        .item
        .allSequencers
        .forgetNE should contain(newSequencerId)
    }
  }

  protected def setUpAdditionalConnections(
      existingSequencer: SequencerReference,
      newSequencer: SequencerReference,
  ): Unit = ()

  protected def onboardNewSequencer(
      synchronizerId: SynchronizerId,
      newSequencer: SequencerReference,
      existingSequencer: SequencerReference,
      synchronizerOwners: Set[InstanceReference],
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val synchronizerOwnersNE = NonEmpty
      .from(synchronizerOwners)
      .getOrElse(throw new IllegalArgumentException("synchronizerOwners must not be empty"))

    // extract onboarding sequencer's identity transactions
    val onboardingSequencerIdentity =
      newSequencer.topology.transactions.identity_transactions()

    // upload onboarding sequencer's identity transactions
    existingSequencer.topology.transactions
      .load(onboardingSequencerIdentity, synchronizerId, ForceFlag.AlienMember)

    logger.info("Uploaded a new sequencer identity")

    // fetch the latest SequencerSynchronizerState mapping
    val seqState1 = existingSequencer.topology.sequencers
      .list(store = synchronizerId)
      .headOption
      .getOrElse(fail("No sequencer state found"))
      .item

    // propose the SequencerSynchronizerState that adds the new sequencer
    synchronizerOwnersNE
      .foreach(
        _.topology.sequencers.propose(
          synchronizerId,
          threshold = seqState1.threshold,
          active = seqState1.active :+ newSequencer.id,
        )
      )

    logger.info("Proposed a sequencer synchronizer state with the new sequencer")

    // wait for SequencerSynchronizerState to be observed by the sequencer
    BaseTest.eventually() {
      val sequencerStates =
        existingSequencer.topology.sequencers.list(store = synchronizerId)

      sequencerStates should not be empty
      val sequencerState = sequencerStates.head
      sequencerState.item.active.forgetNE should contain(newSequencer.id)
    }
    logger.info("New sequencer synchronizer state has been observed")

    synchronizeTopologyAfterAddingNewSequencer(existingSequencer, newSequencer.id)
    logger.info("The new sequencer is part of the ordering topology")

    // now we can establish the sequencer snapshot
    val onboardingState =
      existingSequencer.setup.onboarding_state_for_sequencer(newSequencer.id)

    newSequencer.health.initialized() shouldBe false

    // finally, initialize "newSequencer"
    newSequencer.setup.assign_from_onboarding_state(onboardingState)

    setUpAdditionalConnections(existingSequencer, newSequencer)

    BaseTest.eventually() {
      newSequencer.health.initialized() shouldBe true
    }
  }
}
