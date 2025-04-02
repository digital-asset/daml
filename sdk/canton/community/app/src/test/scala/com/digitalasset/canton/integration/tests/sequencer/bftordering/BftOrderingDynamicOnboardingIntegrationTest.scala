// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.bftordering

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.console.SequencerReference
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.integration.tests.sequencer.reference.ReferenceDynamicOnboardingIntegrationTestBase
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftSequencerFactory
import com.digitalasset.canton.topology.SequencerId

class BftOrderingDynamicOnboardingIntegrationTest
    extends ReferenceDynamicOnboardingIntegrationTestBase(BftSequencerFactory.ShortName) {

  import BftOrderingDynamicOnboardingIntegrationTest.*

  override protected val isBftSequencer: Boolean = true

  override protected lazy val plugin: UseBftSequencer = new UseBftSequencer(
    loggerFactory,
    dynamicallyOnboardedSequencerNames = Seq(dynamicallyOnboardedSequencerName),
  )

  override protected def synchronizeTopologyAfterAddingNewSequencer(
      existingSequencer: SequencerReference,
      newSequencerId: SequencerId,
  )(implicit env: TestConsoleEnvironment): Unit =
    // BFT ordering topology is generally effective after sequencer runtime topology (at the next epoch's boundary).
    BaseTest.eventually() {
      existingSequencer.bft.get_ordering_topology().sequencerIds should contain(newSequencerId)
    }

  override protected def setUpAdditionalConnections(
      existingSequencer: SequencerReference,
      newSequencer: SequencerReference,
  ): Unit =
    plugin.sequencerEndpoints.get.foreach { endpoints =>
      val existingSequencerEndpoint = endpoints(InstanceName.tryCreate(existingSequencer.name))
      val newSequencerEndpoint = endpoints(InstanceName.tryCreate(newSequencer.name))
      existingSequencer.bft.add_peer_endpoint(newSequencerEndpoint)
      newSequencer.bft.add_peer_endpoint(existingSequencerEndpoint)
    }
}

object BftOrderingDynamicOnboardingIntegrationTest {
  private val dynamicallyOnboardedSequencerName: InstanceName = InstanceName.tryCreate("sequencer2")
}
