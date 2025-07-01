// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.bftordering

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.console.SequencerReference
import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.integration.tests.sequencer.reference.ReferenceDynamicOnboardingIntegrationTestBase
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.sequencing.BftSequencerFactory

class BftOrderingDynamicOnboardingIntegrationTest
    extends ReferenceDynamicOnboardingIntegrationTestBase(BftSequencerFactory.ShortName) {

  import BftOrderingDynamicOnboardingIntegrationTest.*

  override protected val isBftSequencer: Boolean = true

  override protected lazy val plugin: UseBftSequencer = new UseBftSequencer(
    loggerFactory,
    dynamicallyOnboardedSequencerNames = Seq(dynamicallyOnboardedSequencerName),
  )

  override protected def setUpAdditionalConnections(
      existingSequencerReference: SequencerReference,
      newSequencerReference: SequencerReference,
  ): Unit =
    plugin.sequencerEndpoints.get.foreach { endpoints =>
      val existingSequencerEndpoint =
        endpoints(InstanceName.tryCreate(existingSequencerReference.name))
      val newSequencerEndpoint = endpoints(InstanceName.tryCreate(newSequencerReference.name))
      // user-manual-entry-begin: BftSequencerAddPeerEndpoint
      existingSequencerReference.bft.add_peer_endpoint(newSequencerEndpoint)
      newSequencerReference.bft.add_peer_endpoint(existingSequencerEndpoint)
    // user-manual-entry-end: BftSequencerAddPeerEndpoint
    }
}

object BftOrderingDynamicOnboardingIntegrationTest {
  private val dynamicallyOnboardedSequencerName: InstanceName = InstanceName.tryCreate("sequencer2")
}
