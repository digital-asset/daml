// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.bftordering

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.integration.tests.sequencer.reference.ReferenceDynamicOnboardingIntegrationTestBase
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.sequencing.BftSequencerFactory

class BftOrderingDynamicOnboardingIntegrationTest
    extends ReferenceDynamicOnboardingIntegrationTestBase(BftSequencerFactory.ShortName) {

  import BftOrderingDynamicOnboardingIntegrationTest.*

  // We narrow the type explicitly to access `sequencerEndpoints` later
  override protected lazy val plugin: UseBftSequencer =
    new UseBftSequencer(
      loggerFactory,
      dynamicallyOnboardedSequencerNames = Seq(dynamicallyOnboardedSequencerName),
    )
  override val bftSequencerPlugin: Option[UseBftSequencer] = Some(plugin)
}

object BftOrderingDynamicOnboardingIntegrationTest {

  private val dynamicallyOnboardedSequencerName: InstanceName = InstanceName.tryCreate("sequencer2")
}
