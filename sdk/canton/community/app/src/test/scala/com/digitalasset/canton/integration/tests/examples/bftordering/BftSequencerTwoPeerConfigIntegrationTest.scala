// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples.bftordering

import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.integration.tests.examples.ExampleIntegrationTest.bftSequencerConfigurationFolder

class BftSequencerTwoPeerConfigIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext {

  registerPlugin(new UseBftSequencer(loggerFactory, shouldGenerateEndpointsOnly = true))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.fromFiles(bftSequencerConfigurationFolder / "two-peers.conf")

  "participant" should {
    "successfully connect to both sequencers and ping" in { implicit env =>
      import env.*
      val synchronizerAlias = "bft-sequencer-config-test"
      val participant = lp("participant1")
      val sequencer1 = ls("sequencer1")
      val sequencer2 = ls("sequencer2")

      bootstrap.synchronizer(
        synchronizerAlias,
        sequencers = sequencers.all,
        mediators = mediators.all,
        synchronizerOwners = sequencers.all,
        synchronizerThreshold = PositiveInt.two,
        staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
      )

      participant.synchronizers.connect_local(sequencer1, synchronizerAlias)
      participant.health.ping(participant)
      participant.synchronizers.disconnect_local(synchronizerAlias)

      participant.synchronizers.connect_local(sequencer2, synchronizerAlias)
      participant.health.ping(participant)
    }
  }
}
