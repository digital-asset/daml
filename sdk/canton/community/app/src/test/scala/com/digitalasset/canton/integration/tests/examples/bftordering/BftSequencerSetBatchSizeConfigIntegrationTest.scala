// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples.bftordering

import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.integration.tests.examples.ExampleIntegrationTest.bftSequencerConfigurationFolder

class BftSequencerSetBatchSizeConfigIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext {

  registerPlugin(new UseBftSequencer(loggerFactory, shouldGenerateEndpointsOnly = true))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.fromFiles(bftSequencerConfigurationFolder / "set-batch-size.conf")

  "participant" should {
    "successfully connect to synchronizer and ping" in { implicit env =>
      import env.*
      val participant = lp("participant1")
      val sequencer = ls("sequencer1")
      val synchronizerAlias = "bft-sequencer-config-test"
      bootstrap.synchronizer(
        synchronizerAlias,
        sequencers = Seq(sequencer),
        mediators = mediators.all,
        synchronizerOwners = Seq(sequencer),
        synchronizerThreshold = PositiveInt.one,
        staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
      )
      participant.synchronizers.connect_local(sequencer, synchronizerAlias)
      participant.health.ping(participant)
    }
  }
}
