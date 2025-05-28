// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples.bftordering

import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.integration.tests.examples.ExampleIntegrationTest.bftSequencerConfigurationFolder
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig.PruningConfig

import scala.concurrent.duration.*

class BftSequencerPruningConfigIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.fromFiles(bftSequencerConfigurationFolder / "pruning.conf")

  "sequencer" should {
    "load pruning config from file" in { implicit env =>
      import env.*
      val sequencer = sequencer1
      inside(sequencer.config.sequencer) { case conf: SequencerConfig.BftSequencer =>
        conf.config.pruningConfig shouldBe PruningConfig(
          enabled = true,
          retentionPeriod = 10.days,
          pruningFrequency = 3.hour,
        )
      }
    }
  }
}
