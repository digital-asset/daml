// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.ActiveRequestLimitsConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule.LevelAndAbove
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import scala.concurrent.Future

class SequencerApiRateLimitingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Manual
      .addConfigTransform(
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.publicApi.limits).replace(
            Some(
              ActiveRequestLimitsConfig(active =
                Map(
                  com.digitalasset.canton.sequencer.api.v30.SequencerServiceGrpc.METHOD_DOWNLOAD_TOPOLOGY_STATE_FOR_INIT.getFullMethodName -> NonNegativeInt.one,
                  com.digitalasset.canton.sequencer.api.v30.SequencerServiceGrpc.METHOD_SUBSCRIBE.getFullMethodName -> NonNegativeInt.maxValue,
                )
              )
            )
          )
        )
      )
      .withSetup { implicit env =>
        import env.*
        nodes.local.start()
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName.unwrap,
            synchronizerOwners = Seq(sequencer1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
          )
        )(env).bootstrap()
      }

  "Sequencer API open stream limiting" should {
    "reject subscriptions to sequencer message service when limit is reached" in { implicit env =>
      import env.*

      // enforce the limit
      sequencer1.underlying.value.activeRequestCounter.value.updateLimits(
        com.digitalasset.canton.sequencer.api.v30.SequencerServiceGrpc.METHOD_DOWNLOAD_TOPOLOGY_STATE_FOR_INIT.getFullMethodName,
        Some(NonNegativeInt.zero),
      )
      // start connect in background
      loggerFactory.assertLogs(LevelAndAbove(Level.WARN))(
        {
          val background = Future(participant1.synchronizers.connect_local(sequencer1, daName))
          // wait for 5s
          Threading.sleep(5000)
          // must not have completed as otherwise we didn't get blocked
          background.isCompleted shouldBe false
          // increase limit
          sequencer1.underlying.value.activeRequestCounter.value.updateLimits(
            com.digitalasset.canton.sequencer.api.v30.SequencerServiceGrpc.METHOD_DOWNLOAD_TOPOLOGY_STATE_FOR_INIT.getFullMethodName,
            Some(NonNegativeInt.one),
          )
          // wait for it to finish
          background.futureValue
        }
        // retry log noise only starts after 30s
      )

      participant1.health.ping(participant1)

    }
  }

}
