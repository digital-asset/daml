// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*

/** Trivial test which can be used as a first end to end test */
sealed trait SimplestPingIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1

  "we can run a trivial ping" in { implicit env =>
    import env.*

    clue("participant1 connect") {
      participant1.synchronizers.connect_local(sequencer1, daName)
    }
    clue("participant2 connect") {
      participant2.synchronizers.connect_local(sequencer1, daName)
    }
    clue("maybe ping") {
      participant1.health.maybe_ping(
        participant2,
        timeout = 30.seconds,
      ) shouldBe defined
    }
  }
}

class SimplestPingIntegrationTestInMemory extends SimplestPingIntegrationTest {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .addConfigTransform(ConfigTransforms.allInMemory)
      .addConfigTransform(_.focus(_.monitoring.logging.api.messagePayloads).replace(false))

  registerPlugin(new UseBftSequencer(loggerFactory))

}

class SimplestPingBftOrderingIntegrationTestH2 extends SimplestPingIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class SimplestPingBftOrderingIntegrationTestPostgres extends SimplestPingIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
