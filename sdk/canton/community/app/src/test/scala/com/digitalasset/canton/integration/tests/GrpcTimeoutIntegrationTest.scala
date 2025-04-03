// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}

import scala.concurrent.duration.*

trait GrpcTimeoutIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .withSetup { implicit env =>
        import env.*

        participants.all.foreach(_.synchronizers.connect_local(sequencer1, alias = daName))
      }

  "gRPC doesn't time out before ping times out" in { implicit env =>
    import env.*
    participant2.synchronizers.disconnect(daName)
    console.set_command_timeout(300.millis)
    participant1.health.maybe_ping(participant2.id, timeout = 2.seconds) shouldBe None
    participant1.health.maybe_ping(participant1) shouldBe defined
  }
}

class GrpcTimeoutIntegrationTestPostegres extends GrpcTimeoutIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
