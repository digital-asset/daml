// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.reliability

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}

final class QuickRestartAfterStartIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S1M1_Manual

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))

  "start / stop / restart and ping" in { implicit env =>
    import env.*

    clue("start / stop synchronizer") {
      sequencer1.start()
      mediator1.start()
      bootstrap.synchronizer(
        synchronizerName = daName.toProtoPrimitive,
        staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
        synchronizerOwners = Seq(sequencer1),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer1),
        mediators = Seq(mediator1),
      )
      sequencer1.stop()
      mediator1.stop()
    }

    clue("start / stop participant") {
      participant1.start()
      participant1.stop()
    }

    clue("start synchronizer again") {
      sequencer1.start()
      mediator1.start()
    }

    clue("start participant1 again") {
      participant1.start()
    }

    clue("connect participant") {
      participant1.synchronizers.connect_local(sequencer1, alias = daName)
    }

    clue("ping participant") {
      participant1.health.ping(participant1)
    }
  }

}
