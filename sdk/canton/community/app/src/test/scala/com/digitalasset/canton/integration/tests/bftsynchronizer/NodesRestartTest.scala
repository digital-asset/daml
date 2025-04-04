// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

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
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription.LostSequencerSubscription
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId

trait NodesRestartTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with NodeTestingUtils {

  override def environmentDefinition: EnvironmentDefinition =
    // not using withManualStart because we use Environment#startAndReconnect as part of the test
    EnvironmentDefinition.P1_S1M1

  "Restart participant nodes not connected to a synchronizer" in { implicit env =>
    import env.*

    stopAndWait(participant1)
    startAndWait(participant1)

    // participants run a package vetting process on startup.
    // let's make sure that this mechanism doesn't run again on the second startup
    eventually() {
      val result = participant1.topology.vetted_packages
        .list(store = TopologyStoreId.Authorized)
      result should have size (1)
      result.head.item.packages shouldNot be(empty)
      result.head.context.serial shouldBe PositiveInt.one
    }
  }

  "Restart an onboarded mediator node" in { implicit env =>
    import env.*
    stopAndWait(mediator1)
    startAndWait(mediator1)
  }

  "Restart an onboarded sequencer node" in { implicit env =>
    import env.*
    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        stopAndWait(sequencer1)
        startAndWait(sequencer1)
      },
      LogEntry.assertLogSeq(
        mustContainWithClue = Seq.empty,
        mayContain = Seq(
          _.warningMessage should include("Health-check service responded NOT_SERVING for"),
          _.warningMessage should include("Token refresh failed with Status{code=UNAVAILABLE"),
          _.shouldBeCantonErrorCode(LostSequencerSubscription), // mediator might squeak
        ),
      ),
    )
  }

  "Restart a participant node and reconnect to a previously connected synchronizer" in {
    implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, daName)

      eventually() {
        val result = participant1.synchronizers.list_connected()
        result should have size (1)
      }

      stopAndWait(participant1)

      // This "simulates" a startup of the Canton application, which triggers and automatic reconnect
      env.environment.startAndReconnect()

      eventually() {
        val result = participant1.synchronizers.list_connected()
        result should have size (1)
      }
  }
}

class NodesRestartTestPostgres extends NodesRestartTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
