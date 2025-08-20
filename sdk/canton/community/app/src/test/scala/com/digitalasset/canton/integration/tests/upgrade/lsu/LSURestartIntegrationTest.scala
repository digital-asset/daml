// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.upgrade.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.tests.upgrade.lsu.LSUBase.Fixture
import monocle.macros.syntax.lens.*

/*
 * This test is used to test LSU works well when participants are restarted.
 */
abstract class LSURestartIntegrationTest extends LSUBase {

  override protected def testName: String = "lsu-restart"

  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      // Set a connection pool timeout larger than the upgrade time, otherwise it may trigger when we advance the simclock
      .addConfigTransform(
        _.focus(_.parameters.timeouts.processing.sequencerInfo)
          .replace(config.NonNegativeDuration.ofSeconds(40))
      )
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)

        /*
        Without that, an additional fetch_time is needed after the restart because the last known
        timestamp is considered as sufficiently recent.
         */
        participants.all.foreach(
          _.synchronizers.modify(
            daName,
            _.focus(_.timeTracker.observationLatency)
              .replace(config.NonNegativeFiniteDuration.ofMillis(0))
              .focus(_.timeTracker.patienceDuration)
              .replace(config.NonNegativeFiniteDuration.ofMillis(0)),
          )
        )

        participants.all.dars.upload(CantonExamplesPath)

        synchronizerOwners1.foreach(
          _.topology.synchronizer_parameters.propose_update(
            daId,
            _.copy(reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)),
          )
        )

        oldSynchronizerNodes = SynchronizerNodes(Seq(sequencer1), Seq(mediator1))
        newSynchronizerNodes = SynchronizerNodes(Seq(sequencer2), Seq(mediator2))
      }

  "Logical synchronizer upgrade" should {
    "work when participants are restarted" in { implicit env =>
      import env.*

      val fixture = Fixture(daId, upgradeTime)

      participant1.health.ping(participant1)

      performSynchronizerNodesLSU(fixture)

      participants.local.stop()

      participant1.start() // restarted before the upgrade time
      participant1.synchronizers.reconnect_all()

      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

      participant2.start() // restarted after the upgrade time
      participant2.synchronizers.reconnect_all()

      eventually() {
        participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
      }
    }
  }
}

final class LSURestartReferenceIntegrationTest extends LSURestartIntegrationTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
}

final class LSURestartBftOrderingIntegrationTest extends LSURestartIntegrationTest {
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
}
