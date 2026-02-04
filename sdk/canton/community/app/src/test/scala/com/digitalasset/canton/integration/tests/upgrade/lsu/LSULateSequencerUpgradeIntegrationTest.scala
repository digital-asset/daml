// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config
import com.digitalasset.canton.config.{DbConfig, NonNegativeDuration}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S2M2
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.upgrade.LogicalUpgradeUtils.SynchronizerNodes
import monocle.macros.syntax.lens.*

/*
Goal:
- Test that if a sequencer upgrade late, it does not create problems

Initial topology:
- p1 connected to s1, s2 with trust threshold=1
- p2 connected to s1, s2 with trust threshold=2
- p3 connected to s2

LSU:
- (s1, m1) upgrade
- all participants can perform the automatic lsu
  - p1 can connect to new synchronizer
  - p2, p3 cannot connect to new synchronizer yet (s2 did not upgrade yet)
- (s2, m2) upgrade
- p2 and p3 automatically connect to the new synchronizer
 */
final class LSULateSequencerUpgradeIntegrationTest extends LSUBase {

  override protected def testName: String = "lsu-late-sequencer"

  // TODO(#30360) Use DA BFT
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1", "sequencer2"), Set("sequencer3", "sequencer4")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer3" -> "sequencer1", "sequencer4" -> "sequencer2")
  override protected lazy val newOldMediators: Map[String, String] =
    Map("mediator3" -> "mediator1", "mediator4" -> "mediator2")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S4M4_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S2M2)
      }
      .addConfigTransforms(configTransforms*)
      .addConfigTransform(
        _.focus(_.parameters.timeouts.processing.sequencerInfo)
          /*
          The first handshake with the new synchronizer fail for P2 and P3 because s2 successor is not up yet.
          The default timeout (before giving up) is 30 seconds and during that time, the simple execution queue
          for the synchronizer connect/disconnect/handshakes is blocked, which means LSU cannot succeed.
          A lower value makes the test faster. A value that is too low would make the test flaky.
           */
          .replace(NonNegativeDuration.ofSeconds(3))
      )
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_by_config(
          synchronizerConnectionConfig(Seq(sequencer1, sequencer2), threshold = 1)
        )

        participant2.synchronizers.connect_by_config(
          synchronizerConnectionConfig(Seq(sequencer1, sequencer2), threshold = 2)
        )

        participant3.synchronizers.connect_by_config(synchronizerConnectionConfig(sequencer2))

        participants.all.dars.upload(CantonExamplesPath)

        synchronizerOwners1.foreach(
          _.topology.synchronizer_parameters.propose_update(
            daId,
            _.copy(reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)),
          )
        )

        oldSynchronizerNodes =
          SynchronizerNodes(Seq(sequencer1, sequencer2), Seq(mediator1, mediator2))
        newSynchronizerNodes =
          SynchronizerNodes(Seq(sequencer3, sequencer4), Seq(mediator3, mediator4))
      }

  "Logical synchronizer upgrade" should {
    "work when there is a late sequencer" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      participants.all.map(p => p.health.ping(p))

      fixture.oldSynchronizerOwners.foreach(
        _.topology.synchronizer_upgrade.announcement
          .propose(fixture.newPSId, fixture.upgradeTime)
      )

      val exportDirectory = exportNodesData(
        SynchronizerNodes(
          sequencers = fixture.oldSynchronizerNodes.sequencers,
          mediators = fixture.oldSynchronizerNodes.mediators,
        ),
        successorPSId = fixture.newPSId,
      )

      // only (s1, m1) is upgrading, not (s2, m2)
      migrateSequencer(
        migratedSequencer = sequencer3,
        newStaticSynchronizerParameters = fixture.newStaticSynchronizerParameters,
        exportDirectory = exportDirectory,
        oldNodeName = "sequencer1",
      )
      migrateMediator(
        migratedMediator = mediator3,
        newPSId = fixture.newPSId,
        newSequencers = Seq(sequencer3),
        exportDirectory = exportDirectory,
        oldNodeName = "mediator1",
      )

      // both successors are announced
      sequencer1.topology.synchronizer_upgrade.sequencer_successors.propose_successor(
        sequencerId = sequencer1.id,
        endpoints = sequencer3.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
        synchronizerId = fixture.currentPSId,
      )
      sequencer2.topology.synchronizer_upgrade.sequencer_successors.propose_successor(
        sequencerId = sequencer2.id,
        endpoints = sequencer4.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
        synchronizerId = fixture.currentPSId,
      )

      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

      // LSU succeeds for all the participants but only p1 is connected to the successor
      eventually() {
        participant1.synchronizers.is_connected(fixture.currentPSId) shouldBe false
        participant1.synchronizers.is_connected(fixture.newPSId) shouldBe true

        participant2.synchronizers.is_connected(fixture.newPSId) shouldBe false
        participant2.synchronizers.is_connected(fixture.currentPSId) shouldBe false

        participant3.synchronizers.is_connected(fixture.newPSId) shouldBe false
        participant3.synchronizers.is_connected(fixture.currentPSId) shouldBe false
      }

      participant1.health.ping(participant1)

      // migrate (s2, m2) to (s4, m4)
      migrateSequencer(
        migratedSequencer = sequencer4,
        newStaticSynchronizerParameters = fixture.newStaticSynchronizerParameters,
        exportDirectory = exportDirectory,
        oldNodeName = "sequencer2",
      )
      migrateMediator(
        migratedMediator = mediator4,
        newPSId = fixture.newPSId,
        newSequencers = Seq(sequencer4),
        exportDirectory = exportDirectory,
        oldNodeName = "mediator2",
      )

      // Now, p2 and p3 automatically connect to the successor
      eventually() {
        participant2.synchronizers.is_connected(fixture.newPSId) shouldBe true
        participant3.synchronizers.is_connected(fixture.newPSId) shouldBe true
      }

      participants.all.map(p => p.health.ping(p))
    }
  }
}
