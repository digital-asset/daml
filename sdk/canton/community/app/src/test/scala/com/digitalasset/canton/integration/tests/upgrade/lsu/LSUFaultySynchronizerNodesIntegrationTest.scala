// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S2M2
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.upgrade.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressionRule.LevelAndAbove
import org.scalatest.Assertion
import org.slf4j.event

import scala.concurrent.Future

/*
Check what happens if some of the synchronizer nodes upgrade late or don't upgrade.

Initial topology:
- p1 connected to s1
- p2 connected to s2
- p3 connected to s1, s2 with trust threshold=1
- p4 connected to s1, s2 with trust threshold=2

LSU
- s2 and m2 don't upgrade
- s1 and m1 upgrade to s3 and m3; m3 upgrade is done late (after the upgrade time)
  - We check that a request submitted to s3 after upgrade time but before m3 upgrade succeeds eventually

What happens
- p1 automatically upgrade
- p2 needs repair
- p3 automatically upgrade (s2 removed from the list of successors)
- p4 needs repair
 */
final class LSUFaultySynchronizerNodesIntegrationTest extends LSUBase {

  override protected def testName: String = "lsu-faulty-synchronizer-nodes"

  // TODO(#30360) Use DA BFT
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1", "sequencer2"), Set("sequencer3")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer3" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator3" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  private var automaticallyUpgraded: Seq[ParticipantReference] = _
  private var manuallyUpgraded: Seq[LocalParticipantReference] = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4S3M3_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S2M2)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_by_config(synchronizerConnectionConfig(sequencer1))
        participant2.synchronizers.connect_by_config(synchronizerConnectionConfig(sequencer2))
        participant3.synchronizers.connect_by_config(
          synchronizerConnectionConfig(Seq(sequencer1, sequencer2), threshold = 1)
        )
        participant4.synchronizers.connect_by_config(
          synchronizerConnectionConfig(Seq(sequencer1, sequencer2), threshold = 2)
        )

        automaticallyUpgraded = Seq(participant1, participant3)
        manuallyUpgraded = Seq(participant2, participant4)

        participants.all.dars.upload(CantonExamplesPath)

        synchronizerOwners1.foreach(
          _.topology.synchronizer_parameters.propose_update(
            daId,
            _.copy(reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)),
          )
        )

        oldSynchronizerNodes = SynchronizerNodes(Seq(sequencer1), Seq(mediator1))
        newSynchronizerNodes = SynchronizerNodes(Seq(sequencer3), Seq(mediator3))
      }

  "Logical synchronizer upgrade" should {
    "work when there are faulty synchronizer nodes" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      participant1.health.ping(participant2)
      participant1.health.ping(participant3)
      // no activity for P4 on purpose: LSU should also work that way

      val upgradeFailureError = s"Upgrade to ${fixture.newPSId} failed"

      val logAssertions: Seq[LogEntry] => Assertion = LogEntry.assertLogSeq(
        Seq(
          (
            _.toString should (include("participant2") and include(upgradeFailureError) and include(
              "No sequencer successor was found"
            )),
            "p2 automatic upgrade failure",
          ),
          (
            _.toString should (include("participant4") and include(upgradeFailureError) and include(
              "Not enough successors sequencers (1) to meet the sequencer threshold (2)"
            )),
            "p4 automatic upgrade failure",
          ),
          (
            entry => {
              entry.toString should include("participant3")
              entry.warningMessage should include(
                s"Missing successor information for the following sequencers: Set($sequencer2). They will be removed from the pool of sequencers."
              )
            },
            "p3 will be connected to only a sequencer after the lsu",
          ),
        )
      )

      val exportDirectory = loggerFactory.assertEventuallyLogsSeq(LevelAndAbove(event.Level.WARN))(
        clue("Migrate s1") {
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

          // Note that mediator1 is not migrated yet
          migrateSequencer(
            migratedSequencer = sequencer3,
            newStaticSynchronizerParameters = fixture.newStaticSynchronizerParameters,
            exportDirectory = exportDirectory,
            oldNodeName = "sequencer1",
          )

          sequencer1.topology.synchronizer_upgrade.sequencer_successors.propose_successor(
            sequencerId = sequencer1.id,
            endpoints = sequencer3.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
            synchronizerId = fixture.currentPSId,
          )

          environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

          exportDirectory
        },
        logAssertions,
      )

      eventually() {
        forEvery(automaticallyUpgraded)(_.synchronizers.is_connected(fixture.newPSId) shouldBe true)
        forEvery(automaticallyUpgraded)(
          _.synchronizers.is_connected(fixture.currentPSId) shouldBe false
        )
      }
      waitForTargetTimeOnSequencer(sequencer3, environment.clock.now)

      participant1.underlying.value.sync
        .connectedSynchronizerForAlias(daName)
        .value
        .numberOfDirtyRequests() shouldBe 0

      /*
         P1 and P2 are automatically upgraded but the ping does not succeed yet because
         the mediator did not migrate yet.
       */
      val pingF = Future(participant1.health.ping(participant3))

      eventually() {
        participant1.underlying.value.sync
          .connectedSynchronizerForAlias(daName)
          .value
          .numberOfDirtyRequests() shouldBe 1 // ping is in-flight

        pingF.isCompleted shouldBe false // ping is not completed
      }

      migrateMediator(
        migratedMediator = mediator3,
        newPSId = fixture.newPSId,
        newSequencers = Seq(sequencer3),
        exportDirectory = exportDirectory,
        oldNodeName = "mediator1",
      )

      pingF.futureValue // ping should succeed

      manuallyUpgraded.foreach { p =>
        p.repair.perform_synchronizer_upgrade(
          currentPhysicalSynchronizerId = fixture.currentPSId,
          successorPhysicalSynchronizerId = fixture.newPSId,
          announcedUpgradeTime = fixture.upgradeTime,
          successorConfig = synchronizerConnectionConfig(sequencer3),
        )
        p.synchronizers.reconnect_all()
      }

      // Activity should be possible for all participants
      participant1.health.ping(participant2)
      participant3.health.ping(participant4)
    }
  }
}
