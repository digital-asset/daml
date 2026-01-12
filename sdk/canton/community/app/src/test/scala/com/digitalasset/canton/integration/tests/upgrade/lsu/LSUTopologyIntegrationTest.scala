// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.topology.TopologyManagerError
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.topology.store.db.DbTopologyStore
import com.digitalasset.canton.topology.transaction.TopologyMapping
import org.slf4j.event.Level

/*
 * This test is used to test topology related aspects of LSU.
 */
final class LSUTopologyIntegrationTest extends LSUBase {

  override protected def testName: String = "lsu-topology"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")

  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        defaultEnvironmentSetup()
      }

  "Logical synchronizer upgrade" should {
    "logical upgrade state cannot be queried if no upgrade is ongoing" in { implicit env =>
      assertThrowsAndLogsCommandFailures(
        env.sequencer1.topology.transactions.logical_upgrade_state(),
        _.shouldBeCommandFailure(
          TopologyManagerError.NoOngoingSynchronizerUpgrade,
          "The operation cannot be performed because no upgrade is ongoing",
        ),
      )
    }

    "the upgrade time must be sufficiently in the future" in { implicit env =>
      import env.*

      synchronizerOwners1.foreach(owner =>
        assertThrowsAndLogsCommandFailures(
          owner.topology.synchronizer_upgrade.announcement
            .propose(
              daId.copy(serial = NonNegativeInt.one),
              CantonTimestamp.Epoch.minusSeconds(10),
            ),
          _.shouldBeCommandFailure(TopologyManagerError.InvalidUpgradeTime),
        )
      )
    }

    "work end-to-end" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()
      val newPSId = fixture.newPSId
      val newStaticSynchronizerParameters = fixture.newStaticSynchronizerParameters

      participant1.health.ping(participant1)

      // perform the synchronizer node upgrade.
      // this also announces the sequencer successors, which the participants will use
      // as the oportunity to initialize the successor synchronizer's topology ahead of the ugprade time.
      // the assertion below verifies that the topology state was indeed copied locally.
      // unfortunately, the least effort way of doing this is to assert on the log message
      // emitted by the local copy process.
      loggerFactory.assertEventuallyLogsSeq(
        SuppressionRule.forLogger[DbTopologyStore[?]] && SuppressionRule.Level(Level.INFO)
      )(
        performSynchronizerNodesLSU(fixture),
        entries => {
          // all participants must log that the state was copied locally
          forAll(participants.all)(participant =>
            forExactly(1, entries) { msg =>
              msg.infoMessage should include regex (raw"Transferred \d+ topology transactions from ${fixture.currentPSId} to ${fixture.newPSId}".r)
              msg.loggerName should include(s"participant=${participant.name}")
            }
          )
        },
      )

      // validate the successor sequencer's topology state
      sequencer2.synchronizer_parameters.static.get() shouldBe newStaticSynchronizerParameters
      val allProposals = sequencer2.topology.transactions.list(
        newPSId,
        proposals = true,
        timeQuery = TimeQuery.Range(None, None),
      )
      forAll(allProposals.result)(proposal => proposal.validUntil shouldBe empty)

      val allLSUMappings = sequencer2.topology.transactions.list(
        newPSId,
        filterMappings = TopologyMapping.Code.logicalSynchronizerUpgradeMappings.toSeq,
        timeQuery = TimeQuery.Range(None, None),
      )
      allLSUMappings.result shouldBe empty

      // fetch the upgrade state from the predecessor sequencer
      val upgradeStateFromPredecessorSequencer =
        sequencer1.topology.transactions.logical_upgrade_state()

      // fetch the participant's upgrade state for the predecessor synchronizer for a later comparison
      val firstUpgradeState =
        participant1.topology.transactions.logical_upgrade_state(synchronizer1Id)

      // advance the time past the upgrade time, so that the participant connects to the new physical synchronizer
      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

      eventually() {
        participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
      }

      // propose another upgrade, so that we can fetch the logical upgrade state after the upgrade.
      val temporaryPsid =
        fixture.newPSId.copy(serial = fixture.newPSId.serial.increment.toNonNegative)
      val temporaryUpgradeTime = environment.simClock.value.now.plusSeconds(10)

      Seq[InstanceReference](sequencer2, mediator2).foreach(
        _.topology.synchronizer_upgrade.announcement.propose(
          temporaryPsid,
          temporaryUpgradeTime,
        )
      )

      // fetching the logical upgrade state after the upgrade should contain exactly the same state
      // as the upgrade state before the migration
      val secondUpgradeState =
        participant1.topology.transactions.logical_upgrade_state(fixture.newPSId)

      // compare the predecessor sequencer's upgrade state with the participant's predecessor upgrade state.
      // this shows that both the predecessor sequencer and the participant export the same upgrade state for fixture.currentPSId
      upgradeStateFromPredecessorSequencer shouldBe firstUpgradeState

      // now compare the participant's "temporary" update state with the participant's predecessor upgrade state.
      // since no topology changes were made, they should be the same, from which follows that it must also be the same the sequencer's, and therefore the local copy was correct.
      firstUpgradeState shouldBe secondUpgradeState

      // revoke the temporary upgrade announcement
      Seq[InstanceReference](sequencer2, mediator2).foreach(
        _.topology.synchronizer_upgrade.announcement.revoke(temporaryPsid, temporaryUpgradeTime)
      )
    }
  }
}
