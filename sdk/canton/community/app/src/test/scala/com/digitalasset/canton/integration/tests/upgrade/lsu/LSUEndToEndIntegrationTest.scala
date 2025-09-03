// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config
import com.digitalasset.canton.config.{DbConfig, SynchronizerTimeTrackerConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.upgrade.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.tests.upgrade.lsu.LSUBase.Fixture
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.sequencing.SequencerConnections

import java.time.Duration
import scala.jdk.CollectionConverters.*

/*
 * This test is used to test the logical synchronizer upgrade.
 * It uses 2 participants, 2 sequencers and 2 mediators.
 */
abstract class LSUEndToEndIntegrationTest extends LSUBase {

  override protected def testName: String = "logical-synchronizer-upgrade"

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
      .withSetup { implicit env =>
        import env.*

        val daSequencerConnection =
          SequencerConnections.single(sequencer1.sequencerConnection.withAlias(daName.toString))
        participants.all.synchronizers.connect(
          SynchronizerConnectionConfig(
            synchronizerAlias = daName,
            sequencerConnections = daSequencerConnection,
            timeTracker = SynchronizerTimeTrackerConfig(observationLatency =
              config.NonNegativeFiniteDuration.Zero
            ),
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
    "work end-to-end" in { implicit env =>
      import env.*

      val fixture = Fixture(daId, upgradeTime)

      participant1.health.ping(participant2)

      val alice = participant1.parties.enable("Alice")
      val bank = participant2.parties.enable("Bank")
      IouSyntax.createIou(participant2)(bank, alice).discard

      performSynchronizerNodesLSU(fixture)

      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

      eventually() {
        participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true

        // The sequencer connection pool internal mechanisms to restart connections rely on the clock time advancing.
        environment.simClock.value.advance(Duration.ofSeconds(1))
      }

      oldSynchronizerNodes.all.stop()

      environment.simClock.value.advance(Duration.ofSeconds(1))

      waitForTargetTimeOnSequencer(sequencer2, environment.clock.now)

      /*
      We do several ping, disconnect, reconnect because reconnect comes with crash-recovery
      and acknowledgements to the sequencers.
       */
      (0 to 2).foreach { i =>
        logger.debug(s"Round $i of ping")
        participant1.health.ping(participant2)
        participants.all.synchronizers.disconnect_all()
        participants.all.synchronizers.reconnect_all()
      }

      val aliceIou =
        participant1.ledger_api.javaapi.state.acs.await(IouSyntax.modelCompanion)(alice)
      val bob = participant1.parties.enable("Bob")

      participant1.ledger_api.javaapi.commands
        .submit(Seq(alice), aliceIou.id.exerciseTransfer(bob.toLf).commands().asScala.toSeq)

      val bobIou = participant1.ledger_api.javaapi.state.acs.await(IouSyntax.modelCompanion)(bob)

      participant2.ledger_api.javaapi.commands
        .submit(Seq(bank), bobIou.id.exerciseArchive().commands().asScala.toSeq)

      // Subsequent call should be successful
      participant1.underlying.value.sync
        .upgradeSynchronizerTo(daId, fixture.synchronizerSuccessor)
        .futureValueUS
        .value shouldBe ()
    }
  }
}

final class LSUEndToEndReferenceIntegrationTest extends LSUEndToEndIntegrationTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
}

final class LSUEndToEndBftOrderingIntegrationTest extends LSUEndToEndIntegrationTest {
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
}
