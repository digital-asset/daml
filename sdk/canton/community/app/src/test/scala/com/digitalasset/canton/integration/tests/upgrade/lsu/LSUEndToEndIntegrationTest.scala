// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax

import java.time.Duration
import scala.jdk.CollectionConverters.*

/*
 * This test is used to test the logical synchronizer upgrade.
 * It uses 3 participants, 2 sequencers and 2 mediators.
 */
final class LSUEndToEndIntegrationTest extends LSUBase {

  override protected def testName: String = "lsu-end-to-end"

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
    EnvironmentDefinition.P3S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        defaultEnvironmentSetup()
      }

  "Logical synchronizer upgrade" should {
    "work end-to-end" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      participant1.health.ping(participant2)

      val alice = participant1.parties.enable("Alice")
      val bank = participant2.parties.enable("Bank")
      IouSyntax.createIou(participant2)(bank, alice).discard

      performSynchronizerNodesLSU(fixture)

      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

      eventually() {
        participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
        participants.all.forall(_.synchronizers.is_connected(fixture.currentPSId)) shouldBe false
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

      // Ensure that everything still works, even when a participant (3) had done no activity prior to LSU
      participant3.health.ping(participant1)
      val charlie = participant3.parties.enable("Charlie")
      participant1.ledger_api.javaapi.commands
        .submit(Seq(bob), bobIou.id.exerciseTransfer(charlie.toLf).commands().asScala.toSeq)
      val charlieIou =
        participant3.ledger_api.javaapi.state.acs.await(IouSyntax.modelCompanion)(charlie)

      participant2.ledger_api.javaapi.commands
        .submit(Seq(bank), charlieIou.id.exerciseArchive().commands().asScala.toSeq)

      // Subsequent call should be successful
      participant1.underlying.value.sync
        .upgradeSynchronizerTo(daId, fixture.synchronizerSuccessor)
        .futureValueUS
        .value shouldBe ()
    }
  }
}
