// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config.CommitmentSendDelay
import com.digitalasset.canton.config.RequireTypes.NonNegativeProportion
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import monocle.macros.syntax.lens.*

import java.time.Duration
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.jdk.DurationConverters.*

/*
 * This test verifies that contracts created before a logical synchronizer upgrade
 * can be pruned after a logical synchronizer upgrade.
 * It uses 2 participants, 2 sequencers and 2 mediators.
 */
final class LSUPruningIntegrationTest extends LSUBase {

  override protected def testName: String = "lsu-pruning"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )

  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] = Map(
    "sequencer2" -> "sequencer1"
  )
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")

  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .updateTestingConfig(
        _.focus(_.commitmentSendDelay).replace(
          Some(
            CommitmentSendDelay(
              Some(NonNegativeProportion.zero),
              Some(NonNegativeProportion.zero),
            )
          )
        )
      )
      .addConfigTransforms(
        ConfigTransforms.updateMaxDeduplicationDurations(10.minutes.toJava)
      )
      .withSetup { implicit env =>
        defaultEnvironmentSetup()
      }

  "Pruning after a logical synchronizer upgrade" should {
    "work correctly" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      participant1.health.ping(participant2)

      val alice = participant1.parties.enable("Alice")
      val bank = participant2.parties.enable("Bank")
      val tempIou = IouSyntax.createIou(participant2)(bank, alice)
      IouSyntax.archive(participant2)(tempIou, bank)
      IouSyntax.createIou(participant2)(bank, alice).discard

      performSynchronizerNodesLSU(fixture)

      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

      eventually() {
        participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
      }

      oldSynchronizerNodes.all.stop()

      environment.simClock.value.advance(Duration.ofSeconds(1))

      waitForTargetTimeOnSequencer(sequencer2, environment.clock.now)

      val aliceIou =
        participant1.ledger_api.javaapi.state.acs.await(IouSyntax.modelCompanion)(alice)
      val bob = participant1.parties.enable("Bob")

      participant1.ledger_api.javaapi.commands
        .submit(Seq(alice), aliceIou.id.exerciseTransfer(bob.toLf).commands().asScala.toSeq)

      val bobIou = participant1.ledger_api.javaapi.state.acs.await(IouSyntax.modelCompanion)(bob)

      participant2.ledger_api.javaapi.commands
        .submit(Seq(bank), bobIou.id.exerciseArchive().commands().asScala.toSeq)

      environment.simClock.value.advance(1.hour.toJava)
      participants.local.foreach(_.testing.fetch_synchronizer_times())
      IouSyntax.createIou(participant2)(bank, alice)
      environment.simClock.value.advance(1.hour.toJava)
      participants.local.foreach(_.testing.fetch_synchronizer_times())

      eventually() {
        environment.simClock.value.advance(1.hour.toJava)
        participants.local.foreach(_.testing.fetch_synchronizer_times())
        val offset =
          participant2.pruning.find_safe_offset(beforeOrAt = environment.clock.now.toInstant).value
        logger.debug(s"safe to prune: $offset")
        offset should be > 2L
        logger.debug(s"pcs before pruning: ${participant2.testing.pcs_search(daName)}")
        participant2.pruning.prune(offset)
        logger.debug(s"pcs after pruning: ${participant2.testing.pcs_search(daName)}")
      }
    }
  }
}
