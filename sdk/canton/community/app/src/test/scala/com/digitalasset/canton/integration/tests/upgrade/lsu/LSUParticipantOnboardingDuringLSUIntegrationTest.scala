// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax

import java.time.Duration

/** Ensures that no participant can onboard on the new synchronizer before the upgrade time.
  */
final class LSUParticipantOnboardingDuringLSUIntegrationTest extends LSUBase {
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )

  override protected def testName: String = "lsu-participant-onboarding-during-lsu"

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
        defaultEnvironmentSetup(participantsOverride = Some(Seq(env.participant1)))
      }

  "Onboarding on the new synchronizer" should {
    "not be possible before upgrade time" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      val alice = participant1.parties.enable("Alice")
      IouSyntax.createIou(participant1)(alice, alice)

      // P2 does not know any synchronizer
      participant2.synchronizers.list_registered() shouldBe empty

      performSynchronizerNodesLSU(fixture)

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant2.synchronizers.connect(sequencer2, daName),
        _.errorMessage should include(
          s"Onboarding is possible only from $upgradeTime but current time is"
        ),
      )

      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

      eventually() {
        participant1.synchronizers.is_connected(fixture.newPSId) shouldBe true

        // P2 can now join
        participant2.synchronizers.connect(sequencer2, daName)
      }

      oldSynchronizerNodes.all.stop()

      environment.simClock.value.advance(Duration.ofSeconds(1))
      waitForTargetTimeOnSequencer(sequencer2, environment.clock.now)
    }
  }
}
