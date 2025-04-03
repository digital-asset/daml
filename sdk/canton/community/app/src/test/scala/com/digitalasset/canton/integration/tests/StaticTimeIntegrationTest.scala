// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.StorageConfig
import com.digitalasset.canton.damltests.java.statictimetest.Pass
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil

import java.time.Duration
import java.time.temporal.ChronoUnit
import scala.jdk.CollectionConverters.*

trait StaticTimeIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllParticipantConfigs_(ConfigTransforms.useTestingTimeService),
      )

  "run a scenario with static time" in { implicit env =>
    import env.*
    val clock = env.environment.simClock.value // don't have access to console commands here

    participant1.synchronizers.connect_local(sequencer1, daName)
    participant1.dars.upload(CantonTestsPath)
    val alice = participant1.parties.enable("Alice")

    val now = clock.now
    assertResult(CantonTimestamp.Epoch)(now)
    val pass = new Pass(
      "1",
      alice.toProtoPrimitive,
      now.toInstant.plus(12, ChronoUnit.HOURS),
    )
    val passTx = participant1.ledger_api.javaapi.commands
      .submit_flat(Seq(alice), pass.create.commands.asScala.toSeq)
    val passId = JavaDecodeUtil.decodeAllCreated(Pass.COMPANION)(passTx).loneElement.id

    logger.info("Progress the sim clock by a day")
    clock.advance(Duration.ofDays(1))

    participant1.ledger_api.javaapi.commands
      .submit_flat(Seq(alice), passId.exercisePassTime().commands.asScala.toSeq)
  }

  "advance the static time through the ledger API testing time service" in { implicit env =>
    import env.*

    val preAdvancedTime = clue(s"getting time for the first time") {
      participant1.ledger_api.time.get()
    }

    val expectedAdvancedTime = preAdvancedTime.plusSeconds(5)

    clue(s"advancing time") {
      participant1.ledger_api.time.set(preAdvancedTime, expectedAdvancedTime)
    }

    val actualAdvancedTime = clue(s" second get of time") {
      participant1.ledger_api.time.get()
    }

    actualAdvancedTime shouldBe expectedAdvancedTime

    // let's just upload another dar file, giving the daml ledger client a bit more time to finish processing
    // sometimes, we've seen a shutdown exceptions caused by the daml ledger client processing an RX.onError after
    // shutdown
    participant1.ledger_api.packages.upload_dar(CantonExamplesPath)

  }
}

class StaticTimeIntegrationTestInMemory extends StaticTimeIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[StorageConfig.Memory](loggerFactory))
}
