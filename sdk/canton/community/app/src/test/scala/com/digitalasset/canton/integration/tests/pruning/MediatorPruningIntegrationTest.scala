// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pruning

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.protocol.TestSynchronizerParameters

import scala.concurrent.Future

trait MediatorPruningIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransform(ConfigTransforms.useStaticTime)
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant2.synchronizers.connect_local(sequencer1, alias = daName)
      }

  "pruning the mediator" in { implicit env =>
    import env.*

    val confirmationResponseTimeout =
      TestSynchronizerParameters.defaultDynamic.confirmationResponseTimeout

    // do a bunch of transactions first
    participant1.testing.bong(Set(participant2), levels = 3)

    // make sure we've recorded some responses
    val finalizedResponsesAfterBong = countFinalizedResponses().futureValue
    finalizedResponsesAfterBong shouldBe >(0L)

    // advance past the time that a participant could still respond
    val defaultRetention = environment.config.parameters.retentionPeriodDefaults.mediator
    val responseTimeout = confirmationResponseTimeout
    val prunedBeforeDuration = defaultRetention.asJava.plus(responseTimeout.unwrap)

    environment.simClock.value.advance(prunedBeforeDuration)

    // do something to kick everything to working at this advanced time
    participant1.health.ping(participant2)
    val finalizedResponsesAfterPing = countFinalizedResponses().futureValue

    // we expect that all responses from the bong should be removed after pruning
    val afterPruneExpectedResponseCount = finalizedResponsesAfterPing - finalizedResponsesAfterBong

    // we'll need a deliver event from the clock advancement to reach the mediator's sequencer client in order
    // for the prehead counter to progress
    eventually() {
      // prune the mediator
      mediator1.pruning.prune()

      // should have removed all of the response aggregations as we've exceeded the response window
      val finalizedResponsesAfterPrune = countFinalizedResponses().futureValue

      withClue(s"expecting $afterPruneExpectedResponseCount responses stored after pruning") {
        finalizedResponsesAfterPrune shouldBe afterPruneExpectedResponseCount
      }
    }
  }

  private def countFinalizedResponses()(implicit
      env: TestConsoleEnvironment
  ): Future[Long] = {
    val daMediatorStateInspection =
      env.mediator1.underlying
        .flatMap(_.replicaManager.mediatorRuntime.map(_.mediator.stateInspection))
        .value

    daMediatorStateInspection.finalizedResponseCount()
  }.onShutdown(throw new RuntimeException("Unexpected shutdown."))(env.executionContext)
}

//class MediatorPruningIntegrationTestH2 extends MediatorPruningIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//  registerPlugin (
//    new UseReferenceBlockSequencer[DbConfig.H2] (loggerFactory)
//  )
//}

class MediatorPruningReferenceIntegrationTestPostgres extends MediatorPruningIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  )
}

class MediatorPruningBftOrderingIntegrationTestPostgres extends MediatorPruningIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
