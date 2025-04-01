// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pruning

import com.digitalasset.canton.config.{DbConfig, PositiveDurationSeconds}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration

import java.time.Duration as JDuration
import scala.math.Ordering.Implicits.*

trait ACSPruningIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils {

  // These three parameters are needed to be able to wait sufficiently long to trigger a pruning timeout
  private val reconciliationInterval = JDuration.ofSeconds(10)
  private val confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(5)
  private val mediatorReactionTimeout = NonNegativeFiniteDuration.tryOfSeconds(5)
  private val acsPruningInterval = NonNegativeFiniteDuration.tryOfSeconds(60)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .withSetup { env =>
        import env.*

        sequencer1.topology.synchronizer_parameters.propose_update(
          daId,
          _.update(
            reconciliationInterval = PositiveDurationSeconds(reconciliationInterval),
            confirmationResponseTimeout = confirmationResponseTimeout.toConfig,
            mediatorReactionTimeout = mediatorReactionTimeout.toConfig,
          ),
        )
      }

  "the acs should not contain entries that are no longer needed" in { implicit env =>
    import env.*
    val clock = environment.simClock.value

    val cleanReplayTime = confirmationResponseTimeout.unwrap.plus(mediatorReactionTimeout.unwrap)
    val commitmentInterval = reconciliationInterval

    val waitingInterval = acsPruningInterval.unwrap.max(commitmentInterval).plusMillis(1)

    participants.all.synchronizers.connect_local(sequencer1, alias = daName)
    participant1.health.ping(participant2)

    val acsCount1 = LedgerPruningIntegrationTest.acsCount(participant1, daName)

    // Advance the safeToPrune point so that the first ping can be pruned
    clock.advance(waitingInterval)
    clock.advance(cleanReplayTime)

    participant1.health.ping(participant2)

    // Advance the time sufficiently to trigger the pruning of the first ping
    clock.advance(waitingInterval)
    participants.all.foreach(_.testing.fetch_synchronizer_times())

    logger.debug("Wait for the background pruning to kick in")

    eventually() {
      // The two contracts from the first ping should no longer be in the ACS
      LedgerPruningIntegrationTest.acsCount(participant1, daName) should be <= acsCount1
    }
  }
}

class AcsPruningIntegrationTestPostgres extends ACSPruningIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

//class AcsPruningIntegrationTestH2 extends ACSPruningIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
//}
