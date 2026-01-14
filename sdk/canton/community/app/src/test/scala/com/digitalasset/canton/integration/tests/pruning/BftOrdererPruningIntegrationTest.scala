// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pruning

import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochNumber,
}

import java.time.Duration
import scala.concurrent.duration.*

class BftOrdererPruningIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S2M1
      .addConfigTransform(ConfigTransforms.useStaticTime)

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  "we can prune the bft orderer" in { implicit env =>
    import env.*

    val simClock = env.environment.simClock.value

    participant1.synchronizers.connect_local(sequencer1, daName)
    participant2.synchronizers.connect_local(sequencer2, daName)

    participant1.health.maybe_ping(participant2) shouldBe defined
    simClock.advance(Duration.ofSeconds(10))
    participant2.health.maybe_ping(participant1) shouldBe defined

    sequencer1.bft.pruning.status().lowerBoundBlockNumber shouldBe BlockNumber.First

    // user-manual-entry-begin: BftSequencerPruning
    sequencer1.bft.pruning.prune(retention = 30.days, minBlocksToKeep = 100)
    val status = sequencer1.bft.pruning.status()
    // user-manual-entry-end: BftSequencerPruning

    // nothing got pruned
    status.lowerBoundBlockNumber shouldBe EpochNumber.First

    // shorter with shorter retention period and minBlocksToKeep, stuff will get pruned much more aggressively
    sequencer1.bft.pruning.prune(retention = 1.second, minBlocksToKeep = 5)
    val status2 = sequencer1.bft.pruning.status()

    status2.lowerBoundEpochNumber should be > EpochNumber.First
    status2.lowerBoundBlockNumber should be > BlockNumber.First
  }

  "be able to define and clear the pruning schedule" in { implicit env =>
    import env.*
    sequencer1.bft.pruning.get_bft_schedule() shouldBe empty

    // user-manual-entry-begin: BftSequencerScheduledPruning
    sequencer1.bft.pruning.set_bft_schedule(
      cron = "0 0 8 ? * SAT",
      maxDuration = 8.hours,
      retention = 90.days,
      minBlocksToKeep = 50,
    )
    sequencer1.bft.pruning.set_min_blocks_to_keep(100)
    val schedule = sequencer1.bft.pruning.get_bft_schedule()
    // user-manual-entry-end: BftSequencerScheduledPruning

    schedule shouldBe defined
    schedule.map(_.minBlocksToKeep) should contain(100)

    sequencer1.bft.pruning.clear_schedule()
    sequencer1.bft.pruning.get_bft_schedule() shouldBe empty
  }

  "prune based on the schedule" in { implicit env =>
    import env.*
    val simClock = env.environment.simClock.value
    // the cron schedule interval logic has weird behavior with dates close to the epoch timestamp, so moving it ahead a bit
    simClock.advance(Duration.ofDays(10))

    participant1.health.maybe_ping(participant2) shouldBe defined

    val status1 = sequencer1.bft.pruning.status()

    // prune every second
    sequencer1.bft.pruning.set_bft_schedule(
      cron = "* * * ? * *",
      maxDuration = 8.hours,
      retention = 1.second,
      minBlocksToKeep = 5,
    )

    eventually() {
      simClock.advance(Duration.ofSeconds(1))
      // check that pruning happened (lower bound increased)
      val status2 = sequencer1.bft.pruning.status()
      status2.lowerBoundBlockNumber shouldBe >(status1.lowerBoundBlockNumber)
    }
  }
}
