// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pruning

import com.digitalasset.canton.admin.api.client.data.BftPruningStatus
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochNumber,
}

import scala.concurrent.duration.*

class BftOrdererPruningIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S2M1

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  "we can prune the bft orderer" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, daName)
    participant2.synchronizers.connect_local(sequencer2, daName)
    participant1.health.maybe_ping(participant2) shouldBe defined
    participant2.health.maybe_ping(participant1) shouldBe defined

    sequencer1.bft.pruning.status() shouldBe BftPruningStatus(EpochNumber.First, BlockNumber.First)

    sequencer1.bft.pruning.prune(retention = 1.second, minBlocksToKeep = 5)

    val status = sequencer1.bft.pruning.status()

    status.lowerBoundEpochNumber should be > EpochNumber.First
    status.lowerBoundBlockNumber should be > BlockNumber.First
  }
}
