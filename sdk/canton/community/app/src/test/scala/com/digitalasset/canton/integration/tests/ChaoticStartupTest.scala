// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceSynchronizerDisconnect
import org.slf4j.event.Level

trait ChaoticStartupTest extends CommunityIntegrationTest with SharedEnvironment {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1

  "Canton" can {
    "prepare nodes" in { implicit env =>
      import env.*

      participant1.stop()
    }

    "connect participant2 to synchronizer da" in { implicit env =>
      import env.*

      participant2.synchronizers.connect_local(sequencer1, daName)
    }

    "not ping" in { implicit env =>
      import env.*

      assertThrowsAndLogsCommandFailures(
        participant1.health.ping(participant2.id),
        _.errorMessage should (include("NODE_NOT_STARTED") and include(
          "'participant1' has not been started."
        )),
      )
      assertThrowsAndLogsCommandFailures(
        participant2.health.ping(participant1.id),
        _.errorMessage should (include("NODE_NOT_STARTED") and include(
          "'participant1' has not been started."
        )),
      )
    }

    "start all nodes" in { implicit env =>
      import env.*

      // This must succeed even though some nodes are already running.
      startAll()
    }

    "participant can shutdown" in { implicit env =>
      import env.*

      participant1.stop()
    }

    "participant can shutdown after connected synchronizer is down" in { implicit env =>
      import env.*

      // Stop synchronizer while participant2 is still connected.
      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        {
          sequencer1.stop()
          participant2.stop()
        },
        entries => {
          forAll(entries) { entry =>
            entry.shouldBeCantonErrorCode(SyncServiceSynchronizerDisconnect)
          }
        },
      )

    }
  }
}

//class ChaoticStartupTestDefault extends ChaoticStartupTest

class ChaoticStartupTestPostgres extends ChaoticStartupTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
