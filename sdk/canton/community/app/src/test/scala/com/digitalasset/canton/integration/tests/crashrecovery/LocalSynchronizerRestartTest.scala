// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.crashrecovery

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}

final class LocalSynchronizerRestartTest extends CommunityIntegrationTest with SharedEnvironment {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.withSetup { implicit env =>
      import env.*
      participant1.synchronizers.connect_local(sequencer1, alias = daName)
    }

  "The mediator" when {
    "deleting the clean head" must {
      "recover gracefully" in { env =>
        import env.*

        // Create some transactions
        assertPingSucceeds(participant1, participant1)

        // Rewind clean sequencer counter
        val store =
          mediator1.underlying.value.replicaManager.mediatorRuntime.value.mediator.sequencerCounterTrackerStore
        store.rewindPreheadSequencerCounter(None).futureValueUS

        // Restart synchronizer - mediator will reprocess all events
        // This would trigger an error, if the deduplication store is not cleaned up
        participant1.synchronizers.disconnect(daName)
        mediator1.stop()
        sequencer1.stop()
        sequencer1.start()
        mediator1.start()
        participant1.synchronizers.reconnect_all()

        // Check that the synchronizer is still usable
        assertPingSucceeds(participant1, participant1)
      }
    }
  }
}
