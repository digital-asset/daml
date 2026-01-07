// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.store

import com.digitalasset.canton.config.{BatchingConfig, CachingConfigs}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, PostgresTest}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.SequencerWriterConfig
import com.digitalasset.canton.synchronizer.sequencer.store.{
  DbSequencerStore,
  DbSequencerStoreTest,
  MultiTenantedSequencerStoreTest,
  SequencerStoreTest,
}
import com.digitalasset.canton.synchronizer.sequencing.HASequencerWriterStoreFactoryTest
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.BytesUnit

/** Adds HA related tests to the basic [[DbSequencerStoreTest]]. Is admittedly odd to combine the
  * [[HASequencerWriterStoreFactoryTest]] here but prevents raciness if these were two separate
  * tests sharing the same DB tables (which would likely cause racy watermark interactions).
  */
trait DbSequencerStoreWithHATest
    extends SequencerStoreTest
    with MultiTenantedSequencerStoreTest
    with HASequencerWriterStoreFactoryTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    DbSequencerStoreTest.cleanSequencerTables(storage)

  "DbSequencerStore" should {
    behave like sequencerStore(() =>
      new DbSequencerStore(
        storage,
        testedProtocolVersion,
        bufferedEventsMaxMemory = BytesUnit.zero,
        bufferedEventsPreloadBatchSize =
          SequencerWriterConfig.DefaultBufferedEventsPreloadBatchSize,
        timeouts,
        loggerFactory,
        sequencerMember,
        blockSequencerMode = true,
        cachingConfigs = CachingConfigs(),
        batchingConfig = BatchingConfig(),
        sequencerMetrics = sequencerMetrics(),
      )
    )
    behave like multiTenantedSequencerStore(() =>
      new DbSequencerStore(
        storage,
        testedProtocolVersion,
        bufferedEventsMaxMemory =
          BytesUnit.zero, // Events cache is not currently supported in HA mode
        bufferedEventsPreloadBatchSize =
          SequencerWriterConfig.DefaultBufferedEventsPreloadBatchSize,
        timeouts,
        loggerFactory,
        sequencerMember,
        blockSequencerMode = true,
        cachingConfigs = CachingConfigs(),
        batchingConfig = BatchingConfig(),
        sequencerMetrics = SequencerMetrics.noop("db-sequencer-store-with-ha-test"),
      )
    )
  }

}

class SequencerStoreWithHATestPostgres extends DbSequencerStoreWithHATest with PostgresTest
