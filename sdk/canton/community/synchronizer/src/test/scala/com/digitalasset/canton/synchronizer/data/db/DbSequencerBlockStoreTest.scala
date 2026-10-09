// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.data.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.{
  BatchingConfig,
  CachingConfigs,
  DefaultProcessingTimeouts,
  PositiveFiniteDuration,
}
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.synchronizer.block.data.db.DbSequencerBlockStore
import com.digitalasset.canton.synchronizer.data.SequencerBlockStoreTest
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.SequencerWriterConfig
import com.digitalasset.canton.synchronizer.sequencer.store.DbSequencerStore
import com.digitalasset.canton.synchronizer.sequencing.integrations.state.{
  DbSequencerStateManagerStore,
  SequencerStateManagerStoreTest,
}
import com.digitalasset.canton.topology.{DefaultTestIdentities, Namespace, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

trait DbSequencerBlockStoreTest
    extends AsyncWordSpec
    with BaseTest
    with SequencerBlockStoreTest
    with SequencerStateManagerStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    val deletes = Seq(
      // Cannot use `truncate` here due to foreign key constraints
      sqlu"truncate table seq_block_height",
      // Cannot use `truncate` here due to foreign key constraints
      sqlu"delete from seq_in_flight_aggregation",
    )
    storage.update(DBIO.seq(deletes*), functionFullName)
  }

  // testing DbSequencerStateManagerStore here since DbSequencerBlockStore uses DbSequencerStateManagerStore
  // and if they were in different test suites, parallel test runs could cause interference
  "DbSequencerStateManagerStore" should {
    behave like sequencerStateManagerStore { () =>
      val sequencerStore = new DbSequencerStore(
        storage = storage,
        protocolVersion = testedProtocolVersion,
        bufferedEventsMaxMemory = SequencerWriterConfig.DefaultBufferedEventsMaxMemory,
        bufferedEventsPreloadBatchSize =
          SequencerWriterConfig.DefaultBufferedEventsPreloadBatchSize,
        timeouts = DefaultProcessingTimeouts.testing,
        loggerFactory = loggerFactory,
        sequencerMember = SequencerId(DefaultTestIdentities.physicalSynchronizerId.uid),
        blockSequencerMode = true,
        cachingConfigs = CachingConfigs(),
        batchingConfig = BatchingConfig(),
        sequencerMetrics = SequencerMetrics.noop(getClass.getName),
      )

      (
        new DbSequencerStateManagerStore(
          storage,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
          batchingConfig = BatchingConfig(
            // Required to test the aggregation pruning query batching
            maxPruningTimeInterval = PositiveFiniteDuration.ofSeconds(1)
          ),
          sequencerStore,
        ),
        sequencerStore,
      )
    }
  }

  "DbSequencerBlockStoreTest" should {
    val sequencerId =
      SequencerId.tryCreate("sequencer", Namespace(Fingerprint.tryFromString("default")))
    behave like sequencerBlockStore {
      val sequencerStore = new DbSequencerStore(
        storage,
        testedProtocolVersion,
        bufferedEventsMaxMemory = SequencerWriterConfig.DefaultBufferedEventsMaxMemory,
        bufferedEventsPreloadBatchSize =
          SequencerWriterConfig.DefaultBufferedEventsPreloadBatchSize,
        timeouts = timeouts,
        loggerFactory = loggerFactory,
        sequencerMember = sequencerId,
        blockSequencerMode = true,
        cachingConfigs = CachingConfigs(),
        batchingConfig = BatchingConfig(),
        sequencerMetrics = SequencerMetrics.noop("db-sequencer-block-store-test"),
      )

      val blockStore = new DbSequencerBlockStore(
        storage,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
        batchingConfig = BatchingConfig(),
        sequencerStore,
      )
      (sequencerStore, blockStore)
    }
  }
}

class SequencerBlockStoreTestH2 extends DbSequencerBlockStoreTest with H2Test
class SequencerBlockStoreTestPostgres extends DbSequencerBlockStoreTest with PostgresTest
