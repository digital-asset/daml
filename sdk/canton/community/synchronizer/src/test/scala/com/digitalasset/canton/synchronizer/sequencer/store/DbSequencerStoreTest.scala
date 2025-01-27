// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.synchronizer.sequencer.SequencerWriterConfig
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.BytesUnit

trait DbSequencerStoreTest extends SequencerStoreTest with MultiTenantedSequencerStoreTest {
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
        bufferedEventsMaxMemory = BytesUnit.zero, // test with cache is below
        bufferedEventsPreloadBatchSize =
          SequencerWriterConfig.DefaultBufferedEventsPreloadBatchSize,
        timeouts,
        loggerFactory,
        sequencerMember,
        blockSequencerMode = true,
        CachingConfigs(),
      )
    )
    behave like multiTenantedSequencerStore(() =>
      new DbSequencerStore(
        storage,
        testedProtocolVersion,
        bufferedEventsMaxMemory = BytesUnit.zero, // HA mode does not support events cache
        bufferedEventsPreloadBatchSize =
          SequencerWriterConfig.DefaultBufferedEventsPreloadBatchSize,
        timeouts,
        loggerFactory,
        sequencerMember,
        blockSequencerMode = true,
        CachingConfigs(),
      )
    )
  }
  "DbSequencerStore with cache" should {
    behave like sequencerStore(() =>
      new DbSequencerStore(
        storage,
        testedProtocolVersion,
        bufferedEventsMaxMemory = SequencerWriterConfig.DefaultBufferedEventsMaxMemory,
        bufferedEventsPreloadBatchSize =
          SequencerWriterConfig.DefaultBufferedEventsPreloadBatchSize,
        timeouts,
        loggerFactory,
        sequencerMember,
        blockSequencerMode = true,
        CachingConfigs(),
      )
    )
  }
}

object DbSequencerStoreTest {

  def cleanSequencerTables(
      storage: DbStorage
  )(implicit traceContext: TraceContext, closeContext: CloseContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*

    storage.update(
      DBIO.seq(
        Seq(
          "sequencer_members",
          "sequencer_counter_checkpoints",
          "sequencer_payloads",
          "sequencer_watermarks",
          "sequencer_events",
          "sequencer_acknowledgements",
          "sequencer_lower_bound",
          "seq_traffic_control_consumed_journal",
        )
          .map(name => sqlu"truncate table #$name")*
      ),
      functionFullName,
    )
  }
}

class SequencerStoreTestH2 extends DbSequencerStoreTest with H2Test

class SequencerStoreTestPostgres extends DbSequencerStoreTest with PostgresTest
