// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

trait DbSequencerStoreTest extends SequencerStoreTest with MultiTenantedSequencerStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] =
    DbSequencerStoreTest.cleanSequencerTables(storage)

  "DbSequencerStore" should {
    behave like sequencerStore(() =>
      new DbSequencerStore(
        storage,
        testedProtocolVersion,
        maxBufferedEventsSize = NonNegativeInt.zero, // test with cache is below
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
        maxBufferedEventsSize = NonNegativeInt.zero, // HA mode does not support events cache
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
        maxBufferedEventsSize = NonNegativeInt.tryCreate(10),
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
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[Unit] = {
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
