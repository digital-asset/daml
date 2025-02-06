// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.{IndexedSynchronizer, SequencedEventStoreTest}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

trait DbSequencedEventStoreTest extends AsyncWordSpec with BaseTest with SequencedEventStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*

    storage.update(
      DBIO.seq(
        sqlu"truncate table common_sequenced_events",
        sqlu"truncate table common_sequenced_event_store_pruning",
      ),
      operationName = s"${this.getClass}: truncate table sequenced_events tables",
    )
  }

  "DbSequencedEventStore" should {
    behave like sequencedEventStore(ec =>
      new DbSequencedEventStore(
        storage,
        IndexedSynchronizer.tryCreate(SynchronizerId.tryFromString("da::default"), 1),
        testedProtocolVersion,
        DefaultProcessingTimeouts.testing,
        loggerFactory,
      )(ec)
    )
  }
}

class SequencedEventStoreTestH2 extends DbSequencedEventStoreTest with H2Test

class SequencedEventStoreTestPostgres extends DbSequencedEventStoreTest with PostgresTest
