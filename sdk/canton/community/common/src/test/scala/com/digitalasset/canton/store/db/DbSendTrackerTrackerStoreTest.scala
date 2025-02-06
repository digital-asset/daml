// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.{IndexedSynchronizer, SendTrackerStoreTest}
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

trait DbSendTrackerTrackerStoreTest extends AsyncWordSpec with BaseTest with SendTrackerStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(DBIO.seq(sqlu"truncate table sequencer_client_pending_sends"), functionFullName)
  }

  "DbPendingSendStore" should {
    behave like sendTrackerStore(() =>
      new DbSendTrackerStore_Unused(
        storage,
        IndexedSynchronizer.tryCreate(DefaultTestIdentities.synchronizerId, 1),
        timeouts,
        loggerFactory,
      )
    )

  }
}

class SendTrackerTrackerStoreTestH2 extends DbSendTrackerTrackerStoreTest with H2Test

class SendTrackerTrackerStoreTestPostgres extends DbSendTrackerTrackerStoreTest with PostgresTest
