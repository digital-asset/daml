// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.SubmissionTrackerStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.store.{IndexedSynchronizer, PrunableByTimeParameters}
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.tracing.TraceContext

trait DbSubmissionTrackerStoreTest extends SubmissionTrackerStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table par_fresh_submitted_transaction",
        sqlu"truncate table par_fresh_submitted_transaction_pruning",
      ),
      "clean-up SubmissionTrackerStore tables for test",
    )
  }

  private def mkDbStore(): DbSubmissionTrackerStore =
    new DbSubmissionTrackerStore(
      storage,
      IndexedSynchronizer.tryCreate(DefaultTestIdentities.synchronizerId, 1),
      PrunableByTimeParameters.testingParams,
      timeouts,
      loggerFactory,
    )

  "DbSubmissionTrackerStore" should {
    behave like submissionTrackerStore(() => mkDbStore())
  }
}

class SubmissionTrackerStoreTestH2 extends DbSubmissionTrackerStoreTest with H2Test

class SubmissionTrackerStoreTestPostgres extends DbSubmissionTrackerStoreTest with PostgresTest
