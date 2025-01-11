// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.ParticipantPruningSchedulerStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

trait DbParticipantPruningSchedulerStoreTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with ParticipantPruningSchedulerStoreTest {
  this: DbTest =>
  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(DBIO.seq(sqlu"truncate table par_pruning_schedules"), functionFullName)
  }

  "DbParticipantPruningSchedulerStore" should {
    behave like participantPruningSchedulerStore(() =>
      new DbParticipantPruningSchedulerStore(
        storage,
        timeouts,
        loggerFactory,
      )
    )
  }
}

class DbParticipantPruningSchedulerStoreTestH2
    extends DbParticipantPruningSchedulerStoreTest
    with H2Test

class DbParticipantPruningSchedulerStoreTestPostgres
    extends DbParticipantPruningSchedulerStoreTest
    with PostgresTest
