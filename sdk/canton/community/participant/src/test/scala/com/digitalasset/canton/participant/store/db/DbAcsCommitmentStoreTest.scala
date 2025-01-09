// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.{
  AcsCommitmentStoreTest,
  CommitmentQueueTest,
  IncrementalCommitmentStoreTest,
}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait DbAcsCommitmentStoreTest extends AcsCommitmentStoreTest { this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table par_computed_acs_commitments",
        sqlu"truncate table par_received_acs_commitments",
        sqlu"truncate table par_outstanding_acs_commitments",
        sqlu"truncate table par_last_computed_acs_commitments",
        sqlu"truncate table par_commitment_pruning",
      ),
      functionFullName,
    )
  }

  "DbAcsCommitmentStore" should {
    behave like acsCommitmentStore((ec: ExecutionContext) =>
      new DbAcsCommitmentStore(
        storage,
        IndexedSynchronizer.tryCreate(synchronizerId, 1),
        new DbAcsCommitmentConfigStore(storage, timeouts, loggerFactory),
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      )(ec)
    )
  }
}

trait DbIncrementalCommitmentStoreTest extends IncrementalCommitmentStoreTest { this: DbTest =>
  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table par_commitment_snapshot",
        sqlu"truncate table par_commitment_snapshot_time",
      ),
      functionFullName,
    )
  }

  "DbAcsSnapshotStore" should {
    behave like commitmentSnapshotStore((ec: ExecutionContext) =>
      new DbIncrementalCommitmentStore(
        storage,
        IndexedSynchronizer.tryCreate(synchronizerId, 1),
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      )(ec)
    )
  }
}

trait DbCommitmentQueueTest extends CommitmentQueueTest { this: DbTest =>
  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table par_commitment_queue"
      ),
      functionFullName,
    )
  }

  "DbCommitmentQueue" should {
    behave like commitmentQueue((ec: ExecutionContext) =>
      new DbCommitmentQueue(
        storage,
        IndexedSynchronizer.tryCreate(synchronizerId, 1),
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      )(ec)
    )
  }
}

class AcsCommitmentStoreTestH2 extends DbAcsCommitmentStoreTest with H2Test
class IncrementalCommitmentStoreTestH2 extends DbIncrementalCommitmentStoreTest with H2Test
class CommitmentQueueTestH2 extends DbCommitmentQueueTest with H2Test

class AcsCommitmentStoreTestPostgres extends DbAcsCommitmentStoreTest with PostgresTest
class IncrementalCommitmentStoreTestPostgres
    extends DbIncrementalCommitmentStoreTest
    with PostgresTest
class CommitmentQueueTestPostgres extends DbCommitmentQueueTest with PostgresTest
