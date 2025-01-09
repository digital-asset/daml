// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.CommandDeduplicationStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

trait DbCommandDeduplicationStoreTest
    extends AsyncWordSpec
    with BaseTest
    with CommandDeduplicationStoreTest {
  this: DbTest =>
  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table par_command_deduplication",
        sqlu"truncate table par_command_deduplication_pruning",
      ),
      functionFullName,
    )
  }

  "DbCommandDeduplicationStore" should {
    behave like commandDeduplicationStore(() =>
      new DbCommandDeduplicationStore(
        storage,
        timeouts,
        testedReleaseProtocolVersion,
        loggerFactory,
      )
    )
  }
}

class CommandDeduplicationStoreTestH2 extends DbCommandDeduplicationStoreTest with H2Test

class CommandDeduplicationStoreTestPostgres
    extends DbCommandDeduplicationStoreTest
    with PostgresTest
