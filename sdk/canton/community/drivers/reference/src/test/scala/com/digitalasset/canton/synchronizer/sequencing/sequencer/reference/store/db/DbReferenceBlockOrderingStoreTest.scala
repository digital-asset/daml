// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, MigrationMode, PostgresTest}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.store.{
  DbReferenceBlockOrderingStore,
  ReferenceBlockOrderingStoreTest,
}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

trait DbReferenceBlockOrderingStoreTest
    extends AsyncWordSpec
    with BaseTest
    with ReferenceBlockOrderingStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(sqlu"truncate table blocks", functionFullName).map(_ => ())
  }

  "DbReferenceBlockOrderingStore" should {
    behave like referenceBlockOrderingStore(() =>
      new DbReferenceBlockOrderingStore(storage, timeouts, loggerFactory)
    )
  }
}

class ReferenceBlockOrderingStoreTestH2 extends DbReferenceBlockOrderingStoreTest with H2Test {
  // migrations are placed in the dev directories so they wont be picked up as part of the official migrations
  override val migrationMode: MigrationMode = MigrationMode.DevVersion
}

class ReferenceBlockOrderingStoreTestPostgres
    extends DbReferenceBlockOrderingStoreTest
    with PostgresTest {
  // migrations are placed in the dev directories so they wont be picked up as part of the official migrations
  override val migrationMode: MigrationMode = MigrationMode.DevVersion
}
