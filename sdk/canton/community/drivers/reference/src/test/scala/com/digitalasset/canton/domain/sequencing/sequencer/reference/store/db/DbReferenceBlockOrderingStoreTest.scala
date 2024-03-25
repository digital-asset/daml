// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.domain.sequencing.sequencer.reference.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.domain.sequencing.sequencer.reference.store.{
  DbReferenceBlockOrderingStore,
  ReferenceBlockOrderingStoreTest,
}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, MigrationMode, PostgresTest}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbReferenceBlockOrderingStoreTest
    extends AsyncWordSpec
    with BaseTest
    with ReferenceBlockOrderingStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] = {
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
