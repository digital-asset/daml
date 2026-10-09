// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.DamlPackageStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext

trait DbDamlPackageStoreTest extends DamlPackageStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"delete from par_dar_packages",
        sqlu"delete from par_daml_packages",
        sqlu"delete from par_dars",
      ),
      functionFullName,
    )
  }

  "DbDamlPackagesDarsStore" should {
    behave like damlPackageStore(() =>
      new DbDamlPackageStore(
        storage,
        timeouts,
        futureSupervisor,
        exitOnFatalFailures = true,
        loggerFactory,
      )
    )
  }
}

class DamlPackageStoreTestH2 extends DbDamlPackageStoreTest with H2Test

class DamlPackageStoreTestPostgres extends DbDamlPackageStoreTest with PostgresTest
