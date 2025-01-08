// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbSynchronizerConnectionConfigStoreTest
    extends AsyncWordSpec
    with BaseTest
    with SynchronizerConnectionConfigStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage)(implicit traceContext: TraceContext): Future[_] = {
    import storage.api.*
    storage.update_(sqlu"truncate table par_synchronizer_connection_configs", functionFullName)
  }

  "DbSynchronizerConnectionConfigStoreTest" should {
    behave like synchronizerConnectionConfigStore(
      new DbSynchronizerConnectionConfigStore(
        storage,
        testedReleaseProtocolVersion,
        timeouts,
        loggerFactory,
      ).initialize()
    )
  }
}

class SynchronizerConnectionConfigStoreTestH2
    extends DbSynchronizerConnectionConfigStoreTest
    with H2Test

class SynchronizerConnectionConfigStoreTestPostgres
    extends DbSynchronizerConnectionConfigStoreTest
    with PostgresTest
