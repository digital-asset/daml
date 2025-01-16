// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.RegisteredSynchronizersStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

trait DbRegisteredSynchronizersStoreTest
    extends AsyncWordSpec
    with BaseTest
    with RegisteredSynchronizersStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] = {
    import storage.api.*
    storage.update(sqlu"truncate table par_synchronizers", functionFullName)
  }

  "DbRegisteredSynchronizersStore" should {
    behave like registeredSynchronizersStore(() =>
      new DbRegisteredSynchronizersStore(storage, timeouts, loggerFactory)
    )
  }
}

class RegisteredSynchronizersStoreTestH2 extends DbRegisteredSynchronizersStoreTest with H2Test

class RegisteredSynchronizersStoreTestPostgres
    extends DbRegisteredSynchronizersStoreTest
    with PostgresTest
