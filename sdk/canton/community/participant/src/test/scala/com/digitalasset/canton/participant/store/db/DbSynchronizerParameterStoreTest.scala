// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.DomainParameterStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

trait DbSynchronizerParameterStoreTest
    extends AsyncWordSpec
    with BaseTest
    with DomainParameterStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] = {
    import storage.api.*
    storage.update(sqlu"truncate table par_static_domain_parameters", functionFullName)
  }

  "DbDomainParameterStore" should {
    behave like domainParameterStore(synchronizerId =>
      new DbSynchronizerParameterStore(synchronizerId, storage, timeouts, loggerFactory)
    )
  }
}

class SynchronizerParameterStoreTestH2 extends DbSynchronizerParameterStoreTest with H2Test

class SynchronizerParameterStoreTestPostgres
    extends DbSynchronizerParameterStoreTest
    with PostgresTest
