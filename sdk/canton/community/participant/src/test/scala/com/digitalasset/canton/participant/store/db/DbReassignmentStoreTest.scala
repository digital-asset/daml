// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.BatchingConfig
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.ReassignmentStoreTest
import com.digitalasset.canton.protocol.ExampleContractFactory
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag
import org.scalatest.wordspec.AsyncWordSpec

trait DbReassignmentStoreTest extends AsyncWordSpec with BaseTest with ReassignmentStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] = {
    import storage.api.*
    storage.update(sqlu"truncate table par_reassignments", functionFullName)
  }

  "DbReassignmentStore" should {

    val indexStore = new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 100)

    behave like reassignmentStore(synchronizerId =>
      new DbReassignmentStore(
        storage,
        ReassignmentTag.Target(synchronizerId),
        indexStore,
        futureSupervisor,
        exitOnFatalFailures = true,
        BatchingConfig(),
        timeouts,
        loggerFactory,
      )
    )
  }

}

class DbContractsTest extends AsyncWordSpec with BaseTest {
  import DbReassignmentStore.DbContracts

  "DbContracts" should {
    "roundTrip" in {
      val contract = ExampleContractFactory.build()

      DbContracts.tryDeserializeOne(
        DbContracts.serializeOne(contract)
      ) shouldBe contract
    }
  }
}

class ReassignmentStoreTestH2 extends DbReassignmentStoreTest with H2Test

class ReassignmentStoreTestPostgres extends DbReassignmentStoreTest with PostgresTest
