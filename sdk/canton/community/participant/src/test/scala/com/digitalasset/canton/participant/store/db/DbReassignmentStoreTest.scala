// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.participant.store.ReassignmentStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.util.ReassignmentTag
import com.digitalasset.canton.util.ReassignmentTag.Target
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbReassignmentStoreTest extends AsyncWordSpec with BaseTest with ReassignmentStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Int] = {
    import storage.api.*
    storage.update(sqlu"truncate table par_reassignments", functionFullName)
  }

  "DbReassignmentStore" should {

    val indexStore = new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 100)

    behave like reassignmentStore(domainId =>
      new DbReassignmentStore(
        storage,
        ReassignmentTag.Target(domainId),
        indexStore,
        Target(testedProtocolVersion),
        new SymbolicPureCrypto,
        futureSupervisor,
        exitOnFatalFailures = true,
        timeouts,
        loggerFactory,
      )
    )
  }
}

class ReassignmentStoreTestH2 extends DbReassignmentStoreTest with H2Test

class ReassignmentStoreTestPostgres extends DbReassignmentStoreTest with PostgresTest
