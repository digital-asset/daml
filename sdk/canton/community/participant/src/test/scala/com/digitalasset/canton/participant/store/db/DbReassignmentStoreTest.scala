// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.participant.store.TransferStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.version.Transfer.TargetProtocolVersion
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbReassignmentStoreTest extends AsyncWordSpec with BaseTest with TransferStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Int] = {
    import storage.api.*
    storage.update(sqlu"truncate table par_reassignments", functionFullName)
  }

  "DbTransferStore" should {
    behave like reassignmentStore(domainId =>
      new DbReassignmentStore(
        storage,
        domainId,
        TargetProtocolVersion(testedProtocolVersion),
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
