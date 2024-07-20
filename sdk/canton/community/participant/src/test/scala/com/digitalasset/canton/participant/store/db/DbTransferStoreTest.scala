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

trait DbTransferStoreTest extends AsyncWordSpec with BaseTest with TransferStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Int] = {
    import storage.api.*
    storage.update(sqlu"truncate table par_transfers", functionFullName)
  }

  "DbTransferStore" should {
    behave like transferStore(domainId =>
      new DbTransferStore(
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

class TransferStoreTestH2 extends DbTransferStoreTest with H2Test

class TransferStoreTestPostgres extends DbTransferStoreTest with PostgresTest
