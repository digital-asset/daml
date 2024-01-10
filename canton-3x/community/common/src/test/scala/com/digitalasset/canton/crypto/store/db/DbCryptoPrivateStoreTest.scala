// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.db

import com.digitalasset.canton.crypto.store.CryptoPrivateStoreExtendedTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbCryptoPrivateStoreTest extends AsyncWordSpec with CryptoPrivateStoreExtendedTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*

    /* We delete all private keys that ARE NOT encrypted (wrapper_key_id == NULL).
    This conditional delete is to avoid conflicts with the encrypted crypto private store tests. */
    storage.update(
      DBIO.seq(
        sqlu"delete from crypto_private_keys where wrapper_key_id IS NULL"
      ),
      operationName = s"${this.getClass}: Delete from private crypto table",
    )
  }

  "DbCryptoPrivateStore" can {
    behave like cryptoPrivateStoreExtended(
      new DbCryptoPrivateStore(storage, testedReleaseProtocolVersion, timeouts, loggerFactory),
      encrypted = false,
    )
  }
}

class CryptoPrivateStoreTestH2 extends DbCryptoPrivateStoreTest with H2Test

class CryptoPrivateStoreTestPostgres extends DbCryptoPrivateStoreTest with PostgresTest
