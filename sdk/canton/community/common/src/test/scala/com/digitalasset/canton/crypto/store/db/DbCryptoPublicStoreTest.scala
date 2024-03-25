// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.db

import com.digitalasset.canton.crypto.store.CryptoPublicStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbCryptoPublicStoreTest extends AsyncWordSpec with CryptoPublicStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*

    storage.update(
      DBIO.seq(
        sqlu"truncate table common_crypto_public_keys"
      ),
      operationName = s"${this.getClass}: Truncate public crypto tables",
    )
  }

  "DbCryptoPublicStore" can {
    behave like cryptoPublicStore(
      new DbCryptoPublicStore(storage, testedReleaseProtocolVersion, timeouts, loggerFactory),
      backedByDatabase = true,
    )
  }
}

class CryptoPublicStoreTestH2 extends DbCryptoPublicStoreTest with H2Test

class CryptoPublicStoreTestPostgres extends DbCryptoPublicStoreTest with PostgresTest
