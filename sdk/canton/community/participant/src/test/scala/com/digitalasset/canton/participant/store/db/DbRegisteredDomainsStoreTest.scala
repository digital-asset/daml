// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.RegisteredDomainsStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbRegisteredDomainsStoreTest
    extends AsyncWordSpec
    with BaseTest
    with RegisteredDomainsStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Int] = {
    import storage.api.*
    storage.update(sqlu"truncate table par_domains", functionFullName)
  }

  "DbRegisteredDomainsStore" should {
    behave like registeredDomainsStore(() =>
      new DbRegisteredDomainsStore(storage, timeouts, loggerFactory)
    )
  }
}

class RegisteredDomainsStoreTestH2 extends DbRegisteredDomainsStoreTest with H2Test

class RegisteredDomainsStoreTestPostgres extends DbRegisteredDomainsStoreTest with PostgresTest
