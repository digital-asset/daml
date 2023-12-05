// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.DomainParameterStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbDomainParameterStoreTest extends AsyncWordSpec with BaseTest with DomainParameterStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Int] = {
    import storage.api.*
    storage.update(sqlu"truncate table static_domain_parameters", functionFullName)
  }

  "DbDomainParameterStore" should {
    behave like domainParameterStore(domainId =>
      new DbDomainParameterStore(domainId, storage, timeouts, loggerFactory)
    )
  }
}

class DomainParameterStoreTestH2 extends DbDomainParameterStoreTest with H2Test

class DomainParameterStoreTestPostgres extends DbDomainParameterStoreTest with PostgresTest
