// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.DomainConnectionConfigStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbDomainConnectionConfigStoreTest
    extends AsyncWordSpec
    with BaseTest
    with DomainConnectionConfigStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[_] = {
    import storage.api.*
    storage.update_(sqlu"truncate table par_domain_connection_configs", functionFullName)
  }

  "DbDomainConnectionConfigStoreTest" should {
    behave like domainConnectionConfigStore(
      new DbDomainConnectionConfigStore(
        storage,
        testedReleaseProtocolVersion,
        timeouts,
        loggerFactory,
      ).initialize()
    )
  }
}

class DomainConnectionConfigStoreTestH2 extends DbDomainConnectionConfigStoreTest with H2Test

class DomainConnectionConfigStoreTestPostgres
    extends DbDomainConnectionConfigStoreTest
    with PostgresTest
