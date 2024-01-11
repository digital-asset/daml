// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.service.store.db

import com.digitalasset.canton.domain.service.store.ServiceAgreementAcceptanceStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbServiceAgreementAcceptanceStoreTest
    extends AsyncWordSpec
    with ServiceAgreementAcceptanceStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*

    storage.update_(
      sqlu"truncate table service_agreement_acceptances",
      operationName = s"${this.getClass}: Truncate service agreement acceptances table",
    )
  }

  "DbServiceAgreementAcceptanceStore" can {
    behave like serviceAgreementAcceptanceStore(
      new DbServiceAgreementAcceptanceStore(
        storage,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      )
    )
  }
}

class ServiceAgreementAcceptanceStoreTestH2
    extends DbServiceAgreementAcceptanceStoreTest
    with H2Test

class ServiceAgreementAcceptanceStoreTestPostgres
    extends DbServiceAgreementAcceptanceStoreTest
    with PostgresTest
