// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.BatchAggregatorConfig
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.participant.store.InFlightSubmissionStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbInFlightSubmissionStoreTest
    extends AsyncWordSpec
    with BaseTest
    with InFlightSubmissionStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(sqlu"truncate table in_flight_submission"),
      "clean-up in_flight_submission for test",
    )
  }

  "DbInflightSubmissionStore" should {
    behave like inFlightSubmissionStore { () =>
      new DbInFlightSubmissionStore(
        storage,
        PositiveNumeric.tryCreate(2),
        BatchAggregatorConfig.defaultsForTesting,
        testedReleaseProtocolVersion,
        timeouts,
        loggerFactory,
      )
    }
  }
}

class InFlightSubmissionStoreTestH2 extends DbInFlightSubmissionStoreTest with H2Test

class InFlightSubmissionStoreTestPostgres extends DbInFlightSubmissionStoreTest with PostgresTest
