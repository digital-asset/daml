// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.BatchAggregatorConfig
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.InFlightSubmissionStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

trait DbInFlightSubmissionStoreTest
    extends AsyncWordSpec
    with BaseTest
    with InFlightSubmissionStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(sqlu"truncate table par_in_flight_submission"),
      "clean-up par_in_flight_submission for test",
    )
  }

  "DbInflightSubmissionStore" should {
    behave like inFlightSubmissionStore { () =>
      new DbInFlightSubmissionStore(
        storage,
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
