// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, PostgresStatsAnalysis, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, LogReporter}
import org.scalatest.Args
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

/** In CI: this test is to be executed manually to check for slow queries **after** running other
  * tests as a workload. Locally if you are using your own Postgres instance you can run it after
  * your tests to analyze the query plans. To reset the statistics before the tests you can run:
  * `SELECT pg_stat_statements_reset()` and `SELECT pg_store_plans_reset();`
  */
class PostgresStatsAnalysisTest
    extends AsyncWordSpec
    with BaseTest
    with FlagCloseable
    with HasCloseContext
    with Matchers
    with DbTest
    with PostgresTest {

  "Canton SQL queries" should {
    "not have expensive query plans" in {
      new PostgresStatsAnalysis(storage, timeouts).run(
        testName = None,
        args = Args(reporter = new LogReporter),
      )
      // TODO(#29772): See if an assertion would be necessary here
      succeed
    }
  }

  override def cleanDb(storage: DbStorage)(implicit tc: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.unit
}
