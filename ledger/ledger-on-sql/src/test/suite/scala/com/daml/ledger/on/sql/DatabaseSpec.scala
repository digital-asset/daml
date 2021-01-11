// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.on.sql.Database.InvalidDatabaseException
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class DatabaseSpec extends AsyncWordSpec with Matchers {
  "Database" should {
    "not accept unnamed H2 database URLs" in {
      newLoggingContext { implicit loggingContext =>
        an[InvalidDatabaseException] should be thrownBy
          Database.owner("jdbc:h2:mem:", new Metrics(new MetricRegistry))
      }
    }

    "not accept unnamed SQLite database URLs" in {
      newLoggingContext { implicit loggingContext =>
        an[InvalidDatabaseException] should be thrownBy
          Database.owner("jdbc:sqlite::memory:", new Metrics(new MetricRegistry))
      }
    }
  }
}
