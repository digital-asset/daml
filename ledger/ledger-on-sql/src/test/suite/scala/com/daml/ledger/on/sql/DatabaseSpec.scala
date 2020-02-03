// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.daml.ledger.on.sql.Database.InvalidDatabaseException
import com.digitalasset.logging.LoggingContext.newLoggingContext
import org.scalatest.{AsyncWordSpec, Matchers}

class DatabaseSpec extends AsyncWordSpec with Matchers {
  "Database" should {
    "not accept unnamed H2 database URLs" in {
      newLoggingContext { implicit logCtx =>
        an[InvalidDatabaseException] should be thrownBy
          Database.owner("jdbc:h2:mem:")
      }
    }

    "not accept unnamed SQLite database URLs" in {
      newLoggingContext { implicit logCtx =>
        an[InvalidDatabaseException] should be thrownBy
          Database.owner("jdbc:sqlite::memory:")
      }
    }
  }
}
