// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandboxnext.persistence

import com.digitalasset.platform.sandboxnext.Main
import com.digitalasset.testing.postgresql.PostgresAround

object MainWithEphemeralPostgresql extends PostgresAround {
  def main(args: Array[String]): Unit = {
    startEphemeralPostgres()
    sys.addShutdownHook(stopAndCleanUpPostgres())
    Main.main(args ++ Array("--sql-backend-jdbcurl", postgresFixture.jdbcUrl))
  }
}
