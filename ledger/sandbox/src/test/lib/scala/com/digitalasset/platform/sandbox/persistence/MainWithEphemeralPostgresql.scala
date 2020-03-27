// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.persistence

import com.digitalasset.platform.sandbox.SandboxMain
import com.digitalasset.testing.postgresql.PostgresAround

object MainWithEphemeralPostgresql extends PostgresAround {
  def main(args: Array[String]): Unit = {
    startEphemeralPostgres()
    sys.addShutdownHook(stopAndCleanUpPostgres())
    SandboxMain.main(args ++ Array("--sql-backend-jdbcurl", postgresFixture.jdbcUrl))
  }
}
