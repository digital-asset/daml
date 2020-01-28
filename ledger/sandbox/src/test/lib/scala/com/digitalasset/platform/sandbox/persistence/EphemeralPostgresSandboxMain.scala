// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.persistence

import com.digitalasset.platform.sandbox.SandboxMain
import com.digitalasset.testing.postgresql.PostgresAround

object EphemeralPostgresSandboxMain extends App with PostgresAround {
  startEphemeralPostgres()
  sys.addShutdownHook(stopAndCleanUpPostgres())
  SandboxMain.main(args ++ List("--sql-backend-jdbcurl", postgresFixture.jdbcUrl))
}
