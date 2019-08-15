// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.persistence

import com.digitalasset.platform.sandbox.SandboxMain

object EphemeralPostgresSandboxMain extends App with PostgresAround {
  val fixture = startEphemeralPg()
  sys.addShutdownHook(stopAndCleanUp(fixture.tempDir, fixture.dataDir, fixture.logFile))
  SandboxMain.main(args ++ List("--sql-backend-jdbcurl", fixture.jdbcUrl))
}
