// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.reference.v2

import com.digitalasset.platform.sandbox.persistence.PostgresAround

object EphemeralPostgresReferenceServerMain extends App with PostgresAround {
  val fixture = startEphemeralPg()
  sys.addShutdownHook(stopAndCleanUp(fixture.tempDir, fixture.dataDir, fixture.logFile))
  ReferenceServer.main(args ++ List("--jdbc-url", fixture.jdbcUrl))
}
