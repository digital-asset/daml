// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.reference.v2

import com.digitalasset.platform.sandbox.persistence.PostgresAround

object EphemeralPostgresReferenceServerMain extends App with PostgresAround {
  startEphemeralPostgres()
  sys.addShutdownHook(stopAndCleanUpPostgres())
  ReferenceServer.main(args ++ List("--jdbc-url", postgresFixture.jdbcUrl))
}
