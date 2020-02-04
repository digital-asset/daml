// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.reference.v2

import com.digitalasset.testing.postgresql.PostgresAround

object EphemeralPostgresReferenceServerMain extends PostgresAround {
  def main(args: Array[String]): Unit = {
    startEphemeralPostgres()
    sys.addShutdownHook(stopAndCleanUpPostgres())
    ReferenceServer.main(args ++ Array("--jdbc-url", postgresFixture.jdbcUrl))
  }
}
