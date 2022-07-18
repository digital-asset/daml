// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.testing.postgresql.PostgresAround

object MainWithEphemeralPostgresql extends PostgresAround {
  def main(args: Array[String]): Unit = {
    connectToPostgresqlServer()
    val database = createNewRandomDatabase()
    sys.addShutdownHook(disconnectFromPostgresqlServer())
    System.setProperty("DEFAULT_PARTICIPANT_DATABASE_JDBC_URL", database.url)
    CliSandboxOnXRunner.run(
      args,
      manipulateConfig = originalConfig =>
        originalConfig.copy(
          participants = originalConfig.participants.map(p =>
            p.copy(
              serverJdbcUrl = database.url
            )
          )
        ),
    )
  }
}
