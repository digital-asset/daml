// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.daml.ledger.participant.state.kvutils.app.{Config, Runner}
import com.daml.resources.ProgramResource
import com.daml.testing.postgresql.PostgresAround

object MainWithEphemeralPostgresql extends PostgresAround {
  def main(args: Array[String]): Unit = {
    val originalConfig =
      Config
        .parse[Unit]("SQL Ledger", _ => (), (), args)
        .getOrElse(sys.exit(1))

    connectToPostgresqlServer()
    val database = createNewRandomDatabase()
    sys.addShutdownHook(disconnectFromPostgresqlServer())
    val config = originalConfig.copy(
      participants = originalConfig.participants.map(_.copy(serverJdbcUrl = database.url)),
      extra = ExtraConfig(jdbcUrl = Some(database.url)),
    )
    new ProgramResource(new Runner("SQL Ledger", SqlLedgerFactory).owner(config)).run()
  }
}
