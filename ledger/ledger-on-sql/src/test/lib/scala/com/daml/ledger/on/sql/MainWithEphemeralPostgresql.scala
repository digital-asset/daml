// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.daml.ledger.participant.state.kvutils.app.{Config, Runner}
import com.digitalasset.resources.ProgramResource
import com.digitalasset.testing.postgresql.PostgresAround

object MainWithEphemeralPostgresql extends PostgresAround {
  def main(args: Array[String]): Unit = {
    val originalConfig =
      Config
        .parse[Unit]("SQL Ledger", _ => (), (), args)
        .getOrElse(sys.exit(1))

    startEphemeralPostgres()
    sys.addShutdownHook(stopAndCleanUpPostgres())
    val config = originalConfig.copy(
      serverJdbcUrl = postgresFixture.jdbcUrl,
      extra = ExtraConfig(jdbcUrl = Some(postgresFixture.jdbcUrl)),
    )
    new ProgramResource(new Runner("SQL Ledger", SqlLedgerFactory).owner(config)).run()
  }
}
