// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.on.sql

import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.testing.postgresql.PostgresAround

object MainWithEphemeralPostgresql extends PostgresAround {

  private val defaultConfig: SandboxConfig =
    DefaultConfig.copy(
      seeding = Some(Seeding.Weak),
    )

  def main(args: Array[String]): Unit = {
    connectToPostgresqlServer()
    val database = createNewRandomDatabase()
    sys.addShutdownHook(disconnectFromPostgresqlServer())
    val config =
      new Cli(defaultConfig)
        .parse(args)
        .getOrElse(sys.exit(1))
        .copy(jdbcUrl = Some(database.url))
    Main.run(config)
  }

}
