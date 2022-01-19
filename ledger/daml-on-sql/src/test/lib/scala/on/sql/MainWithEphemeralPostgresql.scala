// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.on.sql

import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.testing.postgresql.PostgresAround

object MainWithEphemeralPostgresql extends PostgresAround {

  def main(args: Array[String]): Unit = {
    connectToPostgresqlServer()
    val database = createNewRandomDatabase()
    sys.addShutdownHook(disconnectFromPostgresqlServer())
    val defaultConfig: SandboxConfig =
      DefaultConfig.copy(
        seeding = Seeding.Weak,
        jdbcUrl = Some(database.url),
      )
    val config = new Cli(defaultConfig).parse(args).getOrElse(sys.exit(1))
    Main.run(config)
  }

}
