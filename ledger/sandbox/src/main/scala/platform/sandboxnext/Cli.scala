// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext

import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.sandbox.cli.{CommonCli, SandboxCli}
import com.daml.platform.sandbox.config.SandboxConfig
import scopt.OptionParser

private[sandboxnext] object Cli extends SandboxCli {

  override def defaultConfig: SandboxConfig = SandboxConfig.defaultConfig

  override protected val parser: OptionParser[SandboxConfig] = {
    val parser = new CommonCli(Name).withEarlyAccess.withDevEngine
      .withContractIdSeeding(
        defaultConfig,
        Some(Seeding.Strong),
        Some(Seeding.Weak),
        Some(Seeding.Static),
      )
      .parser
    parser
      .opt[Boolean](name = "implicit-party-allocation")
      .optional()
      .action((x, c) => c.copy(implicitPartyAllocation = x))
      .text(
        s"When referring to a party that doesn't yet exist on the ledger, $Name will implicitly allocate that party."
          + s" You can optionally disable this behavior to bring $Name into line with other ledgers."
      )
    parser
      .opt[String]("sql-backend-jdbcurl")
      .optional()
      .text(
        s"Deprecated: Use the Daml Driver for PostgreSQL if you need persistence.\nThe JDBC connection URL to a Postgres database containing the username and password as well. If present, $Name will use the database to persist its data."
      )
      .action((url, config) => config.copy(jdbcUrl = Some(url)))

    parser
      .opt[Int]("database-connection-pool-size")
      .optional()
      .text(
        s"The number of connections in the database connection pool. Defaults to ${SandboxConfig.DefaultDatabaseConnectionPoolSize}."
      )
      .action((poolSize, config) => config.copy(databaseConnectionPoolSize = poolSize))

    parser
  }

}
