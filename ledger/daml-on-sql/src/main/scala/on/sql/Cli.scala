// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.on.sql

import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.cli.{CommonCli, SandboxCli}
import com.daml.platform.sandbox.config.{PostgresStartupMode, SandboxConfig}
import scopt.OptionParser

private[sql] final class Cli(
    override val defaultConfig: SandboxConfig = DefaultConfig,
    getEnv: String => Option[String] = sys.env.get,
) extends SandboxCli {

  override protected val parser: OptionParser[SandboxConfig] = {
    val parser =
      new CommonCli(Name)
        .withContractIdSeeding(defaultConfig, Some(Seeding.Strong), Some(Seeding.Weak))
        .parser

    parser
      .opt[Unit]("eager-package-loading")
      .optional()
      .text(
        "Whether to load all the packages in the .dar files provided eagerly, rather than when needed as the commands come."
      )
      .action((_, config) => config.copy(eagerPackageLoading = true))

    parser
      .opt[String]("sql-backend-jdbcurl-env")
      .optional()
      .text(
        "The environment variable containing JDBC connection URL to a Postgres database, " +
          s"including username and password. If present, $Name will use the database to persist its data."
      )
      .action((env, config) =>
        config.copy(jdbcUrl =
          getEnv(env).orElse(
            throw new IllegalArgumentException(s"The '$env' environment variable is undefined.")
          )
        )
      )

    parser
      .opt[String]("sql-backend-jdbcurl")
      .optional()
      .text(
        s"The JDBC connection URL to a Postgres database containing the username and password as well. If present, $Name will use the database to persist its data."
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
      .opt[String]("sql-start-mode")
      .optional()
      .text(
        s"The mode under which the schema SQL script will be run. Possible values are \n\n" +
          s"${PostgresStartupMode.MigrateOnly} - Run schema migration scripts and exit \n" +
          s"${PostgresStartupMode.MigrateAndStart} - Run schema migration scripts and start service.  If the database schema already exists this will validate and start \n" +
          s"Defaults to ${DefaultConfig.sqlStartMode.get} if not specified"
      )
      .action((mode, config) => config.copy(sqlStartMode = PostgresStartupMode.fromString(mode)))

    parser
      .opt[Int]("max-parallel-submissions")
      .optional()
      .action((value, config) => config.copy(maxParallelSubmissions = value))
      .text(
        s"Maximum number of successfully interpreted commands waiting to be sequenced. The threshold is shared across all parties. Overflowing it will cause back-pressure, signaled by a `RESOURCE_EXHAUSTED` error code. Default is ${defaultConfig.maxParallelSubmissions}."
      )

    // Ideally we would set the relevant options to `required()`, but it doesn't seem to work.
    // Even when the value is provided, it still reports that it's missing. Instead, we check the
    // configuration afterwards.
    parser.checkConfig(config =>
      if (config.ledgerIdMode == LedgerIdMode.dynamic)
        Left("The ledger ID is required. Please set it with `--ledgerid`.")
      else
        Right(())
    )
    parser.checkConfig(config =>
      if (config.jdbcUrl.isEmpty)
        Left(
          "The JDBC URL is required. Please set it with `--sql-backend-jdbcurl` or `--sql-backend-jdbcurl-env`."
        )
      else
        Right(())
    )
    parser.checkConfig(config =>
      if (config.jdbcUrl.exists(!_.startsWith("jdbc:postgresql://")))
        Left(s"The JDBC URL is invalid. $Name only supports PostgreSQL.")
      else
        Right(())
    )
    parser.checkConfig(config =>
      if (
        config.sqlStartMode
          .map(startMode => PostgresStartupMode.fromString(startMode.toString))
          .isEmpty
      )
        Left(
          s"The sql-startup-mode specified is invalid.  " +
            s"Possible values are ${PostgresStartupMode.MigrateOnly} or ${PostgresStartupMode.MigrateAndStart}"
        )
      else
        Right(())
    )
    parser
  }

}
