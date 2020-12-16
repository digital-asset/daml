// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.on.sql

import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.cli.{CommonCli, SandboxCli}
import com.daml.platform.sandbox.config.SandboxConfig
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
      .opt[Unit]("dev-mode-unsafe")
      .optional()
      .action((_, config) => config.copy(devMode = true))
      .text("Allows development versions of DAML-LF language and transaction format.")
      .hidden()

    parser
      .opt[String]("sql-backend-jdbcurl-env")
      .optional()
      .text("The environment variable containing JDBC connection URL to a Postgres database, " +
        s"including username and password. If present, $Name will use the database to persist its data.")
      .action((env, config) =>
        config.copy(jdbcUrl = getEnv(env).orElse(
          throw new IllegalArgumentException(s"The '$env' environment variable is undefined."))))

    parser
      .opt[String]("sql-backend-jdbcurl")
      .optional()
      .text(
        s"The JDBC connection URL to a Postgres database containing the username and password as well. If present, $Name will use the database to persist its data.")
      .action((url, config) => config.copy(jdbcUrl = Some(url)))

    // Ideally we would set the relevant options to `required()`, but it doesn't seem to work.
    // Even when the value is provided, it still reports that it's missing. Instead, we check the
    // configuration afterwards.
    parser.checkConfig(
      config =>
        if (config.ledgerIdMode == LedgerIdMode.dynamic)
          Left("The ledger ID is required. Please set it with `--ledgerid`.")
        else
          Right(()))
    parser.checkConfig(
      config =>
        if (config.jdbcUrl.isEmpty)
          Left(
            "The JDBC URL is required. Please set it with `--sql-backend-jdbcurl` or `--sql-backend-jdbcurl-env`.")
        else
          Right(()))
    parser.checkConfig(
      config =>
        if (config.jdbcUrl.exists(!_.startsWith("jdbc:postgresql://")))
          Left(
            s"The JDBC URL, '${config.jdbcUrl.get}', is invalid. $Name only supports PostgreSQL.")
        else
          Right(()))
    parser
  }

}
