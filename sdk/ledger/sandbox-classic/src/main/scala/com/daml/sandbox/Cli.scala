// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.sandbox

import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.sandbox.cli.CommonCli
import com.daml.platform.sandbox.config.{LedgerName, SandboxConfig}
import scopt.OptionParser

import java.time.Duration

object Cli {
  private[sandbox] val Name = LedgerName("Sandbox")

  val defaultConfig: SandboxConfig = SandboxConfig.defaultConfig.copy(
    delayBeforeSubmittingLedgerConfiguration = Duration.ZERO
  )

  protected val parser: OptionParser[SandboxConfig] = {
    val parser = new CommonCli(Name).withEarlyAccess.withDevEngine
      .withContractIdSeeding(
        defaultConfig,
        Seeding.Strong,
        Seeding.Weak,
        Seeding.Static,
      )
      .parser
    parser
      .opt[String]("sql-backend-jdbcurl")
      .optional()
      .text(
        s"Deprecated: Use the Daml Driver for PostgreSQL if you need persistence.\nThe JDBC connection URL to a Postgres database containing the username and password as well. If present, $Name will use the database to persist its data."
      )
      .action((url, config) => config.copy(jdbcUrl = Some(url)))
    parser
      .opt[Int]("max-parallel-submissions")
      .optional()
      .action((value, config) => config.copy(maxParallelSubmissions = value))
      .text(
        s"Maximum number of successfully interpreted commands waiting to be sequenced. The threshold is shared across all parties. Overflowing it will cause back-pressure, signaled by a `RESOURCE_EXHAUSTED` error code. Default is ${defaultConfig.maxParallelSubmissions}."
      )
    parser
  }

  def parse(args: Array[String]): Option[SandboxConfig] =
    parser.parse(args, defaultConfig)

}
