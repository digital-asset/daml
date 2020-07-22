// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.on.sql

import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.configuration.{InvalidConfigException, LedgerConfiguration}
import com.daml.platform.sandbox.cli.Cli
import com.daml.platform.sandbox.config.{LedgerName, SandboxConfig}
import com.daml.platform.sandbox.{GlobalLogLevel, SandboxServer}
import com.daml.resources.ProgramResource
import scalaz.syntax.tag._

object Main {

  private val Name = LedgerName("DAML-on-SQL")

  private[sql] val defaultConfig: SandboxConfig =
    SandboxConfig.defaultConfig.copy(
      participantId = v1.ParticipantId.assertFromString(Name.unwrap.toLowerCase()),
      seeding = Some(Seeding.Strong),
      ledgerConfig = LedgerConfiguration.defaultLedgerBackedIndex,
    )

  def main(args: Array[String]): Unit = {
    val config = new Cli(defaultConfig).parse(args).getOrElse(sys.exit(1))
    run(config)
  }

  private[sql] def run(config: SandboxConfig): Unit = {
    new ProgramResource({
      if (config.ledgerIdMode == LedgerIdMode.dynamic) {
        throw new InvalidConfigException(
          "The ledger ID is mandatory. Please set it with `--ledgerid`.")
      }
      if (config.jdbcUrl.isEmpty) {
        throw new InvalidConfigException(
          "The JDBC URL is mandatory. Please set it with `--sql-backend-jdbcurl`.")
      }
      if (config.jdbcUrl.exists(!_.startsWith("jdbc:postgresql://"))) {
        throw new InvalidConfigException(
          s"The JDBC URL, '${config.jdbcUrl.get}', is invalid. $Name only supports PostgreSQL.")
      }
      if (!config.implicitPartyAllocation) {
        throw new InvalidConfigException(s"You cannot disable implicit party allocation in $Name.")
      }
      config.logLevel.foreach(GlobalLogLevel.set)
      SandboxServer.owner(Name, config)
    }).run()
  }
}
