// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.on.sql

import com.daml.ledger.participant.state.v1
import com.daml.platform.configuration.{InvalidConfigException, LedgerConfiguration}
import com.daml.platform.sandbox.cli.Cli
import com.daml.platform.sandbox.config.{LedgerName, SandboxConfig}
import com.daml.platform.sandbox.{GlobalLogLevel, SandboxServer}
import com.daml.resources.ProgramResource

object Main {

  private val defaultConfig: SandboxConfig =
    SandboxConfig.defaultConfig.copy(
      name = LedgerName("DAML-on-SQL"),
      participantId = v1.ParticipantId.assertFromString("daml-on-sql-participant"),
      seeding = None,
      ledgerConfig = LedgerConfiguration.defaultLedgerBackedIndex,
    )

  def main(args: Array[String]): Unit = {
    new ProgramResource({
      val config = new Cli(defaultConfig).parse(args).getOrElse(sys.exit(1))
      if (config.jdbcUrl.isEmpty) {
        throw new InvalidConfigException("The JDBC URL is mandatory.")
      }
      if (config.jdbcUrl.exists(!_.startsWith("jdbc:postgresql://"))) {
        throw new InvalidConfigException(
          s"The JDBC URL, '${config.jdbcUrl.get}', is invalid. DAML-on-SQL only supports PostgreSQL.")
      }
      if (!config.implicitPartyAllocation) {
        throw new InvalidConfigException(
          "You cannot disable implicit party allocation in DAML-on-SQL.")
      }
      config.logLevel.foreach(GlobalLogLevel.set)
      SandboxServer.owner(config)
    }).run()
  }

}
