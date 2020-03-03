// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.nio.file.Files

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.kvutils.app.{
  Config,
  LedgerFactory,
  ParticipantConfig,
  Runner
}
import com.digitalasset.logging.LoggingContext
import com.digitalasset.resources.{ProgramResource, ResourceOwner}
import scopt.OptionParser

import scala.concurrent.ExecutionContext

object MainWithEphemeralDirectory {
  private val DirectoryPattern = "%DIR"

  def main(args: Array[String]): Unit = {
    new ProgramResource(new Runner("SQL Ledger", TestLedgerFactory).owner(args)).run()
  }

  object TestLedgerFactory extends LedgerFactory[KeyValueParticipantState, ExtraConfig] {
    override val defaultExtraConfig: ExtraConfig = SqlLedgerFactory.defaultExtraConfig

    override def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit =
      SqlLedgerFactory.extraConfigParser(parser)

    override def manipulateConfig(config: Config[ExtraConfig]): Config[ExtraConfig] =
      SqlLedgerFactory.manipulateConfig(config)

    override def readWriteServiceOwner(
        config: Config[ExtraConfig],
        participantConfig: ParticipantConfig
    )(
        implicit executionContext: ExecutionContext,
        materializer: Materializer,
        logCtx: LoggingContext,
    ): ResourceOwner[KeyValueParticipantState] = {
      val directory = Files.createTempDirectory("ledger-on-sql-ephemeral-")
      val jdbcUrl = config.extra.jdbcUrl.map(_.replace(DirectoryPattern, directory.toString))
      SqlLedgerFactory.readWriteServiceOwner(
        config.copy(extra = config.extra.copy(jdbcUrl = jdbcUrl)),
        participantConfig)
    }
  }
}
