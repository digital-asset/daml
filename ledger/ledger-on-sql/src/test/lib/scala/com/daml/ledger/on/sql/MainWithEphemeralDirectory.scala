// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.nio.file.Files

import akka.stream.Materializer
import com.daml.ledger.on.sql.Main.{ExtraConfig, SqlLedgerFactory}
import com.daml.ledger.participant.state.kvutils.app.{Config, KeyValueLedger, LedgerFactory, Runner}
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import scopt.OptionParser

object MainWithEphemeralDirectory extends App {
  val DirectoryPattern = "%DIR"

  val directory = Files.createTempDirectory("ledger-on-sql-ephemeral-")

  Runner("SQL Ledger", TestLedgerFactory).run(args)

  object TestLedgerFactory extends LedgerFactory[ExtraConfig] {
    override val defaultExtraConfig: ExtraConfig = SqlLedgerFactory.defaultExtraConfig

    override def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit =
      SqlLedgerFactory.extraConfigParser(parser)

    override def apply(ledgerId: LedgerId, participantId: ParticipantId, config: ExtraConfig)(
        implicit materializer: Materializer,
    ): KeyValueLedger = {
      val jdbcUrl = config.jdbcUrl.map(_.replace(DirectoryPattern, directory.toString))
      SqlLedgerFactory(ledgerId, participantId, config.copy(jdbcUrl = jdbcUrl))
    }
  }
}
