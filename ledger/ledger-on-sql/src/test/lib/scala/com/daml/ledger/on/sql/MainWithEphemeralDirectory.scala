// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.nio.file.Files

import akka.stream.Materializer
import com.daml.ledger.on.sql.Main.{ExtraConfig, SqlLedgerFactory}
import com.daml.ledger.participant.state.kvutils.app.{Config, LedgerFactory, Runner}
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.digitalasset.resources.{ProgramResource, ResourceOwner}
import scopt.OptionParser

import scala.concurrent.ExecutionContext.Implicits.global

object MainWithEphemeralDirectory extends App {
  val DirectoryPattern = "%DIR"

  val directory = Files.createTempDirectory("ledger-on-sql-ephemeral-")

  new ProgramResource(Runner("SQL Ledger", TestLedgerFactory).owner(args)).run()

  object TestLedgerFactory extends LedgerFactory[SqlLedgerReaderWriter, ExtraConfig] {
    override val defaultExtraConfig: ExtraConfig = SqlLedgerFactory.defaultExtraConfig

    override def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit =
      SqlLedgerFactory.extraConfigParser(parser)

    override def owner(
        ledgerId: LedgerId,
        participantId: ParticipantId,
        config: ExtraConfig,
    )(implicit materializer: Materializer): ResourceOwner[SqlLedgerReaderWriter] = {
      val jdbcUrl = config.jdbcUrl.map(_.replace(DirectoryPattern, directory.toString))
      SqlLedgerFactory.owner(ledgerId, participantId, config.copy(jdbcUrl = jdbcUrl))
    }
  }
}
