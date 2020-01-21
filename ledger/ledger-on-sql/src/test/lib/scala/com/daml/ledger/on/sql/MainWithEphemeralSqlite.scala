// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.nio.file.Files

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.app.{Config, KeyValueLedger, LedgerFactory, Runner}
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.digitalasset.logging.LoggingContext.newLoggingContext
import scopt.OptionParser

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

object MainWithEphemeralSqlite extends App {
  val databaseFile = Files.createTempFile("ledger-on-sql-ephemeral-sqlite", ".db")
  val jdbcUrl = s"jdbc:sqlite:$databaseFile"

  try {
    Runner("Ephemeral SQLite Ledger", SqliteLedgerFactory).run(args)
  } finally {
    Files.delete(databaseFile)
  }

  object SqliteLedgerFactory extends LedgerFactory[Unit] {
    override val defaultExtraConfig: Unit = ()

    override def extraConfigParser(parser: OptionParser[Config[Unit]]): Unit =
      ()

    override def apply(ledgerId: LedgerId, participantId: ParticipantId, config: Unit)(
        implicit materializer: Materializer,
    ): KeyValueLedger = {
      Await.result(
        newLoggingContext { implicit loggingContext =>
          SqlLedgerReaderWriter(
            ledgerId = ledgerId,
            participantId = participantId,
            jdbcUrl = jdbcUrl,
          )
        },
        10.seconds,
      )
    }
  }
}
