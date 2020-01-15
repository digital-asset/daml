// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.nio.file.Files

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.app.{Config, KeyValueLedger, LedgerFactory, Runner}
import com.daml.ledger.participant.state.v1.ParticipantId
import scopt.OptionParser

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

object MainWithEphemeralSqlite extends App {
  val databaseFile = Files.createTempFile("ledger-on-sql-ephemeral-sqlite", ".db")

  try {
    Runner("Ephemeral SQLite Ledger", SqliteLedgerConstructor).run(args)
  } finally {
    Files.delete(databaseFile)
  }

  object SqliteLedgerConstructor extends LedgerFactory[Unit] {
    override val defaultExtraConfig: Unit = ()

    override def extraConfigParser(parser: OptionParser[Config[Unit]]): Unit =
      ()

    override def apply(participantId: ParticipantId, config: Unit)(
        implicit materializer: Materializer,
    ): KeyValueLedger =
      Await.result(
        SqlLedgerReaderWriter(
          participantId = participantId,
          jdbcUrl = s"jdbc:sqlite:$databaseFile",
        ),
        10.seconds)
  }
}
