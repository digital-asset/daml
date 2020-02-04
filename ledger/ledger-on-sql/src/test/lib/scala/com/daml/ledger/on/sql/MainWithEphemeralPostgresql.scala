// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import akka.stream.Materializer
import com.daml.ledger.on.sql.Main.{ExtraConfig, SqlLedgerFactory}
import com.daml.ledger.participant.state.kvutils.app.{Config, LedgerFactory, Runner}
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.digitalasset.resources.{ProgramResource, ResourceOwner}
import com.digitalasset.testing.postgresql.PostgresAround
import scopt.OptionParser

import scala.concurrent.ExecutionContext.Implicits.global

object MainWithEphemeralPostgresql extends App with PostgresAround {
  startEphemeralPostgres()
  sys.addShutdownHook(stopAndCleanUpPostgres())

  new ProgramResource(Runner("SQL Ledger", PostgresqlLedgerFactory).owner(args)).run()

  object PostgresqlLedgerFactory extends LedgerFactory[SqlLedgerReaderWriter, Unit] {
    override val defaultExtraConfig: Unit = ()

    override def extraConfigParser(parser: OptionParser[Config[Unit]]): Unit = ()

    override def owner(
        ledgerId: LedgerId,
        participantId: ParticipantId,
        config: Unit,
    )(implicit materializer: Materializer): ResourceOwner[SqlLedgerReaderWriter] = {
      SqlLedgerFactory.owner(
        ledgerId,
        participantId,
        ExtraConfig(jdbcUrl = Some(postgresFixture.jdbcUrl)),
      )
    }
  }
}
