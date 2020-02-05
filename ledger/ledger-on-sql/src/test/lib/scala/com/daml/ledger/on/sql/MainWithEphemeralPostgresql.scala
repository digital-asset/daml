// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import akka.stream.Materializer
import com.daml.ledger.on.sql.Main.{ExtraConfig, SqlLedgerFactory}
import com.daml.ledger.participant.state.kvutils.app.LedgerFactory.SimpleLedgerFactory
import com.daml.ledger.participant.state.kvutils.app.Runner
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.digitalasset.resources.{ProgramResource, ResourceOwner}
import com.digitalasset.testing.postgresql.PostgresAround

import scala.concurrent.ExecutionContext

object MainWithEphemeralPostgresql extends PostgresAround {
  def main(args: Array[String]): Unit = {
    startEphemeralPostgres()
    sys.addShutdownHook(stopAndCleanUpPostgres())
    new ProgramResource(new Runner("SQL Ledger", PostgresqlLedgerFactory).owner(args)).run()
  }

  object PostgresqlLedgerFactory extends SimpleLedgerFactory[SqlLedgerReaderWriter] {
    override def owner(
        ledgerId: LedgerId,
        participantId: ParticipantId,
        config: Unit,
    )(
        implicit executionContext: ExecutionContext,
        materializer: Materializer,
    ): ResourceOwner[SqlLedgerReaderWriter] = {
      SqlLedgerFactory.owner(
        ledgerId,
        participantId,
        ExtraConfig(jdbcUrl = Some(postgresFixture.jdbcUrl)),
      )
    }
  }
}
