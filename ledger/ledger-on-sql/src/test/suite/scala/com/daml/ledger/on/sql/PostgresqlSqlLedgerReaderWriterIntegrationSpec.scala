// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.sql.DriverManager
import java.time.Clock

import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase.ParticipantState
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.resources.ResourceOwner
import com.digitalasset.testing.postgresql.PostgresAroundAll

import scala.concurrent.ExecutionContext

class PostgresqlSqlLedgerReaderWriterIntegrationSpec
    extends ParticipantStateIntegrationSpecBase("SQL implementation using PostgreSQL")
    with PostgresAroundAll {
  private implicit val ec: ExecutionContext = ExecutionContext.global

  override val startIndex: Long = SqlLedgerReaderWriter.StartIndex

  override def participantStateFactory(
      participantId: ParticipantId,
      ledgerId: LedgerString,
  ): ResourceOwner[ParticipantState] = {
    newLoggingContext { implicit logCtx =>
      SqlLedgerReaderWriter
        .owner(ledgerId, participantId, postgresFixture.jdbcUrl)
        .map(readerWriter => new KeyValueParticipantState(readerWriter, readerWriter))
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val connection = DriverManager.getConnection(postgresFixture.jdbcUrl)
    try {
      connection.prepareStatement("TRUNCATE log RESTART IDENTITY").execute()
      connection.prepareStatement("TRUNCATE state RESTART IDENTITY").execute()
      ()
    } finally {
      connection.close()
    }
  }

  override def currentRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())
}
