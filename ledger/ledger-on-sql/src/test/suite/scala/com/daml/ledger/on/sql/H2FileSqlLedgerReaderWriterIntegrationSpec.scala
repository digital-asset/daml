// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.nio.file.{Files, Path}
import java.time.Clock

import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase.ParticipantState
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.resources.ResourceOwner

import scala.concurrent.ExecutionContext

class H2FileSqlLedgerReaderWriterIntegrationSpec
    extends ParticipantStateIntegrationSpecBase("SQL implementation using H2 with a file") {
  private implicit val ec: ExecutionContext = ExecutionContext.global

  override val startIndex: Long = SqlLedgerReaderWriter.StartIndex

  private var directory: Path = _

  override def beforeEach(): Unit = {
    directory = Files.createTempDirectory(getClass.getSimpleName)
    super.beforeEach()
  }

  override def participantStateFactory(
      participantId: ParticipantId,
      ledgerId: LedgerString,
  ): ResourceOwner[ParticipantState] = {
    val jdbcUrl = s"jdbc:h2:file:$directory/test"
    newLoggingContext { implicit logCtx =>
      SqlLedgerReaderWriter
        .owner(ledgerId, participantId, jdbcUrl)
        .map(readerWriter => new KeyValueParticipantState(readerWriter, readerWriter))
    }
  }

  override def currentRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())
}
