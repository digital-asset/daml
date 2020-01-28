// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

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

abstract class SqlLedgerReaderWriterIntegrationSpecBase(implementationName: String)
    extends ParticipantStateIntegrationSpecBase(implementationName) {
  protected final implicit val ec: ExecutionContext = ExecutionContext.global

  protected def jdbcUrl: String

  protected def databaseIsReused: Boolean = false

  override final val startIndex: Long = SqlLedgerReaderWriter.StartIndex

  override final def participantStateFactory(
      participantId: ParticipantId,
      ledgerId: LedgerString,
  ): ResourceOwner[ParticipantState] = {
    val currentJdbcUrl = jdbcUrl
    newLoggingContext { implicit logCtx =>
      for {
        _ <- if (databaseIsReused)
          Database.owner(currentJdbcUrl).map(_.clear()).map(_ => ())
        else
          ResourceOwner.successful(())
        readerWriter <- SqlLedgerReaderWriter.owner(ledgerId, participantId, currentJdbcUrl)
      } yield new KeyValueParticipantState(readerWriter, readerWriter)
    }
  }

  override final def currentRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())
}
