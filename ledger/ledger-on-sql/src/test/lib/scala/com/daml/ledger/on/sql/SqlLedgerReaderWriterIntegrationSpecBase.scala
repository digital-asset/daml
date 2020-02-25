// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase.ParticipantState
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.digitalasset.logging.LoggingContext
import com.digitalasset.resources.ResourceOwner

import scala.concurrent.ExecutionContext

abstract class SqlLedgerReaderWriterIntegrationSpecBase(implementationName: String)
    extends ParticipantStateIntegrationSpecBase(implementationName) {
  protected final implicit val ec: ExecutionContext = ExecutionContext.global

  protected def jdbcUrl(id: String): String

  override final val startIndex: Long = StartIndex

  override final def participantStateFactory(
      ledgerId: Option[LedgerId],
      participantId: ParticipantId,
      testId: String,
  )(implicit logCtx: LoggingContext): ResourceOwner[ParticipantState] =
    SqlLedgerReaderWriter
      .owner(ledgerId, participantId, jdbcUrl(testId))
      .map(readerWriter => new KeyValueParticipantState(readerWriter, readerWriter))
}
