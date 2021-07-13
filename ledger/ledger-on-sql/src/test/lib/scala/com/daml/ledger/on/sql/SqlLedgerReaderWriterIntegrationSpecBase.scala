// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase.ParticipantState
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.daml.ledger.resources.ResourceOwner
import com.daml.ledger.validator.LogEntryIdAllocator
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics

abstract class SqlLedgerReaderWriterIntegrationSpecBase(implementationName: String)
    extends ParticipantStateIntegrationSpecBase(implementationName) {
  protected def jdbcUrl(id: String): String

  override protected final val startIndex: Long = StartIndex

  override protected final def participantStateFactory(
      ledgerId: LedgerId,
      participantId: ParticipantId,
      testId: String,
      metrics: Metrics,
  )(implicit loggingContext: LoggingContext): ResourceOwner[ParticipantState] =
    new SqlLedgerReaderWriter.Owner(
      ledgerId = ledgerId,
      participantId = participantId,
      metrics = metrics,
      engine = Engine.DevEngine(),
      jdbcUrl = jdbcUrl(testId),
      resetOnStartup = false,
      logEntryIdAllocator = LogEntryIdAllocator.Random,
    ).map(readerWriter => new KeyValueParticipantState(readerWriter, readerWriter, metrics))
}
