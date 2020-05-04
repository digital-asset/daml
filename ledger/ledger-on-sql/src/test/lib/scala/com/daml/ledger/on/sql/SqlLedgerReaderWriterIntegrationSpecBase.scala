// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase.ParticipantState
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId, SeedService}
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.resources.ResourceOwner

abstract class SqlLedgerReaderWriterIntegrationSpecBase(implementationName: String)
    extends ParticipantStateIntegrationSpecBase(implementationName) {
  protected def jdbcUrl(id: String): String

  override protected final val startIndex: Long = StartIndex

  override protected final def participantStateFactory(
      ledgerId: Option[LedgerId],
      participantId: ParticipantId,
      testId: String,
      metrics: Metrics,
  )(implicit logCtx: LoggingContext): ResourceOwner[ParticipantState] =
    new SqlLedgerReaderWriter.Owner(
      ledgerId,
      participantId,
      metrics,
      engine = Engine(),
      jdbcUrl(testId),
      resetOnStartup = false,
      // Using a weak random source to avoid slowdown during tests.
      seedService = SeedService(Seeding.Weak),
    ).map(readerWriter => new KeyValueParticipantState(readerWriter, readerWriter, metrics))
}
