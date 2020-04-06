// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase.ParticipantState
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.daml.logging.LoggingContext
import com.daml.resources.ResourceOwner

class InMemoryLedgerReaderWriterIntegrationSpec
    extends ParticipantStateIntegrationSpecBase("In-memory ledger/participant") {

  override val isPersistent: Boolean = false

  override def participantStateFactory(
      ledgerId: Option[LedgerId],
      participantId: ParticipantId,
      testId: String,
      metricRegistry: MetricRegistry,
  )(implicit logCtx: LoggingContext): ResourceOwner[ParticipantState] =
    new InMemoryLedgerReaderWriter.SingleParticipantOwner(
      ledgerId,
      participantId,
      metricRegistry = metricRegistry,
    ).map(readerWriter => new KeyValueParticipantState(readerWriter, readerWriter, metricRegistry))
}
