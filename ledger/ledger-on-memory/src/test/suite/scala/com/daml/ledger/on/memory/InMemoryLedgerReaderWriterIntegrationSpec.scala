// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase.ParticipantState
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.digitalasset.logging.LoggingContext
import com.digitalasset.resources.ResourceOwner

class InMemoryLedgerReaderWriterIntegrationSpec
    extends ParticipantStateIntegrationSpecBase(
      "In-memory participant state via simplified API implementation") {

  override val isPersistent: Boolean = false

  override def participantStateFactory(
      ledgerId: Option[LedgerId],
      participantId: ParticipantId,
      testId: String,
  )(implicit logCtx: LoggingContext): ResourceOwner[ParticipantState] =
    InMemoryLedgerReaderWriter
      .singleParticipantOwner(ledgerId, participantId)
      .map(readerWriter => new KeyValueParticipantState(readerWriter, readerWriter))
}
