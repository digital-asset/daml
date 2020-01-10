// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.time.Clock

import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase
import com.daml.ledger.participant.state.kvutils.api.impl.InMemoryLedgerReaderWriter
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.daml.lf.data.Time.Timestamp

class InMemoryLedgerReaderWriterIntegrationSpec
    extends ParticipantStateIntegrationSpecBase(
      "In-memory participant state via simplified API implementation") {

  override def participantStateFactory(
      participantId: ParticipantId,
      ledgerId: LedgerString): ReadService with WriteService with AutoCloseable = {
    val readerWriter = new InMemoryLedgerReaderWriter(ledgerId, participantId)
    new KeyValueParticipantState(readerWriter, readerWriter)
  }

  override def currentRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())
}
