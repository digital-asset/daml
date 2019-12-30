// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import java.time.Clock

import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.daml.lf.data.Time.Timestamp

class InMemoryLedgerReaderWriterIntegrationSpec
    extends ParticipantStateIntegrationSpecBase("in-memory") {

  override def participantStateFactory(
      participantId: ParticipantId,
      ledgerId: LedgerString): ReadService with WriteService = {
    val readerWriter = new InMemoryLedgerReaderWriter(ledgerId, participantId)
    new KeyValueParticipantState(readerWriter, readerWriter)
  }

  override def currentRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())
}
