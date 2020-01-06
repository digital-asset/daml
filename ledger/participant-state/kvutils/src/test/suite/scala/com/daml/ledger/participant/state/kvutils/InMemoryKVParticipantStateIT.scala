// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.Clock

import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.daml.lf.data.Time.Timestamp

class InMemoryKVParticipantStateIT extends ParticipantStateIntegrationSpecBase {

  override def participantStateFactory(
      participantId: ParticipantId,
      ledgerId: LedgerString): ReadService with WriteService with AutoCloseable =
    new InMemoryKVParticipantState(participantId, ledgerId)

  override def currentRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())
}
