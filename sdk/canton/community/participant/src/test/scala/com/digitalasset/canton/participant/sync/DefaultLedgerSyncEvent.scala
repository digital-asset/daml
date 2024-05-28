// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.{LedgerParticipantId, LfPartyId}

object DefaultLedgerSyncEvent {
  def dummyStateUpdate(
      timestamp: CantonTimestamp = CantonTimestamp.Epoch
  ): LedgerSyncEvent =
    LedgerSyncEvent.PartyAddedToParticipant(
      party = LfPartyId.assertFromString("someparty"),
      displayName = "someparty",
      participantId = LedgerParticipantId.assertFromString("someparticipant"),
      recordTime = timestamp.toLf,
      submissionId = None,
    )
}
