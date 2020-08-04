// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.participant.state.kvutils.DamlStateMap
import com.daml.ledger.participant.state.kvutils.TestHelpers.mkParticipantId
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.lf.data.Time.Timestamp

class FakeCommitContext(
    recordTime: Option[Timestamp],
    override val inputs: DamlStateMap = Map.empty,
    participantId: Int = 0,
    entryId: Int = 0)
    extends CommitContext {
  override def getRecordTime: Option[Timestamp] = recordTime

  override def getParticipantId: ParticipantId = mkParticipantId(participantId)
}
