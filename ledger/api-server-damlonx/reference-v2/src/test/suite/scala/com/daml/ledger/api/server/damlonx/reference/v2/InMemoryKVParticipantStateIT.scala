// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.reference.v2

import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase.ParticipantState
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.resources.ResourceOwner

class InMemoryKVParticipantStateIT
    extends ParticipantStateIntegrationSpecBase("In-memory participant state for Reference v2") {

  override val isPersistent: Boolean = false

  override def participantStateFactory(
      participantId: ParticipantId,
      ledgerId: LedgerString,
  ): ResourceOwner[ParticipantState] =
    ResourceOwner.forCloseable(() => new InMemoryKVParticipantState(participantId, ledgerId))
}
