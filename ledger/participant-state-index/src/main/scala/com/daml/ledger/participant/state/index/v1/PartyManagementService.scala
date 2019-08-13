// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1

import scala.concurrent.Future
import com.daml.ledger.participant.state.v1.ParticipantId
// TODO: This should go to participant-state.vX
import com.digitalasset.ledger.api.domain.PartyDetails

/**
  * Serves as a backend to implement
  * [[com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc]]
  */
trait PartyManagementService {
  def getParticipantId: Future[ParticipantId]

  def listParties: Future[List[PartyDetails]]
}
