// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.digitalasset.canton.ledger.api.domain.ParticipantOffset

import scala.concurrent.Future

/** Serves as a backend to implement ledger end related API calls.
  */
trait LedgerEndService {
  def currentLedgerEnd(): Future[ParticipantOffset.Absolute]
}
