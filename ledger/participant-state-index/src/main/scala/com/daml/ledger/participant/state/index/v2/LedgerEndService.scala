// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.digitalasset.ledger.api.domain.LedgerOffset

import scala.concurrent.Future

/**
  * Serves as a backend to implement ledger end related API calls.
  **/
trait LedgerEndService {
  def currentLedgerEnd(): Future[LedgerOffset.Absolute]
}
