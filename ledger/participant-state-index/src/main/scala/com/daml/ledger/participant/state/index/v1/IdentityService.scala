// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1

import com.daml.ledger.participant.state.v1.LedgerId

import scala.concurrent.Future

/**
  * Serves as a backend to implement
  * [[com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService]]
  **/
trait IdentityService {
  def getLedgerId(): Future[LedgerId]
}
