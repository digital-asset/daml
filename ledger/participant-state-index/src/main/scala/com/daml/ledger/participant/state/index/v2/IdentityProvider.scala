// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.daml.ledger.api.domain.LedgerId
import scala.concurrent.Future

/**
  * Serves as a backend to implement
  * [[com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService]]
  **/
trait IdentityProvider {
  def getLedgerId(): Future[LedgerId]
}
