// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index.v2

import com.digitalasset.canton.ledger.api.domain.LedgerId

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService]]
  */
trait IdentityProvider {
  def ledgerId: LedgerId
}
