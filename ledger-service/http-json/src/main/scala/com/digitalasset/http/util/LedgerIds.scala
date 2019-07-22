// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}

object LedgerIds {
  def convertLedgerId(a: com.digitalasset.ledger.api.domain.LedgerId): lar.LedgerId =
    lar.LedgerId(com.digitalasset.ledger.api.domain.LedgerId.unwrap(a))
}
