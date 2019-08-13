// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.util.UUID

import com.digitalasset.ledger.api.domain.LedgerId

object LedgerIdGenerator {
  def generateRandomId(): LedgerId =
    LedgerId(s"sandbox-${UUID.randomUUID().toString}")
}
