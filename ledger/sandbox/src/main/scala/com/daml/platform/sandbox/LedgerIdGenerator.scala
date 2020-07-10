// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import java.util.UUID

import com.daml.ledger.api.domain.LedgerId

object LedgerIdGenerator {
  def generateRandomId(): LedgerId =
    LedgerId(s"sandbox-${UUID.randomUUID().toString}")
}
