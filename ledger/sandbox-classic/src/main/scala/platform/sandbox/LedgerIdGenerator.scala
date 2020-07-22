// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import java.util.UUID

import com.daml.ledger.api.domain.LedgerId
import com.daml.platform.sandbox.config.LedgerName
import scalaz.syntax.tag._

class LedgerIdGenerator(name: LedgerName) {
  def generateRandomId(): LedgerId =
    LedgerId(s"${name.unwrap.toLowerCase()}-${UUID.randomUUID().toString}")
}
