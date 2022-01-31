// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import java.util.UUID

import com.daml.ledger.api.domain.LedgerId
import com.daml.platform.sandbox.config.LedgerName
import scalaz.syntax.tag._

private[sandbox] object LedgerIdGenerator {
  def generateRandomId(prefix: LedgerName): LedgerId =
    LedgerId(s"${prefix.unwrap.toLowerCase}-${UUID.randomUUID}")
}
