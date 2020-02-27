// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.time.Instant

import com.daml.ledger.on.sql.Index
import com.daml.ledger.participant.state.v1.LedgerId
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

import scala.util.Try

trait WriteQueries {
  def updateOrRetrieveLedgerId(providedLedgerId: LedgerId): Try[LedgerId]

  def insertRecordIntoLog(key: Key, value: Value): Try[Index]

  def insertHeartbeatIntoLog(timestamp: Instant): Try[Index]

  def updateState(stateUpdates: Seq[(Key, Value)]): Try[Unit]
}
