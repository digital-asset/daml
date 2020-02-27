// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.time.Instant

import com.daml.ledger.on.sql.Index
import com.daml.ledger.participant.state.v1.LedgerId
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

trait WriteQueries {
  def updateOrRetrieveLedgerId(providedLedgerId: LedgerId): LedgerId

  def insertRecordIntoLog(key: Key, value: Value): Index

  def insertHeartbeatIntoLog(timestamp: Instant): Index

  def updateState(stateUpdates: Seq[(Key, Value)]): Unit
}
