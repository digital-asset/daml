// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.sql.Connection

import com.daml.ledger.on.sql.Index
import com.daml.ledger.participant.state.v1.LedgerId
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

trait WriteQueries {
  def updateOrRetrieveLedgerId(
      providedLedgerId: LedgerId,
  )(implicit connection: Connection): LedgerId

  def insertIntoLog(key: Key, value: Value)(implicit connection: Connection): Index

  def updateState(stateUpdates: Seq[(Key, Value)])(implicit connection: Connection): Unit
}
