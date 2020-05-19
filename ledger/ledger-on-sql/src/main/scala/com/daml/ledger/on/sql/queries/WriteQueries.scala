// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import com.daml.ledger.on.sql.Index
import com.daml.ledger.participant.state.v1.LedgerId
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

import scala.util.Try

trait WriteQueries {
  def updateOrRetrieveLedgerId(providedLedgerId: LedgerId): Try[LedgerId]

  def insertRecordIntoLog(key: Key, value: Value): Try[Index]

  def updateState(stateUpdates: Seq[(Key, Value)]): Try[Unit]

  def truncate(): Try[Unit]

}
