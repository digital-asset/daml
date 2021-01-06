// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import com.daml.ledger.on.sql.Index
import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.participant.state.v1.LedgerId

import scala.util.Try

trait WriteQueries {
  def updateOrRetrieveLedgerId(providedLedgerId: LedgerId): Try[LedgerId]

  def insertRecordIntoLog(key: Raw.Key, value: Raw.Value): Try[Index]

  def updateState(stateUpdates: Iterable[Raw.Pair]): Try[Unit]

  def truncate(): Try[Unit]

}
