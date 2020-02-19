// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.sql.Connection

import com.daml.ledger.on.sql.Index
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

import scala.collection.immutable

trait ReadQueries {
  def selectLatestLogEntryId()(implicit connection: Connection): Option[Index]

  def selectFromLog(
      start: Index,
      end: Index,
  )(implicit connection: Connection): immutable.Seq[(Index, LedgerRecord)]

  def selectStateValuesByKeys(
      keys: Seq[Key],
  )(implicit connection: Connection): immutable.Seq[Option[Value]]
}
