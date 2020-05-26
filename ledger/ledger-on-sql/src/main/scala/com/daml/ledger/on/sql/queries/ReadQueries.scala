// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import com.daml.ledger.on.sql.Index
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

import scala.collection.immutable
import scala.util.Try

trait ReadQueries {
  def selectLatestLogEntryId(): Try[Option[Index]]

  def selectFromLog(start: Index, end: Index): Try[immutable.Seq[(Index, LedgerRecord)]]

  def selectStateValuesByKeys(keys: Seq[Key]): Try[immutable.Seq[Option[Value]]]
}
