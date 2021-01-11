// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import com.daml.ledger.on.sql.Index
import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord

import scala.collection.immutable
import scala.util.Try

trait ReadQueries {
  def selectLatestLogEntryId(): Try[Option[Index]]

  def selectFromLog(start: Index, end: Index): Try[immutable.Seq[(Index, LedgerRecord)]]

  def selectStateValuesByKeys(keys: Iterable[Raw.Key]): Try[immutable.Seq[Option[Raw.Value]]]
}
