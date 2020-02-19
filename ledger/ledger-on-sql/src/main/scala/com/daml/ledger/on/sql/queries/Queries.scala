// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.sql.Connection

import anorm.{BatchSql, NamedParameter}
import com.daml.ledger.on.sql.Index
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.daml.ledger.participant.state.v1.LedgerId
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

import scala.collection.immutable

trait Queries {
  def updateOrRetrieveLedgerId(
      providedLedgerId: LedgerId,
  )(implicit connection: Connection): LedgerId

  def selectLatestLogEntryId()(implicit connection: Connection): Option[Index]

  def selectFromLog(
      start: Index,
      end: Index,
  )(implicit connection: Connection): immutable.Seq[(Index, LedgerRecord)]

  def insertIntoLog(key: Key, value: Value)(implicit connection: Connection): Index

  def selectStateValuesByKeys(
      keys: Seq[Key],
  )(implicit connection: Connection): immutable.Seq[Option[Value]]

  def updateState(stateUpdates: Seq[(Key, Value)])(implicit connection: Connection): Unit
}

object Queries {
  val TablePrefix = "ledger"
  val LogTable = s"${TablePrefix}_log"
  val MetaTable = s"${TablePrefix}_meta"
  val StateTable = s"${TablePrefix}_state"

  private[queries] val MetaTableKey = 0

  def executeBatchSql(
      query: String,
      params: Iterable[Seq[NamedParameter]],
  )(implicit connection: Connection): Unit = {
    if (params.nonEmpty)
      BatchSql(query, params.head, params.drop(1).toArray: _*).execute()
    ()
  }
}
