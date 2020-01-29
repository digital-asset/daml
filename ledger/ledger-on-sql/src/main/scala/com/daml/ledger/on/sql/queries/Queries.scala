// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.sql.Connection

import anorm.{BatchSql, NamedParameter}
import com.daml.ledger.on.sql.queries.Queries._
import com.daml.ledger.participant.state.kvutils.DamlKvutils
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.google.protobuf.ByteString

import scala.collection.immutable

trait Queries {
  def selectFromLog(
      start: Index,
      end: Index,
  )(implicit connection: Connection): immutable.Seq[(Index, LedgerRecord)]

  def insertIntoLog(
      entry: DamlKvutils.DamlLogEntryId,
      envelope: ByteString,
  )(implicit connection: Connection): Index

  def selectStateByKeys(
      keys: Iterable[DamlKvutils.DamlStateKey],
  )(
      implicit connection: Connection,
  ): immutable.Seq[(DamlKvutils.DamlStateKey, Option[DamlKvutils.DamlStateValue])]

  def updateState(
      stateUpdates: Map[DamlKvutils.DamlStateKey, DamlKvutils.DamlStateValue],
  )(implicit connection: Connection): Unit
}

object Queries {
  type Index = Long

  val TablePrefix = "ledger"
  val LogTable = s"${TablePrefix}_log"
  val StateTable = s"${TablePrefix}_state"

  def executeBatchSql(
      query: String,
      params: Iterable[immutable.Seq[NamedParameter]],
  )(implicit connection: Connection): Unit = {
    if (params.nonEmpty)
      BatchSql(query, params.head, params.drop(1).toArray: _*).execute()
    ()
  }
}
