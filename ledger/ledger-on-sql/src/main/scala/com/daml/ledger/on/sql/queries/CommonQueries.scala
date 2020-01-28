// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.sql.Connection

import anorm.SqlParser._
import anorm._
import com.daml.ledger.on.sql.queries.Queries._
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.daml.ledger.participant.state.v1.Offset
import com.google.protobuf.ByteString

import scala.collection.immutable

trait CommonQueries extends Queries {
  override def selectFromLog(
      start: Index,
      end: Index,
  )(implicit connection: Connection): immutable.Seq[(Index, LedgerRecord)] =
    SQL"SELECT sequence_no, entry_id, envelope FROM #$LogTable WHERE sequence_no >= $start AND sequence_no < $end"
      .as(
        (long("sequence_no") ~ byteArray("entry_id") ~ byteArray("envelope")).map {
          case index ~ entryId ~ envelope =>
            index -> LedgerRecord(
              Offset(Array(index)),
              DamlLogEntryId
                .newBuilder()
                .setEntryId(ByteString.copyFrom(entryId))
                .build(),
              envelope,
            )
        }.*,
      )

  override def selectStateByKeys(
      keys: Iterable[DamlStateKey],
  )(implicit connection: Connection): immutable.Seq[(DamlStateKey, Option[DamlStateValue])] =
    SQL"SELECT key, value FROM #$StateTable WHERE key IN (${keys.map(_.toByteArray).toSeq})"
      .as((byteArray("key") ~ byteArray("value")).map {
        case key ~ value =>
          DamlStateKey.parseFrom(key) -> Some(DamlStateValue.parseFrom(value))
      }.*)

  override def updateState(
      stateUpdates: Map[DamlStateKey, DamlStateValue],
  )(implicit connection: Connection): Unit =
    Queries.executeBatchSql(
      updateStateQuery,
      stateUpdates.map {
        case (key, value) =>
          immutable.Seq[NamedParameter]("key" -> key.toByteArray, "value" -> value.toByteArray)
      },
    )

  protected val updateStateQuery: String
}
