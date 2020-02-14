// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.io.InputStream
import java.sql.Connection

import anorm.SqlParser._
import anorm._
import com.daml.ledger.on.sql.Index
import com.daml.ledger.on.sql.queries.Queries._
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.kvutils.api.LedgerRecord
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.google.protobuf.ByteString

import scala.collection.{breakOut, immutable}

trait CommonQueries extends Queries {
  override def selectLatestLogEntryId()(implicit connection: Connection): Option[Index] =
    SQL"SELECT MAX(sequence_no) max_sequence_no FROM #$LogTable"
      .as(get[Option[Long]]("max_sequence_no").singleOpt)
      .flatten

  override def selectFromLog(
      start: Index,
      end: Index,
  )(implicit connection: Connection): immutable.Seq[(Index, LedgerRecord)] =
    SQL"SELECT sequence_no, entry_id, envelope FROM #$LogTable WHERE sequence_no >= $start AND sequence_no < $end"
      .as(
        (long("sequence_no") ~ binaryStream("entry_id") ~ byteArray("envelope")).map {
          case index ~ entryId ~ envelope =>
            index -> LedgerRecord(Offset(Array(index)), DamlLogEntryId.parseFrom(entryId), envelope)
        }.*,
      )

  def selectStateValuesByKeys(
      keys: Seq[Key],
  )(implicit connection: Connection): immutable.Seq[Option[Value]] = {
    val results = SQL"SELECT key, value FROM #$StateTable WHERE key IN ($keys)"
      .fold(Map.newBuilder[ByteString, Array[Byte]], ColumnAliaser.empty)((builder, row) =>
        builder += ByteString.readFrom(row[InputStream]("key")) -> row[Value]("value"))
      .right
      .get
      .result()
    keys.map(key => results.get(ByteString.copyFrom(key)))(breakOut)
  }

  override def updateState(stateUpdates: Seq[(Key, Value)])(implicit connection: Connection): Unit =
    executeBatchSql(updateStateQuery, stateUpdates.map {
      case (key, value) => Seq[NamedParameter]("key" -> key, "value" -> value)
    })

  protected val updateStateQuery: String
}
