// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.io.InputStream
import java.sql.Connection
import java.time.Instant

import anorm.SqlParser._
import anorm._
import com.daml.ledger.on.sql.Index
import com.daml.ledger.on.sql.queries.Queries._
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.kvutils.api.LedgerEntry
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.google.protobuf.ByteString

import scala.collection.{breakOut, immutable}
import scala.util.Try

trait CommonQueries extends Queries {
  protected implicit val connection: Connection

  override final def selectLatestLogEntryId(): Try[Option[Index]] = Try {
    SQL"SELECT MAX(sequence_no) max_sequence_no FROM #$LogTable"
      .as(get[Option[Long]]("max_sequence_no").singleOpt)
      .flatten
  }

  override final def selectFromLog(
      start: Index,
      end: Index,
  ): Try[immutable.Seq[(Index, LedgerEntry)]] = Try {
    SQL"SELECT sequence_no, entry_id, envelope, heartbeat_timestamp FROM #$LogTable WHERE sequence_no >= $start AND sequence_no < $end ORDER BY sequence_no"
      .as(
        (long("sequence_no")
          ~ get[Option[InputStream]]("entry_id")
          ~ get[Option[InputStream]]("envelope")
          ~ get[Option[Long]]("heartbeat_timestamp")).map {
          case index ~ Some(entryId) ~ Some(envelope) ~ None =>
            index -> LedgerEntry.LedgerRecord(
              Offset(Array(index)),
              DamlLogEntryId.parseFrom(entryId),
              ByteString.readFrom(envelope),
            )
          case index ~ None ~ None ~ Some(heartbeatTimestamp) =>
            index -> LedgerEntry.Heartbeat(
              Offset(Array(index)),
              Instant.ofEpochMilli(heartbeatTimestamp),
            )
          case _ =>
            throw new IllegalStateException(s"Invalid data in the $LogTable table.")
        }.*,
      )
  }

  override final def selectStateValuesByKeys(keys: Seq[Key]): Try[immutable.Seq[Option[Value]]] =
    Try {
      val results =
        SQL"SELECT key, value FROM #$StateTable WHERE key IN (${keys.map(_.toByteArray)})"
          .fold(Map.newBuilder[Key, Value], ColumnAliaser.empty) { (builder, row) =>
            val key = ByteString.readFrom(row[InputStream]("key"))
            val value = ByteString.readFrom(row[InputStream]("value"))
            builder += key -> value
          }
          .fold(exceptions => throw exceptions.head, _.result())
      keys.map(results.get)(breakOut)
    }

  override final def updateState(stateUpdates: Seq[(Key, Value)]): Try[Unit] = Try {
    executeBatchSql(updateStateQuery, stateUpdates.map {
      case (key, value) =>
        Seq[NamedParameter]("key" -> key.toByteArray, "value" -> value.toByteArray)
    })
  }

  protected val updateStateQuery: String
}
