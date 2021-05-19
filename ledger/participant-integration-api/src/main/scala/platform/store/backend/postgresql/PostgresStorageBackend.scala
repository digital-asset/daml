// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.platform.store.backend.{DBDTOV1, StorageBackend}

import scala.collection.mutable

object PostgresStorageBackend extends StorageBackend[PostgresDbBatch] {

  private val preparedDeleteCommandSubmissions =
    """
      |DELETE FROM participant_command_submissions
      |WHERE deduplication_key IN (
      |  SELECT deduplication_key_in
      |  FROM unnest(?)
      |  as t(deduplication_key_in)
      |)
      |""".stripMargin

  override def insertBatch(
      connection: Connection,
      postgresDbBatch: PostgresDbBatch,
  ): Unit = {

    def execute(statement: String, setupData: PreparedStatement => Unit): Unit = {
      val preparedStatement = connection.prepareStatement(statement)
      setupData(preparedStatement)
      preparedStatement.execute()
      preparedStatement.close()
      ()
    }

    def executeTable(pgTable: PGTable[_], data: Array[Array[_]]): Unit =
      if (data(0).length > 0) execute(pgTable.insertStatement, pgTable.setupData(data, _))

    executeTable(PGSchema.commandCompletions, postgresDbBatch.commandCompletionsBatch)
    executeTable(PGSchema.configurationEntries, postgresDbBatch.configurationEntriesBatch)
    executeTable(PGSchema.eventsDivulgence, postgresDbBatch.eventsBatchDivulgence)
    executeTable(PGSchema.eventsCreate, postgresDbBatch.eventsBatchCreate)
    executeTable(PGSchema.eventsConsumingExercise, postgresDbBatch.eventsBatchConsumingExercise)
    executeTable(
      PGSchema.eventsNonConsumingExercise,
      postgresDbBatch.eventsBatchNonConsumingExercise,
    )
    executeTable(PGSchema.packageEntries, postgresDbBatch.packageEntriesBatch)
    executeTable(PGSchema.packages, postgresDbBatch.packagesBatch)
    executeTable(PGSchema.parties, postgresDbBatch.partiesBatch)
    executeTable(PGSchema.partyEntries, postgresDbBatch.partyEntriesBatch)

    if (postgresDbBatch.commandDeduplicationBatch.length > 0)
      execute(
        preparedDeleteCommandSubmissions,
        _.setObject(1, postgresDbBatch.commandDeduplicationBatch),
      )
  }

  private val preparedUpdateLedgerEnd: Connection => PreparedStatement = _.prepareStatement(
    """
      |UPDATE
      |  parameters
      |SET
      |  ledger_end = ?,
      |  ledger_end_sequential_id = ?
      |
      |""".stripMargin
  )

  override def updateParams(connection: Connection, params: StorageBackend.Params): Unit = {
    val preparedStatement = preparedUpdateLedgerEnd(connection)
    preparedStatement.setString(1, params.ledgerEnd.toHexString)
    preparedStatement.setLong(2, params.eventSeqId)
    preparedStatement.execute()
    preparedStatement.close()
    ()
  }

  override def initialize(connection: Connection): StorageBackend.LedgerEnd = {
    val result @ StorageBackend.LedgerEnd(offset, _) = ledgerEnd(connection)

    offset.foreach { existingOffset =>
      val preparedStatement = preparedDeleteIngestionOverspillEntries(connection)
      List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).foreach(
        preparedStatement.setString(_, existingOffset.toHexString)
      )
      preparedStatement.execute()
      preparedStatement.close()
    }

    result
  }

  override def ledgerEnd(connection: Connection): StorageBackend.LedgerEnd = {
    val queryStatement = connection.createStatement()
    val params = fetch(
      queryStatement.executeQuery(
        """
        |SELECT
        |  ledger_end,
        |  ledger_end_sequential_id
        |FROM
        |  parameters
        |
        |""".stripMargin
      )
    )(rs =>
      StorageBackend.LedgerEnd(
        lastOffset =
          if (rs.getString(1) == null) None
          else Some(Offset.fromHexString(Ref.HexString.assertFromString(rs.getString(1)))),
        lastEventSeqId = Option(rs.getLong(2)),
      )
    )
    queryStatement.close()
    assert(params.size == 1)
    params.head
  }

  private val preparedDeleteIngestionOverspillEntries: Connection => PreparedStatement =
    _.prepareStatement(
      """
      |DELETE
      |FROM configuration_entries
      |WHERE ledger_offset > ?;
      |
      |DELETE
      |FROM package_entries
      |WHERE ledger_offset > ?;
      |
      |-- TODO append-only: we do not have currently an index to support efficiently this operation. either add or make sure it is okay to do full table scans here
      |DELETE
      |FROM packages
      |WHERE ledger_offset > ?;
      |
      |DELETE
      |FROM participant_command_completions
      |WHERE completion_offset > ?;
      |
      |DELETE
      |FROM participant_events_divulgence
      |WHERE event_offset > ?;
      |
      |DELETE
      |FROM participant_events_create
      |WHERE event_offset > ?;
      |
      |DELETE
      |FROM participant_events_consuming_exercise
      |WHERE event_offset > ?;
      |
      |DELETE
      |FROM participant_events_non_consuming_exercise
      |WHERE event_offset > ?;
      |
      |-- TODO append-only: we do not have currently an index to support efficiently this operation. either add or make sure it is okay to do full table scans here
      |DELETE
      |FROM parties
      |WHERE ledger_offset > ?;
      |
      |DELETE
      |FROM party_entries
      |WHERE ledger_offset > ?;
      |
      |""".stripMargin
    )

  private def fetch[T](resultSet: ResultSet)(parse: ResultSet => T): Vector[T] = {
    val buffer = mutable.ArrayBuffer.empty[T]
    while (resultSet.next()) {
      buffer += parse(resultSet)
    }
    resultSet.close()
    buffer.toVector
  }

  override def batch(dbDtos: Vector[DBDTOV1]): PostgresDbBatch = PostgresDbBatch(dbDtos)
}
