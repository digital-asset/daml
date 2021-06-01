// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.sql.{Connection, ResultSet}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.platform.store.DbType
import com.daml.platform.store.backend.oracle.OracleStorageBackend
import com.daml.platform.store.backend.postgresql.PostgresStorageBackend

import scala.collection.mutable

/** Encapsulates the interface which hides database technology specific implementations for parallel ingestion.
  *
  * @tparam DB_BATCH Since parallel ingestion comes also with batching, this implementation specific type allows separation of the CPU intensive batching operation from the pure IO intensive insertBatch operation.
  */
trait StorageBackend[DB_BATCH] {

  /** The CPU intensive batching operation hides the batching logic, and the mapping to the database specific representation of the inserted data.
    * This should be pure CPU logic without IO.
    *
    * @param dbDtos is a collection of DbDto from which the batch is formed
    * @return the database-specific batch DTO, which can be inserted via insertBatch
    */
  def batch(dbDtos: Vector[DbDto]): DB_BATCH

  /** Using a JDBC connection, a batch will be inserted into the database.
    * No significant CPU load, mostly blocking JDBC communication with the database backend.
    *
    * @param connection to be used when inserting the batch
    * @param batch to be inserted
    */
  def insertBatch(connection: Connection, batch: DB_BATCH): Unit

  /** This method is used to update the parameters table: setting the new observable ledger-end, and other parameters.
    * No significant CPU load, mostly blocking JDBC communication with the database backend.
    *
    * @param connection to be used when updating the parameters table
    * @param params the parameters
    */
  def updateParams(connection: Connection, params: StorageBackend.Params): Unit

  /** Custom initialization code before the start of an ingestion.
    * This method is responsible for the recovery after a possibly non-graceful stop of previous indexing.
    * No significant CPU load, mostly blocking JDBC communication with the database backend.
    *
    * @param connection to be used when initializing
    * @return the LedgerEnd, which should be the basis for further indexing.
    */
  def initialize(connection: Connection): StorageBackend.LedgerEnd

  /** Query the ledgerEnd, read from the parameters table.
    * No significant CPU load, mostly blocking JDBC communication with the database backend.
    *
    * @param connection to be used to get the LedgerEnd
    * @return the LedgerEnd, which should be the basis for further indexing
    */
  def ledgerEnd(connection: Connection): StorageBackend.LedgerEnd = {
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

  private def fetch[T](resultSet: ResultSet)(parse: ResultSet => T): Vector[T] = {
    val buffer = mutable.ArrayBuffer.empty[T]
    while (resultSet.next()) {
      buffer += parse(resultSet)
    }
    resultSet.close()
    buffer.toVector
  }
}

object StorageBackend {
  case class Params(ledgerEnd: Offset, eventSeqId: Long)

  case class LedgerEnd(lastOffset: Option[Offset], lastEventSeqId: Option[Long])

  def of(dbType: DbType): StorageBackend[_] =
    dbType match {
      case DbType.H2Database => throw new UnsupportedOperationException("H2 not supported yet")
      case DbType.Postgres => PostgresStorageBackend
      case DbType.Oracle => OracleStorageBackend
    }
}
