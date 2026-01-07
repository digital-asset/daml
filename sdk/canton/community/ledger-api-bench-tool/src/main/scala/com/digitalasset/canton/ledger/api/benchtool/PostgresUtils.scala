// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool

import com.digitalasset.canton.ledger.api.benchtool.LedgerApiBenchTool.logger
import com.digitalasset.canton.platform.store.backend.common.QueryStrategy

import java.sql.{Connection, DriverManager}

object PostgresUtils {

  def invokeVacuumAnalyze(indexDbJdbcUrl: String): Unit = {
    val connection = DriverManager.getConnection(indexDbJdbcUrl)
    try {
      val stmt = connection.createStatement()
      val vacuumQuery = "VACUUM ANALYZE"
      try {
        logger.info(
          s"Executing '$vacuumQuery' on the IndexDB identified by JDBC URL: '$indexDbJdbcUrl' ..."
        )
        stmt.executeUpdate(vacuumQuery)
        logger.info(s"Executed '$vacuumQuery'")
      } finally {
        stmt.close()
        inspectVacuumAndAnalyzeState(connection)
      }
    } finally {
      connection.close()
    }
  }

  private def inspectVacuumAndAnalyzeState(connection: Connection): Unit = {
    val query =
      "SELECT relname, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze FROM pg_stat_user_tables ORDER BY relname"
    logger.info("Executing SQL query: " + query)
    val rows = QueryStrategy.plainJdbcQuery(query) { rs =>
      val meta = rs.getMetaData
      val colCount = meta.getColumnCount
      1.to(colCount)
        .map(colNumber => f"${meta.getColumnName(colNumber)} ${rs.getString(colNumber) + ","}%-45s")
        .mkString(" ")
    }(connection)
    logger.info(rows.mkString("\n"))
  }

}
