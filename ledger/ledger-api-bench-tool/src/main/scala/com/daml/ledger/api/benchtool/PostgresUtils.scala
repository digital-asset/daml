// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import java.sql.{Connection, DriverManager, Statement}

import com.daml.ledger.api.benchtool.LedgerApiBenchTool.logger

object PostgresUtils {

  def invokeVacuumAnalyze(indexDbJdbcUrl: String): Unit = {
    val connection = DriverManager.getConnection(indexDbJdbcUrl)
    try {
      val stmt = connection.createStatement()
      val vacuumQuery = "VACUUM ANALYZE"
      try {
        logger.info(
          s"Executing '$vacuumQuery' on the IndexDB identified by JDBC URL: '${indexDbJdbcUrl}' ..."
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
    val stmt = connection.createStatement()
    val query =
      "SELECT relname, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze FROM pg_stat_user_tables ORDER BY relname"
    try {
      logger.info(
        "Executing SQL query: " + query
      )
      stmt.execute(
        query
      )
      printQueryResult(stmt)
    } finally {
      stmt.close()
    }
  }

  private def printQueryResult(s: Statement): Unit = {
    val rs = s.getResultSet
    val meta = rs.getMetaData
    val colCount = meta.getColumnCount
    val buffer = new StringBuffer()
    try {
      while (rs.next()) {
        val text = 1
          .to(colCount)
          .map(colNumber =>
            f"${meta.getColumnName(colNumber)} ${rs.getString(colNumber) + ","}%-45s"
          )
          .mkString(" ")
        buffer.append(text).append("\n")
      }
    } finally {
      logger.info(buffer.toString)
    }
  }

}
