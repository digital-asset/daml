// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import com.daml.dbutils.ConnectionPool
import org.slf4j.LoggerFactory

import java.io.Closeable
import java.sql.SQLTransientConnectionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicReference
import javax.sql.DataSource
import scala.concurrent.duration.FiniteDuration
import scala.language.existentials
import scala.util.{Failure, Success}

case class Diagnostics(
    dataSource: DataSource with Closeable,
    connectionPool: ConnectionPool.T,
    executorService: ExecutorService,
    clientInfoName: Option[String],
    query: String,
    minDelay: FiniteDuration,
) {

  private val lastRun = new AtomicReference((0L, false))
  private val logger = LoggerFactory.getLogger(classOf[Diagnostics])
  logger.info(s"Diagnostic query:\n$query")

  private def throttle(fx: => Unit) = {
    val (_, doRun) = lastRun.updateAndGet { case (lastRun, _) =>
      val currentTime = System.currentTimeMillis()
      if ((lastRun + minDelay.toMillis) < currentTime)
        (currentTime, true)
      else
        (lastRun, false)
    }

    if (doRun) fx
  }

  def run() = throttle {
    try {
      runQuery(query)(connectionPool) match {
        case Success(rs) =>
          val allRows = rs.columnNames :: rs.rows
          val colLengths = allRows.foldLeft(List.fill(allRows.length)(0)) { case (lengths, row) =>
            lengths.zip(row.map(_.length)).map(x => Math.max(x._1, x._2))
          }
          val format = colLengths.map(l => s"%-${l}s").mkString("| ", " | ", " |")
          val connectionsInfo = allRows.map(row => format.format(row: _*)).mkString("\n")
          logger.warn(s"Connection pool exhausted:\n$connectionsInfo")
        case Failure(exception) => throw exception
      }
    } catch {
      case e: Throwable => logger.error(s"Connection pool diagnostic failed:\n $e")
    }
  }
}

object Diagnostics {
  def isDiagnosticTrigger(exception: SQLTransientConnectionException): Boolean =
    exception.getMessage.contains("Connection is not available")
  def loadQuery(query: Option[java.io.File]): Either[String, Option[String]] = {
    query
      .map { query =>
        try {
          val source = scala.io.Source.fromFile(query)
          try Right(Some(source.mkString))
          finally source.close()
        } catch {
          case e: Exception =>
            Left(s"Failed to load diagnostic query from file ${query.getAbsolutePath}.\n$e")
        }
      }
      .getOrElse(Right(None))
  }
}
