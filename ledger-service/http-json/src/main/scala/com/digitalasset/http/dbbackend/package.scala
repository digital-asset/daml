// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import cats.implicits._
import com.daml.dbutils.ConnectionPool
import doobie.{FRS, HPS, HRS, PreparedStatementIO, ResultSetIO}
import doobie._
import doobie.implicits._

import scala.util.Try
import scala.annotation.nowarn

package object dbbackend {
  @nowarn
  final case class SqlQueryResult(columnNames: List[String], rows: List[List[String]])

  private def exec: PreparedStatementIO[SqlQueryResult] = {

    // Read the specified columns from the resultset.
    def readAll(cols: List[Int]): ResultSetIO[List[List[Object]]] =
      readOne(cols).whileM[List](HRS.next)

    // Take a list of column offsets and read a parallel list of values.
    def readOne(cols: List[Int]): ResultSetIO[List[Object]] =
      cols.traverse(FRS.getObject)

    for {
      md <- HPS.getMetaData
      cols = (1 to md.getColumnCount).toList
      colNames = cols.map(md.getColumnName)
      data <- HPS.executeQuery(readAll(cols))
    } yield SqlQueryResult(colNames, data.map(_.map(Option(_).map(_.toString).getOrElse("null"))))
  }

  def runQuery(query: String)(implicit xa: ConnectionPool.T): Try[SqlQueryResult] =
    Try(Fragment.const(query).execWith(exec).transact(xa).unsafeRunSync())
}
