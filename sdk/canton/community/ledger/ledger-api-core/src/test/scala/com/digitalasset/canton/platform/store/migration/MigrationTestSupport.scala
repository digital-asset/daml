// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.migration

import com.digitalasset.canton.platform.store.DbType
import com.digitalasset.canton.platform.store.FlywayMigrations.locations
import org.flywaydb.core.Flyway

import java.sql.{Connection, ResultSet}
import javax.sql.DataSource
import scala.util.Using
import scala.util.control.NonFatal

object MigrationTestSupport {
  def migrateTo(version: String)(implicit dataSource: DataSource, dbType: DbType): Unit = {
    Flyway
      .configure()
      .locations(locations(dbType)*)
      .dataSource(dataSource)
      .target(version)
      .load()
      .migrate()
    ()
  }

  trait DbDataType {
    def get(resultSet: ResultSet, index: Int): Any
    def put(value: Any): String
    def optional: DbDataType = DbDataType.Optional(this)
  }

  object DbDataType {
    final case class Optional(delegate: DbDataType) extends DbDataType {
      override def get(resultSet: ResultSet, index: Int): Any =
        if (resultSet.getObject(index) == null) None
        else Some(delegate.get(resultSet, index))

      override def put(value: Any): String = value.asInstanceOf[Option[Any]] match {
        case Some(someValue) => delegate.put(someValue)
        case None => "null"
      }
    }
  }

  final case class TableSchema(
      tableName: String,
      orderByColumn: String,
      columns: Map[String, DbDataType],
  ) {
    val columnsList: List[String] = columns.keySet.toList

    def ++(entries: (String, DbDataType)*): TableSchema =
      copy(columns = columns ++ entries)

    def --(cols: String*): TableSchema =
      copy(columns = columns -- cols)
  }

  object TableSchema {
    def apply(tableName: String, orderByColumn: String)(
        entries: (String, DbDataType)*
    ): TableSchema =
      TableSchema(tableName, orderByColumn, entries.toMap)
  }

  type Row = Map[String, Any]

  def row(entries: (String, Any)*): Row = entries.toMap[String, Any]

  implicit class RowOps(val r: Row) extends AnyVal {
    def updateIn[T](key: String)(f: T => Any): Row = r + (key -> f(r(key).asInstanceOf[T]))
  }

  implicit class VectorRowOps(val r: Vector[Row]) extends AnyVal {
    def updateInAll[T](key: String)(f: T => Any): Vector[Row] = r.map(_.updateIn(key)(f))
  }

  def insertMany(
      inputs: (TableSchema, Seq[Row])*
  )(implicit connection: Connection): Unit =
    inputs.foreach { case (tableSchema, rows) =>
      insert(tableSchema, rows*)
    }

  def insert(tableSchema: TableSchema, rows: Row*)(implicit connection: Connection): Unit =
    rows.foreach { row =>
      assert(
        tableSchema.columns.keySet == row.keySet, {
          val onlyInTable = tableSchema.columns.keySet.removedAll(row.keySet)
          val onlyInRow = row.keySet.removedAll(tableSchema.columns.keySet)
          s"table name: ${tableSchema.tableName} - columns only in the table's schema $onlyInTable; columns only in the row's schema: $onlyInRow"
        },
      )
      val values =
        tableSchema.columnsList.map(column =>
          try
            tableSchema.columns(column).put(row(column))
          catch {
            case NonFatal(e) =>
              throw new RuntimeException(s"Could not convert value for column: '$column'", e)
          }
        )
      val insertStatement =
        s"""INSERT INTO ${tableSchema.tableName}
         |(${tableSchema.columnsList.mkString(", ")})
         |VALUES (${values.mkString(", ")})""".stripMargin
      try
        Using.resource(connection.createStatement())(_.execute(insertStatement))
      catch {
        case NonFatal(e) =>
          throw new RuntimeException(s"Error while executing query: $insertStatement", e)
      }
      ()
    }

  @SuppressWarnings(Array("org.wartremover.warts.While"))
  def fetchTable(tableSchema: TableSchema)(implicit connection: Connection): Vector[Row] = {
    val query =
      s"""SELECT ${tableSchema.columnsList.mkString(", ")}
         |FROM ${tableSchema.tableName}
         |ORDER BY ${tableSchema.orderByColumn}
         |""".stripMargin
    Using.resource(connection.createStatement())(statement =>
      Using.resource(statement.executeQuery(query)) { resultSet =>
        val resultBuilder = Vector.newBuilder[Map[String, Any]]
        while (resultSet.next()) {
          val row = tableSchema.columnsList.zipWithIndex.map { case (column, i) =>
            column -> tableSchema.columns(column).get(resultSet, i + 1)
          }.toMap
          resultBuilder.addOne(row)
        }
        resultBuilder.result()
      }
    )
  }
}
