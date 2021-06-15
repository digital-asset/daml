// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.sql.Connection

trait Table[FROM] {
  def prepareData(in: Vector[FROM]): Array[Array[_]]
  def executeUpdate(data: Array[Array[_]], connection: Connection): Unit
}

abstract class BaseTable[FROM](fields: Seq[(String, PGField[FROM, _, _])]) extends Table[FROM] {
  override def prepareData(in: Vector[FROM]): Array[Array[_]] =
    fields.view.map(_._2.toArray(in)).toArray
}

object PGTable {

  private def transposedInsertBase[FROM](
      insertStatement: String
  )(fields: Seq[(String, PGField[FROM, _, _])]): Table[FROM] =
    new BaseTable[FROM](fields) {
      override def executeUpdate(data: Array[Array[_]], connection: Connection): Unit =
        if (data(0).length > 0) { // data(0) accesses the array of data for the first column of the table. This is safe because tables without columns are not supported. Also because of the transposed data-structure here all columns will have data-arrays of the same length.
          val preparedStatement = connection.prepareStatement(insertStatement)
          fields.indices.foreach { i =>
            preparedStatement.setObject(i + 1, data(i))
          }
          preparedStatement.execute()
          preparedStatement.close()
          ()
        }
    }

  private def transposedInsertStatement(
      tableName: String,
      fields: Seq[(String, PGField[_, _, _])],
      statementSuffix: String = "",
  ): String = {
    def commaSeparatedOf(extractor: ((String, PGField[_, _, _])) => String): String =
      fields.view
        .map(extractor)
        .mkString(",")
    def inputFieldName: String => String = fieldName => s"${fieldName}_in"
    val tableFields = commaSeparatedOf(_._1)
    val selectFields = commaSeparatedOf { case (fieldName, field) =>
      field.selectFieldExpression(inputFieldName(fieldName))
    }
    val unnestFields = commaSeparatedOf(_ => "?")
    val inputFields = commaSeparatedOf(fieldDef => inputFieldName(fieldDef._1))
    s"""
       |INSERT INTO $tableName
       |   ($tableFields)
       | SELECT
       |   $selectFields
       | FROM
       |   unnest($unnestFields)
       | as t($inputFields)
       | $statementSuffix
       |""".stripMargin
  }

  def transposedInsert[FROM](tableName: String)(
      fields: (String, PGField[FROM, _, _])*
  ): Table[FROM] =
    transposedInsertBase(transposedInsertStatement(tableName, fields))(fields)

  def transposedInsertWithSuffix[FROM](tableName: String, insertSuffix: String)(
      fields: (String, PGField[FROM, _, _])*
  ): Table[FROM] =
    transposedInsertBase(transposedInsertStatement(tableName, fields, insertSuffix))(fields)

  def transposedDelete[FROM](
      tableName: String
  )(field: (String, PGField[FROM, _, _])): Table[FROM] = {
    val tableField = field._1
    val inputField = s"${field._1}_in"
    val selectField = field._2.selectFieldExpression(inputField)
    val deleteStatement = {
      s"""
        |DELETE FROM $tableName
        |WHERE $tableField IN (
        |  SELECT $selectField
        |  FROM unnest(?)
        |  as t($inputField)
        |)
        |""".stripMargin
    }
    new BaseTable[FROM](Seq(field)) {
      override def executeUpdate(data: Array[Array[_]], connection: Connection): Unit =
        if (data(0).length > 0) { // data(0) accesses the array of data for the first column of the table. This is safe because tables without columns are not supported.
          val preparedStatement = connection.prepareStatement(deleteStatement)
          preparedStatement.setObject(1, data(0))
          preparedStatement.execute()
          preparedStatement.close()
          ()
        }
    }
  }
}
