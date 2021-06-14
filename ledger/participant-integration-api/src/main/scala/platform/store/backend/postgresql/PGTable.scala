// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.sql.Connection

private[postgresql] case class PGTable[FROM](
    tableName: String,
    fields: Vector[(String, PGField[FROM, _, _])],
    insertSuffix: String = "",
) {
  private val insertStatement: String = {
    def commaSeparatedOf(extractor: ((String, PGField[FROM, _, _])) => String): String =
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
       | $insertSuffix
       |""".stripMargin
  }

  def prepareData(in: Vector[FROM]): Array[Array[_]] =
    fields.view.map(_._2.toArray(in)).toArray

  def executeInsert(data: Array[Array[_]], connection: Connection): Unit =
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

private[postgresql] object PGTable {
  def apply[FROM](tableName: String)(fields: (String, PGField[FROM, _, _])*): PGTable[FROM] =
    PGTable(tableName, fields.toVector)
}
