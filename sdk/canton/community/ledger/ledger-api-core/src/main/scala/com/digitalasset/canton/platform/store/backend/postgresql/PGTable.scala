// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import com.digitalasset.canton.platform.store.backend.common.{BaseTable, Field, Table}

import java.sql.Connection

private[postgresql] object PGTable {

  private def transposedInsertBase[FROM](
      insertStatement: String,
      ordering: Option[Ordering[FROM]] = None,
  )(fields: Seq[(String, Field[FROM, _, _])]): Table[FROM] =
    new BaseTable[FROM](fields, ordering) {
      override def executeUpdate: Array[Array[_]] => Connection => Unit =
        data =>
          connection =>
            Table.ifNonEmpty(data) {
              val preparedStatement = connection.prepareStatement(insertStatement)
              fields.indices.foreach { i =>
                preparedStatement.setObject(i + 1, data(i))
              }
              preparedStatement.execute()
              preparedStatement.close()
            }
    }

  private def transposedInsertStatement(
      tableName: String,
      fields: Seq[(String, Field[_, _, _])],
      statementSuffix: String = "",
  ): String = {
    def commaSeparatedOf(extractor: ((String, Field[_, _, _])) => String): String =
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
      fields: (String, Field[FROM, _, _])*
  ): Table[FROM] =
    transposedInsertBase(transposedInsertStatement(tableName, fields))(fields)

  def idempotentTransposedInsert[FROM](
      tableName: String,
      keyFieldIndex: Int,
      ordering: Ordering[FROM],
  )(
      fields: (String, Field[FROM, _, _])*
  ): Table[FROM] = {
    val insertSuffix = s"on conflict (${fields(keyFieldIndex)._1}) do nothing"
    transposedInsertBase(
      transposedInsertStatement(tableName, fields, insertSuffix),
      Some(ordering),
    )(fields)
  }
}
