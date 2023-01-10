// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import com.daml.platform.store.interning.StringInterning

private[backend] trait Table[FROM] {
  def prepareData(in: Vector[FROM], stringInterning: StringInterning): Array[Array[_]]
  def executeUpdate: Array[Array[_]] => Connection => Unit
}

private[backend] abstract class BaseTable[FROM](fields: Seq[(String, Field[FROM, _, _])])
    extends Table[FROM] {
  override def prepareData(
      in: Vector[FROM],
      stringInterning: StringInterning,
  ): Array[Array[_]] =
    fields.view.map(_._2.toArray(in, stringInterning)).toArray
}

private[backend] object Table {
  def ifNonEmpty(data: Array[Array[_]])(effect: => Any): Unit =
    // data(0) accesses the array of data for the first column of the table. This is safe because tables without columns are not supported. Also because of the transposed data-structure here all columns will have data-arrays of the same length.
    if (data(0).length > 0) {
      effect
      ()
    }

  private def batchedInsertBase[FROM](
      insertStatement: String
  )(fields: Seq[(String, Field[FROM, _, _])]): Table[FROM] =
    new BaseTable[FROM](fields) {
      override def executeUpdate: Array[Array[_]] => Connection => Unit =
        data =>
          connection =>
            ifNonEmpty(data) {
              val preparedStatement = connection.prepareStatement(insertStatement)
              data(0).indices.foreach { dataIndex =>
                fields.indices.foreach { fieldIndex =>
                  fields(fieldIndex)._2.prepareData(
                    preparedStatement,
                    fieldIndex + 1,
                    data(fieldIndex)(dataIndex),
                  )
                }
                preparedStatement.addBatch()
              }
              preparedStatement.executeBatch()
              preparedStatement.close()
            }
    }

  private def batchedInsertStatement(
      tableName: String,
      fields: Seq[(String, Field[_, _, _])],
  ): String = {
    def commaSeparatedOf(extractor: ((String, Field[_, _, _])) => String): String =
      fields.view
        .map(extractor)
        .mkString(",")
    val tableFields = commaSeparatedOf(_._1)
    val selectFields = commaSeparatedOf { case (_, field) =>
      field.selectFieldExpression("?")
    }
    s"""
       |INSERT INTO $tableName
       |   ($tableFields)
       | VALUES
       |   ($selectFields)
       |""".stripMargin
  }

  def batchedInsert[FROM](tableName: String)(
      fields: (String, Field[FROM, _, _])*
  ): Table[FROM] =
    batchedInsertBase(batchedInsertStatement(tableName, fields))(fields)
}
