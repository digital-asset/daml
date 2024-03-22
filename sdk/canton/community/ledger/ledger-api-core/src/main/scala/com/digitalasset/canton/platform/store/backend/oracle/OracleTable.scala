// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.oracle

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.platform.store.backend.common.{BaseTable, Field, Table}

import java.sql.Connection

private[oracle] object OracleTable {
  private def idempotentInsertBase[FROM](
      insertStatement: String
  )(fields: Seq[(String, Field[FROM, _, _])]): Table[FROM] =
    new BaseTable[FROM](fields) {
      override def executeUpdate: Array[Array[_]] => Connection => Unit =
        data =>
          connection =>
            Table.ifNonEmpty(data) {
              val preparedStatement = connection.prepareStatement(insertStatement)
              data(0).indices.foreach { dataIndex =>
                fields.indices.foreach { fieldIndex =>
                  fields(fieldIndex)._2.prepareData(
                    preparedStatement,
                    fieldIndex + 1,
                    data(fieldIndex)(dataIndex),
                  )
                }
                preparedStatement.execute().discard
              }
              preparedStatement.close()
            }
    }

  private def idempotentInsertStatement(
      tableName: String,
      fields: Seq[(String, Field[_, _, _])],
      keyFieldIndex: Int,
  ): String = {
    def commaSeparatedOf(extractor: ((String, Field[_, _, _])) => String): String =
      fields.view
        .map(extractor)
        .mkString(",")
    val tableFields = commaSeparatedOf(_._1)
    val selectFields = commaSeparatedOf { case (_, field) =>
      field.selectFieldExpression("?")
    }
    val keyFieldName = fields(keyFieldIndex)._1
    s"""
       |INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX ( $tableName ( $keyFieldName ) ) */ INTO $tableName
       |   ($tableFields)
       | VALUES
       |   ($selectFields)
       |""".stripMargin
  }

  def idempotentInsert[FROM](tableName: String, keyFieldIndex: Int)(
      fields: (String, Field[FROM, _, _])*
  ): Table[FROM] =
    idempotentInsertBase(
      idempotentInsertStatement(tableName, fields, keyFieldIndex)
    )(fields)
}
