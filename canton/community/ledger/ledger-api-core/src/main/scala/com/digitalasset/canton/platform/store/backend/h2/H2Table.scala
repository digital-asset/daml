// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.h2

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.platform.store.backend.common.{BaseTable, Field, Table}

import java.sql.Connection
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

private[h2] object H2Table {
  private def idempotentBatchedInsertBase[FROM](
      insertStatement: String,
      keyFieldIndex: Int,
  )(fields: Seq[(String, Field[FROM, _, _])]): Table[FROM] =
    new BaseTable[FROM](fields) {
      override def executeUpdate: Array[Array[_]] => Connection => Unit =
        data =>
          connection =>
            Table.ifNonEmpty(data) {
              val preparedStatement = connection.prepareStatement(insertStatement)
              data(0).indices.foreach { dataIndex =>
                fields(keyFieldIndex)._2.prepareData(
                  preparedStatement,
                  1,
                  data(keyFieldIndex)(dataIndex),
                )
                fields.indices.foreach { fieldIndex =>
                  fields(fieldIndex)._2.prepareData(
                    preparedStatement,
                    fieldIndex + 2,
                    data(fieldIndex)(dataIndex),
                  )
                }
                preparedStatement.addBatch()
              }
              retry(10) {
                preparedStatement.executeBatch()
              }.discard
              preparedStatement.close()
            }
    }

  @tailrec
  private def retry[T](maxRetry: Int)(body: => T): T = Try(body) match {
    case Success(result) => result
    case Failure(t) if maxRetry <= 0 => throw t
    case Failure(_) => retry(maxRetry - 1)(body)
  }

  private def idempotentBatchedInsertStatement(
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
    val keyFieldSelectExpression = fields(keyFieldIndex)._2.selectFieldExpression("?")
    s"""MERGE INTO $tableName USING DUAL on $keyFieldName = $keyFieldSelectExpression
       |WHEN NOT MATCHED THEN INSERT ($tableFields)
       |VALUES ($selectFields)
       |""".stripMargin
  }

  def idempotentBatchedInsert[FROM](tableName: String, keyFieldIndex: Int)(
      fields: (String, Field[FROM, _, _])*
  ): Table[FROM] =
    idempotentBatchedInsertBase(
      idempotentBatchedInsertStatement(tableName, fields, keyFieldIndex),
      keyFieldIndex,
    )(fields)
}
