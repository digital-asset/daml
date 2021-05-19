// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.sql.PreparedStatement

case class PGTable[FROM](
    tableName: String,
    fields: Vector[PGField[FROM, _]],
    insertSuffix: String = "",
) {
  val insertStatement: String = {
    def comaSeparatedOf(fieldExtractor: PGField[FROM, _] => String): String =
      fields.view.map(fieldExtractor).mkString(",")
    s"""
       |INSERT INTO $tableName
       |   (${comaSeparatedOf(_.fieldName)})
       | SELECT
       |   ${comaSeparatedOf(_.selectFieldExpression)}
       | FROM
       |   unnest(${comaSeparatedOf(_ => "?")})
       | as t(${comaSeparatedOf(_.inputFieldName)})
       | $insertSuffix
       |""".stripMargin
  }

  def prepareData(in: Vector[FROM]): Array[Array[_]] =
    fields.view.map(_.generateArray(in)).toArray

  def setupData(data: Array[Array[_]], preparedStatement: PreparedStatement): Unit =
    fields.indices.foreach { i =>
      fields(i).setDBObject(preparedStatement, i, data(i))
    }
}

object PGTable {
  def apply[FROM](tableName: String)(fields: PGField[FROM, _]*): PGTable[FROM] =
    PGTable(tableName, fields.toVector)
}
