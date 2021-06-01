// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.sql.PreparedStatement

case class PGTable[FROM](
    tableName: String,
    fields: Vector[(String, PGField[FROM, _, _])],
    insertSuffix: String = "",
) {
  val insertStatement: String = {
    def comaSeparatedOf(extractor: ((String, PGField[FROM, _, _])) => String): String =
      fields.view
        .map(extractor)
        .mkString(",")
    def inputFieldName: String => String = fieldName => s"${fieldName}_in"
    val tableFields = comaSeparatedOf(_._1)
    val selectFields = comaSeparatedOf { case (fieldName, field) =>
      field.selectFieldExpression(inputFieldName(fieldName))
    }
    val unnestFields = comaSeparatedOf(_ => "?")
    val inputFields = comaSeparatedOf(fieldDef => inputFieldName(fieldDef._1))
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

  def setupData(data: Array[Array[_]], preparedStatement: PreparedStatement): Unit =
    fields.indices.foreach { i =>
      preparedStatement.setObject(i + 1, data(i))
    }
}

object PGTable {
  def apply[FROM](tableName: String)(fields: (String, PGField[FROM, _, _])*): PGTable[FROM] =
    PGTable(tableName, fields.toVector)
}
