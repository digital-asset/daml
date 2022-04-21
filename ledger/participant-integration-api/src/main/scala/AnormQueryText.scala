// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package anorm

import scala.util.Try

object AnormQueryText {

  // Based on usage from `anorm.SimpleSql.unsafeStatement`
  def queryText(sql: SimpleSql[Row]): Try[String] = {
    Sql
      .query(
        tok = sql.sql.stmt.tokens,
        ns = sql.sql.paramsInitialOrder,
        ps = sql.params,
        i = 0,
        buf = new StringBuilder(),
        vs = List.empty[(Int, ParameterValue)],
      )
      .map { case (text, params) =>
        val paramsText = params
          .map { x =>
            val aaa: ParameterValue = x._2
            val valueText = aaa match {
              case dd: DefaultParameterValue[_] =>
                dd.value match {
                  case arr: Array[_] => arr.mkString("[", ", ", "]")
                  case o => o.toString
                }
//
              case o => o.show
            }
            s"${x._1}: ${valueText}"
          }
          .mkString("\n")
        println(params)
        s"""
          |Params:
          |$paramsText
          |Query:
          |$text
          |""".stripMargin
      }
  }

}
