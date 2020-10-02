// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import com.daml.navigator.console._

case object Sql extends SimpleCommand {
  def name: String = "sql"

  def description: String = "Execute a SQL query"

  def params: List[Parameter] = List(
    ParameterSQL("query", "The SQL query")
  )

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    val query = args.mkString(" ")
    for {
      ps <- state.getPartyState ~> s"Unknown party ${state.party}"
      result <- ps.ledger.runQuery(query) ~> s"Error while running the query"
    } yield {
      val width = state.reader.getTerminal.getWidth
      val table = AsciiTable()
        .width(if (width > 4) width else 80)
        .multiline(true)
        .columnMinWidth(4)
        .sampleAtMostRows(100000)
        .rowMaxHeight(500)
        .header(result.columnNames)
        .rows(result.rows)
        .toString
      (state, table)
    }
  }

}
