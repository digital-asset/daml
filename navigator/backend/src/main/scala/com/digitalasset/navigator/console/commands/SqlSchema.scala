// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import com.daml.navigator.console._

case object SqlSchema extends SimpleCommand {
  def name: String = "sql_schema"

  def description: String = "Return the database schema"

  def params: List[Parameter] = List.empty

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    for {
      ps <- state.getPartyState ~> s"Unknown party ${state.party}"
      schema <- ps.ledger.databaseSchema() ~> s"Failed to return database schema"
    } yield {
      (state, schema)
    }
  }

}
