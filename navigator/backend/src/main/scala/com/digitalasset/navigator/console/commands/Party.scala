// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import com.daml.navigator.console._

import com.daml.ledger.api.refinements.ApiTypes

case object Party extends SimpleCommand {
  def name: String = "party"

  def description: String = "Set the current party"

  def params: List[Parameter] = List(
    ParameterParty("party", "Name of the party to use")
  )

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    for {
      arg1 <- args.headOption ~> "Missing <party> argument"
      newParty <- state.config.parties
        .find(ps => ApiTypes.Party.unwrap(ps.name) == arg1) ~> s"Unknown party $arg1"
    } yield {
      (state.copy(party = newParty.name), s"Active party set to $newParty")
    }
  }
}
