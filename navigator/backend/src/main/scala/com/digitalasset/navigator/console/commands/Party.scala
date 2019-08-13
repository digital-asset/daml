// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.console.commands

import com.digitalasset.navigator.console._

import com.digitalasset.ledger.api.refinements.ApiTypes

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
