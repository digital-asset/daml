// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import com.daml.navigator.console._

case object Parties extends SimpleCommand {
  def name: String = "parties"

  def description: String = "List all parties available in Navigator"

  def params: List[Parameter] = List.empty

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    for {
      parties <- state.getParties ~> "Application not connected"
    } yield (state, parties.values.map(ps => ps.name).mkString("\n"))
  }

}
