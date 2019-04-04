// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.console.commands

import com.digitalasset.navigator.console._

case object Parties extends SimpleCommand {
  def name: String = "parties"

  def description: String = "List all parties available in Navigator"

  def params: List[Parameter] = List.empty

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    Right((state, state.config.parties.map(p => p.name).mkString("\n")))
  }

}
