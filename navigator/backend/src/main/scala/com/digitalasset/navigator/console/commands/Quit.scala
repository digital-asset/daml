// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import com.daml.navigator.console._

case object Quit extends SimpleCommand {
  def name: String = "quit"

  def description: String = "Quit the application"

  def params: List[Parameter] = List.empty

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    Right((state.copy(quit = true), "Bye."))
  }
}
