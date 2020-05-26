// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import com.daml.navigator.console
import com.daml.navigator.console._
import org.jline.reader.Completer

@SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
case object Help extends Command {
  def name: String = "help"

  def description: String = "Print help"

  private def param(set: CommandSet) =
    ParameterLiterals("command", set.commands.map(_.name).filter(s => s != name))

  def usage(args: List[String], set: CommandSet): String =
    console.Command.printUsage(name, description, List(param(set)))

  def completer(state: State, set: CommandSet): Completer =
    Parameter.completer(state, List(ParameterLiteral(name), param(set)))

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    args match {
      case Nil => Right((state, set.usage()))
      case "" :: _ => Right((state, set.usage()))
      case cmdName :: cmdArgs =>
        set.commandsByName.get(cmdName) match {
          case Some(cmd) => Right((state, cmd.usage(cmdArgs, set)))
          case None => Left(CommandError(s"Unknown command '$cmdName'.", None))
        }
    }
  }
}
