// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console

import org.jline.reader.Completer
import org.jline.reader.impl.completer.AggregateCompleter

import scala.collection.JavaConverters._

final case class CommandSet(
    commands: List[Command]
) {
  val commandsByName: Map[String, Command] = commands.map(cmd => (cmd.name -> cmd)).toMap

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def usage(): String = {
    val width = 20
    val cmds = commands
      .map(cmd => {
        val help: String = cmd.description
        f"  ${cmd.name.formatted(s"%-${width}s")} $help"
      })
    "Available commands:\n" + cmds.mkString("\n")
  }

  def completer(state: State): Completer =
    new AggregateCompleter(commands.map(_.completer(state, this)).asJava)

  def eval(
      state: State,
      cmdS: String,
      args: List[String]): Either[CommandError, (State, String)] = {
    commandsByName.get(cmdS) match {
      case Some(cmd) => cmd.eval(state, args, this)
      case None => Left(CommandError(s"Unknown command '$cmdS'.", None))
    }
  }
}
