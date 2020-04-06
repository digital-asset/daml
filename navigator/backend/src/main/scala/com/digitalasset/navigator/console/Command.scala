// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console

import org.jline.reader.Completer

trait Command {
  def name: String

  /** A sentence of two about what it does */
  def description: String

  /** Print some help text */
  def usage(args: List[String], set: CommandSet): String

  /** Create a completer (for tab completion) */
  def completer(s: State, set: CommandSet): Completer

  /** Action */
  def eval(state: State, args: List[String], set: CommandSet): Either[CommandError, (State, String)]
}

/** A command with a static list of parameters. Comes with a default help text and completer implementation. */
trait SimpleCommand extends Command {

  /** List of parameters this command takes */
  def params: List[Parameter]

  override def usage(args: List[String], set: CommandSet): String =
    Command.printUsage(name, description, params)

  override def completer(state: State, set: CommandSet): Completer =
    Command.completer(state, name, params)
}

object Command {
  def printUsage(name: String, description: String, params: List[Parameter]): String = {
    val header = s"""Usage: $name ${params.map(_.paramName).mkString(" ")}
      |
      |$description""".stripMargin
    val nonLiterals = params.filter(w => !Parameter.isLiteral(w))
    if (nonLiterals.nonEmpty) {
      header + "\n" + printParams(nonLiterals)
    } else {
      header
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def printParams(params: List[Parameter]): String = {
    val width = 20
    val lines = params
      .map(param => {
        val help: String = param.description
        val name: String = param.paramName
        f"  ${name.formatted(s"%-${width}s")} $help"
      })
    "Parameters:\n" + lines.mkString("\n")
  }

  /** Create a default completer for a command of the given name and parameter list */
  def completer(state: State, name: String, params: List[Parameter]): Completer =
    Parameter.completer(state, ParameterLiteral(name) :: params)

}
