// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import com.daml.navigator.console._
import com.daml.navigator.model
import com.daml.ledger.api.refinements.ApiTypes

import scala.util.Try

case object Commands extends SimpleCommand {
  def name: String = "commands"

  def description: String = "List submitted commands"

  def params: List[Parameter] = List.empty

  private def prettyStatus(status: model.CommandStatus): String = {
    status match {
      case s: model.CommandStatusWaiting => "Waiting"
      case s: model.CommandStatusError => s"Error: ${Pretty.abbreviate(s.details, 30)}"
      case s: model.CommandStatusSuccess =>
        s"Success: Transaction ${ApiTypes.TransactionId.unwrap(s.tx.id)}"
      case s: model.CommandStatusUnknown => "Unknown: Command tracking failed"
    }
  }

  private def prettyStatus(status: Option[model.CommandStatus]): String = {
    status.map(s => prettyStatus(s)).getOrElse("Unknown: Status not set")
  }

  private def prettyCommand(cmd: model.Command): String = {
    cmd match {
      case c: model.CreateCommand => s"Create ${Pretty.shortTemplateId(c.template)}"
      case c: model.ExerciseCommand =>
        s"Exercise ${c.choice} on ${ApiTypes.ContractId.unwrap(c.contract)}"
    }
  }

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    for {
      ps <- state.getPartyState ~> s"Unknown party ${state.party}"
      types = ps.packageRegistry
      commands <- Try(ps.ledger.allCommands(types).take(100).toList) ~> s"Could not get commands"
      tuples <- Try(commands.map(cmd => (cmd, ps.ledger.commandStatus(cmd.id, types)))) ~> s"Could not get command statuses"
    } yield {
      val header = List(
        "ID",
        "Command",
        "Status"
      )
      val data = tuples.map(
        t =>
          List(
            ApiTypes.CommandId.unwrap(t._1.id),
            prettyCommand(t._1),
            prettyStatus(t._2)
        ))
      (state, Pretty.asciiTable(state, header, data))
    }
  }

}
