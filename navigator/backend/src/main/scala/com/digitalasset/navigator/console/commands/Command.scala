// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.console._
import com.daml.navigator.model

case object Command extends SimpleCommand {
  def name: String = "command"

  def description: String = "Print command details"

  def params: List[Parameter] = List(
    ParameterCommandId("id", "Command ID")
  )

  private def prettyCommand(cmd: model.Command): PrettyObject = {
    cmd match {
      case c: model.CreateCommand =>
        import model._
        PrettyObject(
          PrettyField("Id", ApiTypes.CommandId.unwrap(c.id)),
          PrettyField("WorkflowId", ApiTypes.WorkflowId.unwrap(c.workflowId)),
          PrettyField("PlatformTime", Pretty.prettyInstant(c.platformTime)),
          PrettyField("Command", "Create contract"),
          PrettyField("Template", c.template.asOpaqueString),
          PrettyField("Argument", Pretty.argument(c.argument))
        )
      case c: model.ExerciseCommand =>
        PrettyObject(
          PrettyField("Id", ApiTypes.CommandId.unwrap(c.id)),
          PrettyField("WorkflowId", ApiTypes.WorkflowId.unwrap(c.workflowId)),
          PrettyField("PlatformTime", Pretty.prettyInstant(c.platformTime)),
          PrettyField("Command", "Exercise choice"),
          PrettyField("Contract", ApiTypes.ContractId.unwrap(c.contract)),
          PrettyField("Choice", ApiTypes.Choice.unwrap(c.choice)),
          PrettyField("Argument", Pretty.argument(c.argument))
        )
    }
  }

  private def prettyStatus(status: model.CommandStatus): PrettyObject = {
    status match {
      case s: model.CommandStatusWaiting =>
        PrettyObject(
          PrettyField("Status", "Waiting")
        )
      case s: model.CommandStatusError =>
        PrettyObject(
          PrettyField("Status", "Error"),
          PrettyField("Code", s.code),
          PrettyField("Details", s.details)
        )
      case s: model.CommandStatusSuccess =>
        PrettyObject(
          PrettyField("Status", "Success"),
          PrettyField("TransactionId", ApiTypes.TransactionId.unwrap(s.tx.id))
        )
      case s: model.CommandStatusUnknown =>
        PrettyObject(
          PrettyField("Status", "Unknown (command tracking failed)")
        )
    }
  }

  private def prettyCommandWithStatus(c: model.Command, s: model.CommandStatus): PrettyObject = {
    PrettyObject(
      PrettyField("Command", prettyCommand(c)),
      PrettyField("Status", prettyStatus(s))
    )
  }

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    for {
      arg1 <- args.headOption ~> "Missing <id> argument"
      ps <- state.getPartyState ~> s"Unknown party ${state.party}"
      types = ps.packageRegistry
      command <- ps.ledger.command(ApiTypes.CommandId(arg1), types) ~> s"Command '$arg1' not found"
      status <- ps.ledger
        .commandStatus(ApiTypes.CommandId(arg1), types) ~> s"Command status '$arg1' not found"
    } yield {
      (state, Pretty.yaml(prettyCommandWithStatus(command, status)))
    }
  }

}
