// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import com.daml.navigator.console._

import scala.util.Try

case object Templates extends SimpleCommand {
  def name: String = "templates"

  def description: String = "List all templates"

  def params: List[Parameter] = List.empty

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    for {
      ps <- state.getPartyState ~> s"Unknown party ${state.party}"
      templates <- Try(ps.packageRegistry.allTemplates().sortBy(_.topLevelDecl)) ~> "Failed to get templates"
    } yield {
      val header = List(
        "Name",
        "Package",
        "Choices",
        "Parameter size"
      )
      val data = templates.map(
        t =>
          List(
            t.topLevelDecl,
            t.id.packageId.take(8),
            t.choices.length.toString,
            Pretty
              .yaml(Pretty.damlLfDefDataType(t.id, ps.packageRegistry.damlLfDefDataType))
              .length
              .toString
        ))
      (state, Pretty.asciiTable(state, header, data))
    }
  }

}
