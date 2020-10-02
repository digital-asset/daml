// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import com.daml.navigator.console._

import scala.util.Try

case object Packages extends SimpleCommand {
  def name: String = "packages"

  def description: String = "List all DAML-LF packages"

  def params: List[Parameter] = List.empty

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    for {
      ps <- state.getPartyState ~> s"Unknown party ${state.party}"
      packs <- Try(ps.packageRegistry.allPackages()) ~> "Failed to get packages"
    } yield {
      val header = List(
        "Hash",
        "Templates",
        "TypeDefs"
      )
      val data = packs.map(
        p =>
          List(
            p.id,
            p.templates.size.toString,
            p.typeDefs.size.toString
        ))
      (state, Pretty.asciiTable(state, header, data))
    }
  }

}
