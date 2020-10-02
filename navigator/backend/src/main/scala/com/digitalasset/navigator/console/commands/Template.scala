// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.console._
import com.daml.navigator.model
import com.daml.navigator.model.TemplateStringId

case object Template extends SimpleCommand {
  def name: String = "template"

  def description: String = "Print template details"

  def params: List[Parameter] = List(
    ParameterTemplateId("id", "Template ID")
  )

  private def prettyTemplate(p: model.PackageRegistry, t: model.Template): PrettyObject = {
    PrettyObject(
      PrettyField("Name", t.topLevelDecl),
      PrettyField("Parameter", Pretty.damlLfDefDataType(t.id, p.damlLfDefDataType)),
      PrettyField(
        "Choices",
        PrettyArray(t.choices.map(c => PrettyPrimitive(ApiTypes.Choice.unwrap(c.name)))))
    )
  }

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    for {
      arg1 <- args.headOption ~> "Missing <id> argument"
      ps <- state.getPartyState ~> s"Unknown party ${state.party}"
      template <- ps.packageRegistry.templateByStringId(TemplateStringId(arg1)) ~> s"Unknown template $arg1"
    } yield {
      (state, Pretty.yaml(prettyTemplate(ps.packageRegistry, template)))
    }
  }

}
