// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.console.commands

import com.digitalasset.ledger.api.refinements.ApiTypes
import com.digitalasset.navigator.console._
import com.digitalasset.navigator.model
import com.digitalasset.navigator.model.TemplateStringId

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
