// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.console._
import com.daml.navigator.model
import com.daml.navigator.model.TemplateStringId

@SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
case object Choice extends SimpleCommand {
  def name: String = "choice"

  def description: String = "Print choice details"

  def params: List[Parameter] = List(
    ParameterTemplateId("template", "Template ID"),
    ParameterChoiceId("choice", "Choice name")
  )

  private def prettyChoice(
      p: model.PackageRegistry,
      t: model.Template,
      c: model.Choice): PrettyObject = {
    PrettyObject(
      PrettyField("Name", ApiTypes.Choice.unwrap(c.name)),
      PrettyField("Consuming", c.consuming.toString),
      PrettyField("Parameter", Pretty.damlLfType(c.parameter, p.damlLfDefDataType)._2)
    )
  }

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    for {
      arg1 <- args.headOption ~> "Missing <template> argument"
      arg2 <- args.drop(1).headOption ~> "Missing <choice> argument"
      ps <- state.getPartyState ~> s"Unknown party ${state.party}"
      template <- ps.packageRegistry.templateByStringId(TemplateStringId(arg1)) ~> s"Unknown template $arg1"
      choice <- template.choices.find(c => ApiTypes.Choice.unwrap(c.name) == arg2) ~> s"Unknown choice $arg2"
    } yield {
      (state, Pretty.yaml(prettyChoice(ps.packageRegistry, template, choice)))
    }
  }

}
