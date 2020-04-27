// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import java.util.concurrent.TimeUnit

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.console._
import com.daml.lf.value.json.ApiCodecCompressed
import com.daml.navigator.model
import com.daml.navigator.store.Store.CreateContract
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Try

@SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
case object Create extends SimpleCommand {
  def name: String = "create"

  def description: String = "Create a contract"

  def params: List[Parameter] = List(
    ParameterTemplateId("template", "Template ID"),
    ParameterLiteral("with"),
    ParameterDamlValue("argument", "Contract argument")
  )

  def sendCommand(
      state: State,
      ps: model.PartyState,
      template: String,
      arg: model.ApiRecord): Future[ApiTypes.CommandId] = {
    implicit val actorTimeout: Timeout = Timeout(20, TimeUnit.SECONDS)
    implicit val executionContext: ExecutionContext = state.ec

    val command = CreateContract(
      ps,
      model.TemplateStringId(template),
      arg
    )
    (state.store ? command)
      .mapTo[Try[ApiTypes.CommandId]]
      .map(c => c.get)
  }

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    args match {
      case templateName :: w :: damlA if w.equalsIgnoreCase("with") =>
        for {
          ps <- state.getPartyState ~> s"Unknown party ${state.party}"
          templateId <- model.parseOpaqueIdentifier(templateName) ~> s"Unknown template $templateName"
          apiValue <- Try(
            ApiCodecCompressed.stringToApiType(
              damlA.mkString(" "),
              templateId,
              ps.packageRegistry.damlLfDefDataType _)) ~> "Failed to parse DAML value"
          apiRecord <- Try(apiValue.asInstanceOf[model.ApiRecord]) ~> "Record argument required"
          future <- Try(sendCommand(state, ps, templateName, apiRecord)) ~> "Failed to create contract"
          commandId <- Try(Await.result(future, 30.seconds)) ~> "Failed to create contract"
        } yield {
          (state, Pretty.yaml(Pretty.commandResult(ps, commandId)))
        }
      case _ =>
        Left(CommandError("Invalid syntax", None))
    }
  }

}
