// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import java.util.concurrent.TimeUnit

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.console._
import com.daml.lf.value.Value.ValueUnit
import com.daml.lf.value.json.ApiCodecCompressed
import com.daml.navigator.model
import com.daml.navigator.store.Store.ExerciseChoice
import akka.pattern.ask
import akka.util.Timeout
import com.daml.navigator.model.ApiValue

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Try

@SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
case object Exercise extends SimpleCommand {
  def name: String = "exercise"

  def description: String = "Exercises a choice"

  def params: List[Parameter] = List(
    ParameterContractId("contract", "Contract ID"),
    ParameterChoiceId("choice", "Name of the choice"),
    ParameterLiteral("with"),
    ParameterDamlValue("argument", "Choice argument")
  )

  def sendCommand(
      state: State,
      ps: model.PartyState,
      contract: String,
      choice: String,
      arg: model.ApiValue): Future[ApiTypes.CommandId] = {
    implicit val actorTimeout: Timeout = Timeout(20, TimeUnit.SECONDS)
    implicit val executionContext: ExecutionContext = state.ec

    val command = ExerciseChoice(
      ps,
      ApiTypes.ContractId(contract),
      ApiTypes.Choice(choice),
      arg
    )
    (state.store ? command)
      .mapTo[Try[ApiTypes.CommandId]]
      .map(c => c.get)
  }

  def exerciseChoice(
      state: State,
      cid: String,
      choice: String,
      damlA: Option[List[String]]): Either[CommandError, (State, String)] = {
    for {
      ps <- state.getPartyState ~> s"Unknown party ${state.party}"
      types = ps.packageRegistry
      contract <- ps.ledger.contract(ApiTypes.ContractId(cid), types) ~> s"Unknown contract $cid"
      choiceType <- contract.template.choices
        .find(c => ApiTypes.Choice.unwrap(c.name) == choice) ~> s"Unknown choice $choice"
      apiValue <- Try(
        // Use unit value if no argument is given
        damlA.fold[ApiValue](ValueUnit)(
          arg =>
            ApiCodecCompressed.stringToApiType(
              arg.mkString(" "),
              choiceType.parameter,
              ps.packageRegistry.damlLfDefDataType _))) ~> "Failed to parse choice argument"
      future <- Try(sendCommand(state, ps, cid, choice, apiValue)) ~> "Failed to exercise choice"
      commandId <- Try(Await.result(future, 30.seconds)) ~> "Failed to exercise choice"
    } yield {
      (state, Pretty.yaml(Pretty.commandResult(ps, commandId)))
    }
  }

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    args match {
      case cid :: choice :: Nil =>
        exerciseChoice(state, cid, choice, None)
      case cid :: choice :: w :: damlA if w.equalsIgnoreCase("with") =>
        exerciseChoice(state, cid, choice, Some(damlA))
      case _ =>
        Left(CommandError("Invalid syntax", None))
    }
  }

}
