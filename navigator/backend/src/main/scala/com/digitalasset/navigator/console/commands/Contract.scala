// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.console.commands

import com.digitalasset.ledger.api.refinements.ApiTypes
import com.digitalasset.navigator.console._
import com.digitalasset.navigator.json.ApiCodecCompressed
import com.digitalasset.navigator.model

@SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
case object Contract extends SimpleCommand {
  def name: String = "contract"

  def description: String = "Print contract details"

  def params: List[Parameter] = List(
    ParameterContractId("id", "Contract ID")
  )

  private def prettyContract(
      c: model.Contract,
      createEv: model.Event,
      archiveEv: Option[model.Event],
      exeEv: List[model.ChoiceExercised]): PrettyObject = {
    PrettyObject(
      PrettyField("Id", ApiTypes.ContractId.unwrap(c.id)),
      PrettyField("TemplateId", c.template.idString),
      PrettyField("Argument", Pretty.argument(c.argument)),
      PrettyField("ArgumentJson", ApiCodecCompressed.apiValueToJsValue(c.argument).compactPrint),
      PrettyField(
        "Created",
        PrettyObject(
          PrettyField("EventId", ApiTypes.EventId.unwrap(createEv.id)),
          PrettyField("TransactionId", ApiTypes.TransactionId.unwrap(createEv.transactionId)),
          PrettyField("WorkflowId", ApiTypes.WorkflowId.unwrap(createEv.workflowId))
        )
      ),
      PrettyField(
        "Archived",
        archiveEv
          .map(e =>
            PrettyObject(
              PrettyField("EventId", ApiTypes.EventId.unwrap(e.id)),
              PrettyField("TransactionId", ApiTypes.TransactionId.unwrap(e.transactionId)),
              PrettyField("WorkflowId", ApiTypes.WorkflowId.unwrap(e.workflowId))
          ))
          .getOrElse(PrettyPrimitive("Contract is active"))
      ),
      PrettyField(
        "Exercise events",
        PrettyArray(exeEv.map(e => PrettyPrimitive(ApiTypes.EventId.unwrap(e.id)))))
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
      contract <- ps.ledger
        .contract(ApiTypes.ContractId(arg1), types) ~> s"Contract '$arg1' not found"
      createEv <- ps.ledger
        .createEventOf(contract, types) ~> s"Failed to load create event for contract '$arg1'"
      archiveEv <- Right(ps.ledger.archiveEventOf(contract, types))
      exeEv <- Right(ps.ledger.exercisedEventsOf(contract, types))
    } yield {
      (state, Pretty.yaml(prettyContract(contract, createEv, archiveEv, exeEv)))
    }
  }

}
