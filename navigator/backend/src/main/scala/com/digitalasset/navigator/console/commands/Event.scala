// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.console.commands

import com.digitalasset.ledger.api.refinements.ApiTypes
import com.digitalasset.navigator.console._
import com.digitalasset.navigator.json.ApiCodecCompressed
import com.digitalasset.navigator.model

case object Event extends SimpleCommand {
  def name: String = "event"

  def description: String = "Print event details"

  def params: List[Parameter] = List(
    ParameterEventId("id", "Event ID")
  )

  private def prettyEvent(event: model.Event, tx: model.Transaction): PrettyObject = event match {
    case ev: model.ContractCreated =>
      PrettyObject(
        PrettyField("Id", ApiTypes.EventId.unwrap(ev.id)),
        PrettyField("ParentId", ev.parentId.map(ApiTypes.EventId.unwrap).getOrElse("???")),
        PrettyField("TransactionId", ApiTypes.TransactionId.unwrap(ev.transactionId)),
        PrettyField("WorkflowId", ApiTypes.WorkflowId.unwrap(ev.workflowId)),
        PrettyField(
          "Witnesses",
          PrettyArray(ApiTypes.Party.unsubst(ev.witnessParties).map(PrettyPrimitive))),
        PrettyField("Type", "Created"),
        PrettyField("Contract", ApiTypes.ContractId.unwrap(ev.contractId)),
        PrettyField("Template", Pretty.shortTemplateId(ev.templateId)),
        PrettyField("Argument", Pretty.argument(ev.argument)),
        PrettyField("ArgumentJson", ApiCodecCompressed.apiValueToJsValue(ev.argument).compactPrint),
      )
    case ev: model.ChoiceExercised =>
      PrettyObject(
        PrettyField("Id", ApiTypes.EventId.unwrap(ev.id)),
        PrettyField("ParentId", ev.parentId.map(ApiTypes.EventId.unwrap).getOrElse("???")),
        PrettyField("TransactionId", ApiTypes.TransactionId.unwrap(ev.transactionId)),
        PrettyField("WorkflowId", ApiTypes.WorkflowId.unwrap(ev.workflowId)),
        PrettyField(
          "Witnesses",
          PrettyArray(ApiTypes.Party.unsubst(ev.witnessParties).map(PrettyPrimitive))),
        PrettyField("Type", "Created"),
        PrettyField("Contract", ApiTypes.ContractId.unwrap(ev.contractId)),
        PrettyField("Choice", ApiTypes.Choice.unwrap(ev.choice)),
        PrettyField("Argument", Pretty.argument(ev.argument)),
        PrettyField("ArgumentJson", ApiCodecCompressed.apiValueToJsValue(ev.argument).compactPrint),
        PrettyField(
          "Children",
          PrettyArray(tx.events.filter(_.parentId.contains(ev.id)).map(prettyEvent(_, tx))))
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
      event <- ps.ledger.event(ApiTypes.EventId(arg1), types) ~> s"Event '$arg1' not found"
      tx <- ps.ledger
        .transaction(event.transactionId, types) ~> s"Transaction '${event.transactionId}' not found"
    } yield {
      (state, Pretty.yaml(prettyEvent(event, tx)))
    }
  }

}
