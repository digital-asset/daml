// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.console.Pretty.prettyInstant
import com.daml.navigator.console._
import com.daml.navigator.model

@SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
case object Transaction extends SimpleCommand {
  def name: String = "transaction"

  def description: String = "Print transaction details"

  def params: List[Parameter] = List(
    ParameterTransactionId("id", "Transaction ID")
  )

  private def prettyTransaction(tx: model.Transaction): PrettyObject = {
    def eventsToYaml(events: List[model.Event]): PrettyNode =
      PrettyArray(events.map({
        case ev: model.ContractCreated =>
          PrettyPrimitive(s"[${ApiTypes.EventId.unwrap(ev.id)}] Created ${ev.contractId} as ${Pretty
            .shortTemplateId(ev.templateId)}")
        case ev: model.ChoiceExercised =>
          PrettyObject(
            PrettyField(
              s"[${ApiTypes.EventId.unwrap(ev.id)}] Exercised ${ev.choice} on ${ev.contractId}",
              eventsToYaml(tx.events.filter(_.parentId.contains(ev.id))))
          )
      }))
    PrettyObject(
      PrettyField("Offset", PrettyPrimitive(tx.offset)),
      PrettyField("Effective at", PrettyPrimitive(prettyInstant(tx.effectiveAt))),
      PrettyField(
        "Command ID",
        PrettyPrimitive(ApiTypes.CommandId.unsubst(tx.commandId).getOrElse("???"))),
      PrettyField("Events", eventsToYaml(tx.events))
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
      tx <- ps.ledger
        .transaction(ApiTypes.TransactionId(arg1), types) ~> s"Transaction '$arg1' not found"
    } yield {
      (state, Pretty.yaml(prettyTransaction(tx)))
    }
  }

}
