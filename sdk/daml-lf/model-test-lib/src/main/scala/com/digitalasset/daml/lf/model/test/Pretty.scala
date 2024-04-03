// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

object Pretty {
  import com.daml.lf.model.test.Ledgers._

  private final case class Tree(label: String, children: List[Tree]) {
    def pretty(level: Int): String = {
      val padding = "  " * level
      val prettyChildren = children.map(_.pretty(level + 1)).mkString("")
      s"$padding$label\n$prettyChildren"
    }
  }

  private def prettyParties(partySet: PartySet): String =
    partySet.mkString("{", ",", "}")

  private def ledgerToTree(ledger: Ledger): Tree =
    Tree("Ledger", ledger.map(commandsToTree))

  private def commandsToTree(commands: Commands): Tree =
    Tree(s"Commands actAs=${prettyParties(commands.actAs)}", commands.actions.map(actionToTree))

  private def actionToTree(action: Action): Tree = action match {
    case Create(contractId, signatories, observers) =>
      Tree(
        s"Create ${contractId} sigs=${prettyParties(signatories)} obs=${prettyParties(observers)}",
        Nil,
      )
    case Exercise(kind, contractId, controllers, choiceObservers, subTransaction) =>
      Tree(
        s"Exercise $kind $contractId ctl=${prettyParties(controllers)} cobs=${prettyParties(choiceObservers)}",
        subTransaction.map(actionToTree),
      )
    case Fetch(contractId) =>
      Tree(s"Fetch $contractId", Nil)
    case Rollback(subTransaction) =>
      Tree(s"Rollback", subTransaction.map(actionToTree))
  }

  def prettyLedger(ledger: Ledger): String =
    ledgerToTree(ledger).pretty(0)
}
