// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.lf.model.test.Ledgers
import com.daml.lf.model.test.Projections

object Pretty {

  private[Pretty] final case class Tree(label: String, children: List[Tree]) {
    def pretty(level: Int): String = {
      val padding = "  " * level
      val prettyChildren = children.map(_.pretty(level + 1)).mkString("")
      s"$padding$label\n$prettyChildren"
    }
  }

  object PrettyLedgers {
    import Ledgers._

    def ledgerToTree(ledger: Ledger): Tree = {
      Tree("Ledger", ledger.map(commandsToTree))
    }

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

    private def prettyParties(partySet: PartySet): String =
      partySet.mkString("{", ",", "}")
  }

  object PrettySkeletons {

    import Skeletons._

    def ledgerToTree(ledger: Ledger): Tree = {
      Tree("Ledger", ledger.map(commandsToTree))
    }

    private def commandsToTree(commands: Commands): Tree =
      Tree("Commands", commands.actions.map(actionToTree))

    private def actionToTree(action: Action): Tree = action match {
      case Create() =>
        Tree(
          "Create",
          Nil,
        )
      case Exercise(kind, subTransaction) =>
        Tree(
          s"Exercise $kind",
          subTransaction.map(actionToTree),
        )
      case Fetch() =>
        Tree("Fetch", Nil)
      case Rollback(subTransaction) =>
        Tree(s"Rollback", subTransaction.map(actionToTree))
    }
  }

  object PrettyProjections {
    import Projections._

    def projectionToTree(projection: Projection): Tree = {
      Tree("Projection", projection.map(commandsToTree))
    }

    private def commandsToTree(commands: Commands): Tree =
      Tree(s"Commands", commands.actions.map(actionToTree))

    private def actionToTree(action: Action): Tree = action match {
      case Create(contractId, signatories) =>
        Tree(
          s"Create ${contractId} sigs=${prettyParties(signatories)}",
          Nil,
        )
      case Exercise(kind, contractId, controllers, subTransaction) =>
        Tree(
          s"Exercise $kind $contractId ctl=${prettyParties(controllers)}",
          subTransaction.map(actionToTree),
        )
    }

    private def prettyParties(partySet: PartySet): String =
      partySet.mkString("{", ",", "}")
  }

  object PrettySymbolic {
    import Symbolic._

    def ledgerToTree(ledger: Ledger): Tree = {
      Tree("Ledger", ledger.map(commandsToTree))
    }

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

    private def prettyParties(partySet: PartySet): String = partySet.toString
  }

  def prettyLedger(ledger: Ledgers.Ledger): String =
    PrettyLedgers.ledgerToTree(ledger).pretty(0)

  def prettySkeleton(ledger: Skeletons.Ledger): String =
    PrettySkeletons.ledgerToTree(ledger).pretty(0)

  def prettySymbolic(ledger: Symbolic.Ledger): String =
    PrettySymbolic.ledgerToTree(ledger).pretty(0)

  def prettyProjection(projection: Projections.Projection): String =
    PrettyProjections.projectionToTree(projection).pretty(0)
}
