// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import scala.annotation.nowarn

object Pretty {

  private[Pretty] final case class Tree(label: String, children: Seq[Tree]) {
    def pretty(level: Int): String = {
      val padding = "  " * level
      val prettyChildren = children.map(_.pretty(level + 1)).mkString("")
      s"$padding$label\n$prettyChildren"
    }
  }

  object PrettyLedgers {
    import Ledgers._

    def scenarioToTree(scenario: Scenario): Tree = {
      Tree("Scenario", Seq(topologyToTree(scenario.topology), ledgerToTree(scenario.ledger)))
    }

    def topologyToTree(topology: Topology): Tree = {
      Tree("Topology", topology.map(participantToTree))
    }

    def participantToTree(participant: Participant): Tree = {
      Tree(
        s"Participant ${participant.participantId} parties=${prettyParties(participant.parties)}",
        Seq.empty,
      )
    }

    def ledgerToTree(ledger: Ledger): Tree = {
      Tree("Ledger", ledger.map(commandsToTree))
    }

    private def commandsToTree(commands: Commands): Tree =
      Tree(
        s"Commands participant=${commands.participantId} actAs=${prettyParties(commands.actAs)} disclosures=${prettyContracts(commands.disclosures)}",
        commands.actions.map(actionToTree),
      )

    private def actionToTree(action: Action): Tree = action match {
      case Create(contractId, signatories, observers) =>
        Tree(
          s"Create ${contractId} sigs=${prettyParties(signatories)} obs=${prettyParties(observers)}",
          Nil,
        )
      case CreateWithKey(contractId, keyId, maintainers, signatories, observers) =>
        Tree(
          s"CreateWithKey ${contractId} key=(${keyId}, ${prettyParties(
              maintainers
            )}) sigs=${prettyParties(signatories)} obs=${prettyParties(observers)}",
          Nil,
        )
      case Exercise(kind, contractId, controllers, choiceObservers, subTransaction) =>
        Tree(
          s"Exercise $kind $contractId ctl=${prettyParties(controllers)} cobs=${prettyParties(choiceObservers)}",
          subTransaction.map(actionToTree),
        )
      case ExerciseByKey(
            kind,
            contractId,
            _,
            _,
            controllers,
            choiceObservers,
            subTransaction,
          ) =>
        Tree(
          s"ExerciseByKey $kind $contractId ctl=${prettyParties(controllers)} cobs=${prettyParties(choiceObservers)}",
          subTransaction.map(actionToTree),
        )
      case Fetch(contractId) =>
        Tree(s"Fetch $contractId", Nil)
      case FetchByKey(contractId, _, _) =>
        Tree(s"FetchByKey $contractId", Nil)
      case LookupByKey(contractId, keyId, maintainers) =>
        contractId match {
          case Some(cid) =>
            Tree(s"LookupByKey success $cid", Nil)
          case None =>
            Tree(s"LookupByKey failure key=($keyId, ${prettyParties(maintainers)})", Nil)
        }
      case Rollback(subTransaction) =>
        Tree(s"Rollback", subTransaction.map(actionToTree))
    }

    private def prettyParties(partySet: PartySet): String =
      partySet.toSeq.sorted.mkString("{", ",", "}")

    private def prettyContracts(contractIdSet: ContractIdSet): String =
      contractIdSet.toSeq.sorted.mkString("{", ",", "}")
  }

  object PrettySkeletons {

    import Skeletons._

    def scenarioToTree(scenario: Scenario): Tree = {
      Tree("Scenario", Seq(topologyToTree(scenario.topology), ledgerToTree(scenario.ledger)))
    }

    def topologyToTree(topology: Topology): Tree = {
      Tree("Topology", topology.map(participantToTree))
    }

    @nowarn("cat=unused")
    def participantToTree(participant: Participant): Tree = {
      Tree("Participant", Seq.empty)
    }

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
      case CreateWithKey() =>
        Tree(
          "CreateWithKey",
          Nil,
        )
      case Exercise(kind, subTransaction) =>
        Tree(
          s"Exercise $kind",
          subTransaction.map(actionToTree),
        )
      case ExerciseByKey(kind, subTransaction) =>
        Tree(
          s"ExerciseByKey $kind",
          subTransaction.map(actionToTree),
        )
      case Fetch() =>
        Tree("Fetch", Nil)
      case FetchByKey() =>
        Tree("FetchByKey", Nil)
      case LookupByKey(successful) =>
        Tree(s"LookupByKey ${if (successful) "success" else "failure"}", Nil)
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
      partySet.toSeq.sorted.mkString("{", ",", "}")
  }

  object PrettySymbolic {
    import Symbolic._

    def scenarioToTree(scenario: Scenario): Tree = {
      Tree("Scenario", Seq(topologyToTree(scenario.topology), ledgerToTree(scenario.ledger)))
    }

    def topologyToTree(topology: Topology): Tree = {
      Tree("Topology", topology.map(participantToTree))
    }

    def participantToTree(participant: Participant): Tree = {
      Tree(
        s"Participant ${participant.participantId} parties=${participant.parties}",
        Seq.empty,
      )
    }

    def ledgerToTree(ledger: Ledger): Tree = {
      Tree("Ledger", ledger.map(commandsToTree))
    }

    private def commandsToTree(commands: Commands): Tree =
      Tree(
        s"Commands participant=${commands.participantId} actAs=${commands.actAs} disclosures=${commands.disclosures}",
        commands.actions.map(actionToTree),
      )

    private def actionToTree(action: Action): Tree = action match {
      case Create(contractId, signatories, observers) =>
        Tree(
          s"Create ${contractId} sigs=${signatories} obs=${observers}",
          Nil,
        )
      case CreateWithKey(contractId, keyId, maintainers, signatories, observers) =>
        Tree(
          s"CreateWithKey ${contractId} key=(${keyId}, ${maintainers}) sigs=${signatories} obs=${observers}",
          Nil,
        )
      case Exercise(kind, contractId, controllers, choiceObservers, subTransaction) =>
        Tree(
          s"Exercise $kind $contractId ctl=${controllers} cobs=${choiceObservers}",
          subTransaction.map(actionToTree),
        )
      case ExerciseByKey(
            kind,
            contractId,
            keyId,
            maintainers,
            controllers,
            choiceObservers,
            subTransaction,
          ) =>
        Tree(
          s"ExerciseByKey $kind $contractId key=(${keyId}, ${maintainers}) ctl=${controllers} cobs=${choiceObservers}",
          subTransaction.map(actionToTree),
        )
      case Fetch(contractId) =>
        Tree(s"Fetch $contractId", Nil)
      case FetchByKey(contractId, keyId, maintainers) =>
        Tree(s"FetchByKey $contractId key=($keyId, ${maintainers})", Nil)
      case LookupByKey(contractId, keyId, maintainers) =>
        contractId match {
          case Some(cid) =>
            Tree(s"LookupByKey success $cid", Nil)
          case None =>
            Tree(s"LookupByKey failure key=($keyId, ${maintainers})", Nil)
        }
      case Rollback(subTransaction) =>
        Tree(s"Rollback", subTransaction.map(actionToTree))
    }
  }

  def prettyScenario(scenarion: Ledgers.Scenario): String =
    PrettyLedgers.scenarioToTree(scenarion).pretty(0)

  def prettyLedger(ledger: Ledgers.Ledger): String =
    PrettyLedgers.ledgerToTree(ledger).pretty(0)

  def prettyScenarioSkeleton(scenario: Skeletons.Scenario): String =
    PrettySkeletons.scenarioToTree(scenario).pretty(0)

  def prettySkeletonLedger(ledger: Skeletons.Ledger): String =
    PrettySkeletons.ledgerToTree(ledger).pretty(0)

  def prettySymScenario(scenario: Symbolic.Scenario): String =
    PrettySymbolic.scenarioToTree(scenario).pretty(0)

  def prettySymLedger(ledger: Symbolic.Ledger): String =
    PrettySymbolic.ledgerToTree(ledger).pretty(0)

  def prettyProjection(projection: Projections.Projection): String =
    PrettyProjections.projectionToTree(projection).pretty(0)
}
