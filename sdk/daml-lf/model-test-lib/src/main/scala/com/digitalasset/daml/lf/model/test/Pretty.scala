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
        commands.commands.map(commandToTree),
      )

    private def commandToTree(command: Command): Tree = command match {
      case Command(pkgId, action) => actionToTree(action, pkgId)
    }

    private def actionToTree(action: Action, pkgId: Option[PackageId]): Tree = action match {
      case Create(contractId, signatories, observers) =>
        Tree(
          s"Create ${contractId}${prettyPackageId(pkgId)} sigs=${prettyParties(signatories)} obs=${prettyParties(observers)}",
          Nil,
        )
      case CreateWithKey(contractId, keyId, maintainers, signatories, observers) =>
        Tree(
          s"CreateWithKey ${contractId}${prettyPackageId(pkgId)} key=(${keyId}, ${prettyParties(
              maintainers
            )}) sigs=${prettyParties(signatories)} obs=${prettyParties(observers)}",
          Nil,
        )
      case Exercise(kind, contractId, controllers, choiceObservers, subTransaction) =>
        Tree(
          s"Exercise $kind $contractId${prettyPackageId(pkgId)} ctl=${prettyParties(controllers)} cobs=${prettyParties(choiceObservers)}",
          subTransaction.map(actionToTree(_, None)),
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
          s"ExerciseByKey $kind $contractId${prettyPackageId(pkgId)} ctl=${prettyParties(controllers)} cobs=${prettyParties(choiceObservers)}",
          subTransaction.map(actionToTree(_, None)),
        )
      case Fetch(contractId) =>
        Tree(s"Fetch ${contractId}${prettyPackageId(pkgId)}", Nil)
      case FetchByKey(contractId, _, _) =>
        Tree(s"FetchByKey ${contractId}${prettyPackageId(pkgId)}", Nil)
      case LookupByKey(contractId, keyId, maintainers) =>
        contractId match {
          case Some(cid) =>
            Tree(s"LookupByKey success ${cid}${prettyPackageId(pkgId)}", Nil)
          case None =>
            Tree(
              s"LookupByKey failure${prettyPackageId(pkgId)} key=($keyId, ${prettyParties(maintainers)})",
              Nil,
            )
        }
      case Rollback(subTransaction) =>
        Tree(s"Rollback", subTransaction.map(actionToTree(_, None)))
    }

    private def prettyParties(partySet: PartySet): String =
      partySet.toSeq.sorted.mkString("{", ",", "}")

    private def prettyContracts(contractIdSet: ContractIdSet): String =
      contractIdSet.toSeq.sorted.mkString("{", ",", "}")

    private def prettyPackageId(pkgId: Option[PackageId]): String =
      pkgId match {
        case Some(id) => s" pkg=$id"
        case None => ""
      }
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
      Tree("Commands", commands.commands.map(commandToTree))

    private def commandToTree(command: Command): Tree = command match {
      case Command(explicitPackageId, action) => actionToTree(action, explicitPackageId)
    }

    private def actionToTree(action: Action, pkgId: Boolean): Tree = action match {
      case Create() =>
        Tree(
          s"Create${prettyPackageId(pkgId)}",
          Nil,
        )
      case CreateWithKey() =>
        Tree(
          s"CreateWithKey${prettyPackageId(pkgId)}",
          Nil,
        )
      case Exercise(kind, subTransaction) =>
        Tree(
          s"Exercise $kind${prettyPackageId(pkgId)}",
          subTransaction.map(actionToTree(_, pkgId = false)),
        )
      case ExerciseByKey(kind, subTransaction) =>
        Tree(
          s"ExerciseByKey $kind${prettyPackageId(pkgId)}",
          subTransaction.map(actionToTree(_, pkgId = false)),
        )
      case Fetch() =>
        Tree(s"Fetch${prettyPackageId(pkgId)}", Nil)
      case FetchByKey() =>
        Tree(s"FetchByKey${prettyPackageId(pkgId)}", Nil)
      case LookupByKey(successful) =>
        Tree(
          s"LookupByKey ${if (successful) "success" else "failure"}${prettyPackageId(pkgId)}",
          Nil,
        )
      case Rollback(subTransaction) =>
        Tree(s"Rollback", subTransaction.map(actionToTree(_, pkgId = false)))
    }

    private def prettyPackageId(explicitPackageId: Boolean): String =
      if (explicitPackageId) " pkg=_" else ""
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
        commands.commands.map(commandToTree),
      )

    private def commandToTree(command: Command): Tree = command match {
      case Command(pkgId, action) => actionToTree(action, pkgId)
    }

    private def actionToTree(action: Action, pkgId: Option[PackageId]): Tree = action match {
      case Create(contractId, signatories, observers) =>
        Tree(
          s"Create ${contractId}${prettyPackageId(pkgId)} sigs=${signatories} obs=${observers}",
          Nil,
        )
      case CreateWithKey(contractId, keyId, maintainers, signatories, observers) =>
        Tree(
          s"CreateWithKey ${contractId}${prettyPackageId(pkgId)} key=(${keyId}, ${maintainers}) sigs=${signatories} obs=${observers}",
          Nil,
        )
      case Exercise(kind, contractId, controllers, choiceObservers, subTransaction) =>
        Tree(
          s"Exercise ${kind} ${contractId}${prettyPackageId(pkgId)} ctl=${controllers} cobs=${choiceObservers}",
          subTransaction.map(actionToTree(_, None)),
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
          s"ExerciseByKey ${kind} ${contractId}${prettyPackageId(
              pkgId
            )} key=(${keyId}, ${maintainers}) ctl=${controllers} cobs=${choiceObservers}",
          subTransaction.map(actionToTree(_, None)),
        )
      case Fetch(contractId) =>
        Tree(s"Fetch ${contractId}${prettyPackageId(pkgId)}", Nil)
      case FetchByKey(contractId, keyId, maintainers) =>
        Tree(s"FetchByKey ${contractId}${prettyPackageId(pkgId)} key=($keyId, ${maintainers})", Nil)
      case LookupByKey(contractId, keyId, maintainers) =>
        contractId match {
          case Some(cid) =>
            Tree(s"LookupByKey success ${cid}${prettyPackageId(pkgId)}", Nil)
          case None =>
            Tree(s"LookupByKey failure${prettyPackageId(pkgId)} key=($keyId, ${maintainers})", Nil)
        }
      case Rollback(subTransaction) =>
        Tree(s"Rollback", subTransaction.map(actionToTree(_, None)))
    }

    private def prettyPackageId(pkgId: Option[PackageId]): String =
      pkgId match {
        case Some(id) => s" pkg=$id"
        case None => ""
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
