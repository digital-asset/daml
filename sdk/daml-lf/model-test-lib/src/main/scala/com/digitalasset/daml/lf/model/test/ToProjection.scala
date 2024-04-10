// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.ledger.javaapi
import com.daml.lf.data.Ref
import com.daml.lf.ledger.EventId
import com.daml.lf.model.test.Projections._
import com.daml.lf.scenario.ScenarioLedger
import com.daml.lf.value.{Value => V}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object ToProjection {
  type PartyIdReverseMapping = Map[Ref.Party, PartyId]
  type ContractIdReverseMapping = Map[V.ContractId, ContractId]

  private[ToProjection] class FromCanton(
      partyIds: PartyIdReverseMapping,
      contractIds: ContractIdReverseMapping,
  ) {

    def fromTransactionTrees(
        trees: List[javaapi.data.TransactionTree]
    ): Projection =
      trees.map(fromTransactionTree)

    private def fromTransactionTree(
        tree: javaapi.data.TransactionTree
    ): Commands = {
      Commands(
        tree.getRootEventIds.asScala.toList
          .map(fromEventId(tree.getEventsById.asScala.toMap, _))
      )
    }

    private def fromEventId(
        eventsById: Map[String, javaapi.data.TreeEvent],
        eventId: String,
    ): Action = {
      eventsById(eventId) match {
        case create: javaapi.data.CreatedEvent =>
          Create(
            contractId = fromContractId(create.getContractId),
            signatories = fromPartyIds(create.getSignatories),
          )
        case exercise: javaapi.data.ExercisedEvent =>
          Exercise(
            kind = if (exercise.isConsuming) Consuming else NonConsuming,
            contractId = fromContractId(exercise.getContractId),
            controllers = fromPartyIds(exercise.getActingParties),
            subTransaction = exercise.getChildEventIds.asScala.toList
              .map(fromEventId(eventsById, _)),
          )
        case event =>
          throw new IllegalArgumentException(s"Unsupported event type: $event")
      }
    }

    private def fromContractId(contractId: String): ContractId =
      contractIds(V.ContractId.assertFromString(contractId))

    private def fromPartyId(partyId: String): PartyId =
      partyIds(Ref.Party.assertFromString(partyId))

    private def fromPartyIds(partyIds: java.lang.Iterable[String]): PartySet =
      partyIds.asScala.map(fromPartyId).toSet
  }

  private[ToProjection] class FromScenarioLedger(
      partyIds: PartyIdReverseMapping,
      contractIds: ContractIdReverseMapping,
      party: Ref.Party,
  ) {

    import com.daml.lf.scenario.{ScenarioLedger => SL}
    import com.daml.lf.transaction.{Node, NodeId}

    def fromScenarioLedger(scenarioLedger: ScenarioLedger): Projection =
      scenarioLedger.scenarioSteps.values
        .collect { case commit: SL.Commit =>
          fromTransactionRoots(
            scenarioLedger.ledgerData.nodeInfos,
            commit.txId,
            commit.richTransaction.transaction.roots.toList,
          )
        }
        .flatten
        .toList

    private def fromTransactionRoots(
        nodeInfos: SL.LedgerNodeInfos,
        txId: ScenarioLedger.TransactionId,
        roots: List[NodeId],
    ): Option[Commands] = {
      val newRoots = mutable.Buffer.empty[Action]
      val _ =
        roots.foreach(fromTransactionNode(nodeInfos, txId, newRoots, _, parentIncluded = false))
      Option.when(newRoots.nonEmpty)(Commands(newRoots.toList))
    }

    private def fromTransactionNode(
        nodeInfos: ScenarioLedger.LedgerNodeInfos,
        txId: ScenarioLedger.TransactionId,
        newRoots: mutable.Buffer[Action],
        nodeId: NodeId,
        parentIncluded: Boolean,
    ): Option[Action] = {
      val eventId = EventId(txId.id, nodeId)
      val nodeInfo = nodeInfos(eventId)
      // Canton doesn't seem to return divulged create events
      val included = nodeInfo.disclosures.get(party).exists(_.explicit)
      val res = nodeInfo.node match {
        case create: Node.Create =>
          Option.when(included) {
            Create(
              contractId = contractIds(create.coid),
              signatories = fromParties(create.signatories),
            )
          }
        case exercise: Node.Exercise =>
          // Run unconditionally in order to collect the new roots
          val subTransaction = exercise.children.toList.flatMap(
            fromTransactionNode(nodeInfos, txId, newRoots, _, included)
          )
          Option.when(included) {
            Exercise(
              kind = if (exercise.consuming) Consuming else NonConsuming,
              contractId = contractIds(exercise.targetCoid),
              controllers = fromParties(exercise.actingParties),
              subTransaction = subTransaction,
            )
          }
        case _ => None
      }
      res.foreach(action =>
        if (!parentIncluded) {
          newRoots += action
        }
      )
      res
    }

    private def fromParties(parties: Set[Ref.Party]): PartySet =
      parties.map(partyIds)
  }

  def convertFromCantonProjection(
      partyIds: PartyIdReverseMapping,
      contractIds: ContractIdReverseMapping,
      trees: List[javaapi.data.TransactionTree],
  ): Projection =
    new FromCanton(partyIds, contractIds).fromTransactionTrees(trees)

  def projectFromScenarioLedger(
      partyIds: PartyIdReverseMapping,
      contractIds: ContractIdReverseMapping,
      scenarioLedger: ScenarioLedger,
      party: Ref.Party,
  ): Projection =
    new FromScenarioLedger(partyIds, contractIds, party).fromScenarioLedger(scenarioLedger)
}
