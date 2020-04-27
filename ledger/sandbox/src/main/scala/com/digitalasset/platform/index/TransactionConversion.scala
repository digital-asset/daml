// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.api.util.TimestampConversion
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.data.Relation.Relation
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction.GenTransaction
import com.daml.lf.transaction.Node.{GenNode, NodeCreate, NodeExercises}
import com.daml.lf.value.{Value => Lf}
import com.daml.ledger.{CommandId, EventId, TransactionId}
import com.daml.ledger.api.domain
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.transaction.{
  TreeEvent,
  Transaction => ApiTransaction,
  TransactionTree => ApiTransactionTree
}
import com.daml.platform.api.v1.event.EventOps.EventOps
import com.daml.platform.api.v1.event.EventOps.TreeEventOps
import com.daml.platform.events.EventIdFormatter.{fromTransactionId, split}
import com.daml.platform.participant.util.LfEngineToApi.{
  assertOrRuntimeEx,
  lfNodeCreateToEvent,
  lfNodeCreateToTreeEvent,
  lfNodeExercisesToEvent,
  lfNodeExercisesToTreeEvent
}
import com.daml.platform.store.entries.LedgerEntry

import scala.annotation.tailrec

object TransactionConversion {

  private type ContractId = Lf.AbsoluteContractId
  private type Transaction = GenTransaction.WithTxValue[EventId, ContractId]
  private type Node = GenNode.WithTxValue[EventId, ContractId]
  private type Create = NodeCreate.WithTxValue[ContractId]
  private type Exercise = NodeExercises.WithTxValue[EventId, ContractId]

  private def collect[A](tx: Transaction)(pf: PartialFunction[(EventId, Node), A]): Vector[A] =
    tx.fold(Vector.empty[A]) {
      case (nodes, node) if pf.isDefinedAt(node) => nodes :+ pf(node)
      case (nodes, _) => nodes
    }

  private def maskCommandId(
      commandId: Option[CommandId],
      submittingParty: Option[Ref.Party],
      requestingParties: Set[Ref.Party],
  ): String =
    commandId.filter(_ => submittingParty.exists(requestingParties)).getOrElse("")

  private def toFlatEvent(verbose: Boolean): PartialFunction[(EventId, Node), Event] = {
    case (eventId, node: Create) =>
      assertOrRuntimeEx(
        failureContext = "converting a create node to a created event",
        lfNodeCreateToEvent(verbose, eventId, node)
      )
    case (eventId, node: Exercise) if node.consuming =>
      assertOrRuntimeEx(
        failureContext = "converting a consuming exercise node to an archived event",
        lfNodeExercisesToEvent(eventId, node)
      )
  }

  private def permanent(events: Vector[Event]): Set[String] = {
    events.foldLeft(Set.empty[String]) { (contractIds, event) =>
      if (event.isCreated || !contractIds.contains(event.contractId)) {
        contractIds + event.contractId
      } else {
        contractIds - event.contractId
      }
    }
  }

  private[platform] def removeTransient(events: Vector[Event]): Vector[Event] = {
    val toKeep = permanent(events)
    events.filter(event => toKeep(event.contractId))
  }

  def ledgerEntryToFlatTransaction(
      offset: domain.LedgerOffset.Absolute,
      entry: LedgerEntry.Transaction,
      filter: domain.TransactionFilter,
      verbose: Boolean,
  ): Option[ApiTransaction] = {
    val flatEvents = removeTransient(collect(entry.transaction)(toFlatEvent(verbose)))
    val filtered = flatEvents.flatMap(EventFilter(_)(filter).toList)
    val requestingParties = filter.filtersByParty.keySet
    val commandId = maskCommandId(entry.commandId, entry.submittingParty, requestingParties)
    Some(
      ApiTransaction(
        transactionId = entry.transactionId,
        commandId = commandId,
        workflowId = entry.workflowId.getOrElse(""),
        effectiveAt = Some(TimestampConversion.fromInstant(entry.ledgerEffectiveTime)),
        events = filtered,
        offset = offset.value,
      )).filter(tx => tx.events.nonEmpty || tx.commandId.nonEmpty)
  }

  private def disclosureForParties(
      transactionId: TransactionId,
      transaction: Transaction,
      parties: Set[Ref.Party],
  ): Option[Relation[EventId, Ref.Party]] =
    Some(
      Blinding
        .blind(transaction.mapNodeId(split(_).get.nodeId))
        .disclosure
        .flatMap {
          case (nodeId, disclosure) =>
            List(disclosure.intersect(parties)).collect {
              case disclosure if disclosure.nonEmpty =>
                fromTransactionId(transactionId, nodeId) -> disclosure
            }
        }
    ).filter(_.nonEmpty)

  private def isCreateOrExercise(n: Node): Boolean = {
    n match {
      case _: Exercise => true
      case _: Create => true
      case _ => false
    }
  }
  private def toTreeEvent(
      verbose: Boolean,
      disclosure: Relation[EventId, Ref.Party],
      eventsById: Map[EventId, Node],
  ): PartialFunction[(EventId, Node), (String, TreeEvent)] = {
    case (eventId, node: Create) if disclosure.contains(eventId) =>
      eventId -> assertOrRuntimeEx(
        failureContext = "converting a create node to a created event",
        lfNodeCreateToTreeEvent(verbose, eventId, disclosure(eventId), node),
      )
    case (eventId, node: Exercise) if disclosure.contains(eventId) =>
      eventId -> assertOrRuntimeEx(
        failureContext = "converting an exercise node to an exercise event",
        lfNodeExercisesToTreeEvent(verbose, eventId, disclosure(eventId), node)
          .map(_.filterChildEventIds(eventId =>
            isCreateOrExercise(eventsById(eventId.asInstanceOf[EventId]))))
      )
  }

  @tailrec
  private def newRoots(
      tx: Transaction,
      disclosure: Relation[EventId, Ref.Party],
  ): Seq[String] = {
    val (replaced, roots) =
      tx.roots.foldLeft((false, IndexedSeq.empty[EventId])) {
        case ((replaced, roots), eventId) =>
          if (isCreateOrExercise(tx.nodes(eventId)) && disclosure.contains(eventId)) {
            (replaced, roots :+ eventId)
          } else
            tx.nodes(eventId) match {
              case e: Exercise => (true, roots ++ e.children.toIndexedSeq)
              case _ => (true, roots)
            }
      }
    if (replaced) newRoots(tx.copy(roots = ImmArray(roots)), disclosure) else roots
  }

  private def applyDisclosure(
      tx: Transaction,
      disclosure: Relation[EventId, Ref.Party],
      verbose: Boolean,
  ): Option[ApiTransactionTree] =
    Some(collect(tx)(toTreeEvent(verbose, disclosure, tx.nodes))).collect {
      case events if events.nonEmpty =>
        ApiTransactionTree(
          eventsById = events.toMap,
          rootEventIds = newRoots(tx, disclosure)
        )
    }

  def ledgerEntryToTransactionTree(
      offset: domain.LedgerOffset.Absolute,
      entry: LedgerEntry.Transaction,
      requestingParties: Set[Ref.Party],
      verbose: Boolean,
  ): Option[ApiTransactionTree] = {
    val filteredTree =
      for {
        disclosure <- disclosureForParties(
          entry.transactionId,
          entry.transaction,
          requestingParties,
        )
        filteredTree <- applyDisclosure(entry.transaction, disclosure, verbose)
      } yield filteredTree

    filteredTree.map(
      _.copy(
        transactionId = entry.transactionId,
        commandId = maskCommandId(entry.commandId, entry.submittingParty, requestingParties),
        workflowId = entry.workflowId.getOrElse(""),
        effectiveAt = Some(TimestampConversion.fromInstant(entry.ledgerEffectiveTime)),
        offset = offset.value,
      ))
  }

}
