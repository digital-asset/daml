// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import com.digitalasset.api.util.TimestampConversion
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.transaction.Node.{GenNode, NodeCreate, NodeExercises}
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml.lf.value.{Value => Lf}
import com.digitalasset.ledger.EventId
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.v1.event.{CreatedEvent, Event, ExercisedEvent}
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree, TreeEvent}
import com.digitalasset.platform.api.v1.event.EventOps.{EventOps, TreeEventOps}
import com.digitalasset.platform.common.PlatformTypes.{CreateEvent, ExerciseEvent}
import com.digitalasset.platform.events.EventIdFormatter
import com.digitalasset.platform.participant.util.LfEngineToApi
import com.digitalasset.platform.participant.util.LfEngineToApi.{
  assertOrRuntimeEx,
  lfNodeCreateToFlatApiCreated,
  lfNodeExerciseToFlatApiArchived
}
import com.digitalasset.platform.store.entries.LedgerEntry

import scala.annotation.tailrec

object TransactionConversion {

  private type ContractId = Lf.AbsoluteContractId
  private type Node = GenNode.WithTxValue[EventId, ContractId]
  private type Create = NodeCreate.WithTxValue[ContractId]
  private type Exercise = NodeExercises.WithTxValue[EventId, ContractId]

  private def toFlatEvent(verbose: Boolean): PartialFunction[(EventId, Node), Event] = {
    case (eventId, node: Create) =>
      assertOrRuntimeEx(
        failureContext = "converting a create node to a created event",
        lfNodeCreateToFlatApiCreated(verbose, eventId, node)
      )
    case (eventId, node: Exercise) if node.consuming =>
      assertOrRuntimeEx(
        failureContext = "converting a consuming exercise node to an archived event",
        lfNodeExerciseToFlatApiArchived(eventId, node)
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

  private[index] def removeTransient(events: Vector[Event]): Vector[Event] = {
    val toKeep = permanent(events)
    events.filter(event => toKeep(event.contractId))
  }

  def ledgerEntryToFlatTransaction(
      offset: domain.LedgerOffset.Absolute,
      entry: LedgerEntry.Transaction,
      filter: domain.TransactionFilter,
      verbose: Boolean,
  ): Option[Transaction] = {

    val flatEvents = removeTransient(entry.transaction.collect(toFlatEvent(verbose)))
    val filtered = flatEvents.flatMap(EventFilter(_)(filter).toList)

    val commandId =
      entry.commandId
        .filter(_ => entry.submittingParty.exists(filter.filtersByParty.keySet))
        .getOrElse("")

    if (filtered.nonEmpty || commandId.nonEmpty) {
      Some(
        Transaction(
          transactionId = entry.transactionId,
          commandId = commandId,
          workflowId = entry.workflowId.getOrElse(""),
          effectiveAt = Some(TimestampConversion.fromInstant(entry.recordedAt)),
          events = filtered,
          offset = offset.value,
        ))
    } else None
  }

  def ledgerEntryToTransactionTree(
      offset: domain.LedgerOffset.Absolute,
      entry: LedgerEntry.Transaction,
      requestingParties: Set[Ref.Party],
      verbose: Boolean,
  ): Option[TransactionTree] = {

    import EventIdFormatter.{fromTransactionId, split}

    val disclosure =
      Blinding
        .blind(entry.transaction.mapNodeId(split(_).get.nodeId))
        .disclosure
        .map {
          case (k, v) =>
            fromTransactionId(entry.transactionId, k) -> v.intersect(requestingParties)
        }

    val allEvents = engine.Event.collectEvents(entry.transaction, disclosure)
    val events = allEvents.events.map {
      case (nodeId, value) =>
        (nodeId: String, value match {
          case e: ExerciseEvent[EventId @unchecked, AbsoluteContractId @unchecked] =>
            lfExerciseToApi(nodeId, e, verbose)
          case c: CreateEvent[AbsoluteContractId @unchecked] =>
            lfCreateToApi(nodeId, c, verbose)
        })
    }

    val (byId, roots) =
      removeInvisibleRoots(events, allEvents.roots.toList)

    val commandId =
      entry.commandId
        .filter(_ => entry.submittingParty.exists(requestingParties))
        .getOrElse("")

    Some(
      TransactionTree(
        transactionId = entry.transactionId,
        commandId = commandId,
        workflowId = entry.workflowId.getOrElse(""),
        effectiveAt = Some(TimestampConversion.fromInstant(entry.recordedAt)),
        offset = offset.value,
        eventsById = byId,
        rootEventIds = roots
      )).filter(_.eventsById.nonEmpty)
  }

  private case class InvisibleRootRemovalState(
      rootsWereReplaced: Boolean,
      eventsById: Map[String, TreeEvent],
      rootEventIds: Seq[String])

  // Remove root nodes that have empty witnesses and put their children in their place as roots.
  // Do this while there are roots with no witnesses.
  @tailrec
  private def removeInvisibleRoots(
      eventsById: Map[String, TreeEvent],
      rootEventIds: Seq[String]): (Map[String, TreeEvent], Seq[String]) = {

    val result =
      rootEventIds.foldRight(
        InvisibleRootRemovalState(rootsWereReplaced = false, eventsById, Seq.empty)) {
        case (eventId, InvisibleRootRemovalState(hasInvisibleRoot, filteredEvents, newRoots)) =>
          val event = eventsById
            .getOrElse(
              eventId,
              throw new IllegalArgumentException(
                s"Root event id $eventId is not present among transaction nodes ${eventsById.keySet}"))
          if (event.witnessParties.nonEmpty)
            InvisibleRootRemovalState(hasInvisibleRoot, filteredEvents, eventId +: newRoots)
          else
            InvisibleRootRemovalState(
              rootsWereReplaced = true,
              filteredEvents - eventId,
              event.childEventIds ++ newRoots)
      }
    if (result.rootsWereReplaced)
      removeInvisibleRoots(result.eventsById, result.rootEventIds)
    else (result.eventsById, result.rootEventIds)
  }

  private def lfCreateToApiCreate(
      eventId: String,
      create: CreateEvent[AbsoluteContractId],
      witnessParties: CreateEvent[AbsoluteContractId] => Set[Ref.Party],
      verbose: Boolean): CreatedEvent = {
    CreatedEvent(
      eventId = eventId,
      contractId = create.contractId.coid,
      templateId = Some(LfEngineToApi.toApiIdentifier(create.templateId)),
      contractKey = create.contractKey.map(
        ck =>
          LfEngineToApi.assertOrRuntimeEx(
            "translating the contract key",
            LfEngineToApi
              .lfValueToApiValue(verbose, ck.key.value))),
      createArguments = Some(
        LfEngineToApi
          .lfValueToApiRecord(verbose, create.argument.value match {
            case rec @ Lf.ValueRecord(_, _) => rec
            case _ => throw new RuntimeException(s"Value is not an record.")
          })
          .fold(_ => throw new RuntimeException("Expected value to be a record."), identity)),
      witnessParties = witnessParties(create).toSeq,
      signatories = create.signatories.toSeq,
      observers = create.observers.toSeq,
      agreementText = Some(create.agreementText)
    )
  }

  private def lfCreateToApi(
      eventId: String,
      create: CreateEvent[ContractId],
      verbose: Boolean,
  ): TreeEvent =
    TreeEvent(TreeEvent.Kind.Created(lfCreateToApiCreate(eventId, create, _.witnesses, verbose)))

  private def lfExerciseToApi(
      eventId: String,
      exercise: ExerciseEvent[EventId, ContractId],
      verbose: Boolean,
  ): TreeEvent =
    TreeEvent(
      TreeEvent.Kind.Exercised(ExercisedEvent(
        eventId = eventId,
        contractId = exercise.contractId.coid,
        templateId = Some(LfEngineToApi.toApiIdentifier(exercise.templateId)),
        choice = exercise.choice,
        choiceArgument = Some(
          LfEngineToApi
            .lfValueToApiValue(verbose, exercise.choiceArgument.value)
            .getOrElse(
              throw new RuntimeException("Error converting choice argument")
            )),
        actingParties = exercise.actingParties.toSeq,
        consuming = exercise.isConsuming,
        witnessParties = exercise.witnesses.toSeq,
        childEventIds = exercise.children.toSeq,
        exerciseResult = exercise.exerciseResult.map(result =>
          LfEngineToApi
            .lfValueToApiValue(verbose, result.value)
            .fold(_ => throw new RuntimeException("Error converting exercise result"), identity)),
      )))

}
