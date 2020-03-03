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
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created, Empty}
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event, ExercisedEvent}
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree, TreeEvent}
import com.digitalasset.platform.api.v1.event.EventOps.TreeEventOps
import com.digitalasset.platform.common.PlatformTypes.{CreateEvent, ExerciseEvent}
import com.digitalasset.platform.events.EventIdFormatter
import com.digitalasset.platform.participant.util.LfEngineToApi
import com.digitalasset.platform.participant.util.LfEngineToApi.{
  assertOrRuntimeEx,
  lfNodeCreateToFlatApiCreated,
  lfNodeExerciseToFlatApiArchived
}
import com.digitalasset.platform.store.entries.LedgerEntry
import com.digitalasset.platform.api.v1.event.EventOps.EventOps

import scala.annotation.tailrec
import scala.collection.{breakOut, mutable}

object TransactionConversion {

  private type Node = GenNode.WithTxValue[EventId, Lf.AbsoluteContractId]
  private type Create = NodeCreate.WithTxValue[Lf.AbsoluteContractId]
  private type Exercise = NodeExercises.WithTxValue[EventId, Lf.AbsoluteContractId]

  private def flatEvent(verbose: Boolean): PartialFunction[(EventId, Node), Event] = {
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

  /**
    * Cancels out witnesses on creates and archives that are about the same contract.
    * If no witnesses remain on either, the node is removed.
    *
    * @param nodes Must be sorted by event index.
    * @throws IllegalArgumentException if the argument is not sorted properly.
    */
  private[index] def removeTransients(nodes: Vector[Event]): Vector[Event] = {

    val resultBuilder = new Array[Option[Event]](nodes.size)
    val creationByContractId = new mutable.HashMap[String, (Int, Event)]()

    nodes.zipWithIndex.foreach {
      case (event, indexInList) =>
        // Each call adds a new (possibly null) element to resultBuilder, and may update items previously added
        updateResultBuilder(resultBuilder, creationByContractId, event, indexInList)
    }

    resultBuilder.collect {
      case Some(v) if v.witnessParties.nonEmpty => v
    }(breakOut)
  }

  /**
    * Update resultBuilder given the next event.
    * This will insert a new element and possibly update a previous one.
    */
  private def updateResultBuilder(
      resultBuilder: Array[Option[Event]],
      creationByContractId: mutable.HashMap[String, (Int, Event)],
      event: Event,
      indexInList: Int
  ): Unit =
    event match {
      case createdEvent @ Event(
            Created(CreatedEvent(_, contractId, _, _, witnessParties, _, _, _, _))) =>
        if (witnessParties.nonEmpty) {
          resultBuilder.update(indexInList, Some(event))
          val _ = creationByContractId.put(contractId, indexInList -> createdEvent)
        }
      case archivedEvent @ Event(Archived(ArchivedEvent(_, contractId, _, witnessParties))) =>
        if (witnessParties.nonEmpty) {
          creationByContractId
            .get(contractId)
            .fold[Unit] {
              // No matching create for this archive. Insert as is.
              resultBuilder.update(indexInList, Some(event))
            } {
              case (createdEventIndex, createdEvent) =>
                // Defensive code to ensure that the set of parties the events are disclosed to are not different.
                if (witnessParties.toSet != createdEvent.getCreated.witnessParties.toSet)
                  throw new IllegalArgumentException(
                    s"Created and Archived event stakeholders are different in $createdEvent, $archivedEvent")

                resultBuilder.update(createdEventIndex, None)
                resultBuilder.update(indexInList, None)
            }
        }
      case Event(Empty) =>
        throw new IllegalArgumentException("Empty event")
    }

  def ledgerEntryToFlatTransaction(
      offset: domain.LedgerOffset.Absolute,
      entry: LedgerEntry.Transaction,
      filter: domain.TransactionFilter,
      verbose: Boolean,
  ): Option[Transaction] = {

    val flatEvents = entry.transaction.collect(flatEvent(verbose))
    val withoutTransientContracts = removeTransients(flatEvents)
    val withFilteredWitnesses = withoutTransientContracts.flatMap(EventFilter(_)(filter).toList)

    val commandId =
      entry.commandId
        .filter(_ => entry.submittingParty.exists(filter.filtersByParty.keySet))
        .getOrElse("")

    if (withFilteredWitnesses.nonEmpty || commandId.nonEmpty) {
      Some(
        Transaction(
          transactionId = entry.transactionId,
          commandId = commandId,
          workflowId = entry.workflowId.getOrElse(""),
          effectiveAt = Some(TimestampConversion.fromInstant(entry.recordedAt)),
          events = withFilteredWitnesses,
          offset = offset.value,
        ))
    } else None
  }

  def ledgerEntryToTransaction(
      offset: domain.LedgerOffset.Absolute,
      entry: LedgerEntry.Transaction,
      filter: domain.TransactionFilter,
      verbose: Boolean,
  ): Option[TransactionTree] = {

    val disclosureByNodeId =
      Blinding
        .blind(entry.transaction.mapNodeId(EventIdFormatter.split(_).get.nodeId))
        .disclosure
        .mapValues(_.intersect(filter.filtersByParty.keySet))
        .map {
          case (k, v) =>
            (EventIdFormatter.fromTransactionId(entry.transactionId, k): EventId) -> v
        }

    if (disclosureByNodeId.exists(_._2.nonEmpty)) {

      val allEvents = engine.Event.collectEvents(entry.transaction, disclosureByNodeId)
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
          .filter(_ => entry.submittingParty.exists(filter.filtersByParty.keySet))
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
        ))
    } else None
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
      create: CreateEvent[Lf.AbsoluteContractId],
      verbose: Boolean,
  ): TreeEvent =
    TreeEvent(TreeEvent.Kind.Created(lfCreateToApiCreate(eventId, create, _.witnesses, verbose)))

  private def lfExerciseToApi(
      eventId: String,
      exercise: ExerciseEvent[EventId, Lf.AbsoluteContractId],
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
