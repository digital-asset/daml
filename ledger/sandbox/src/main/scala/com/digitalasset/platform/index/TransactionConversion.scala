// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import com.digitalasset.api.util.TimestampConversion
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.engine
import com.digitalasset.daml.lf.transaction.Node.{GenNode, NodeCreate, NodeExercises}
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml.lf.value.{Value => Lf}
import com.digitalasset.ledger.EventId
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.TransactionFilter
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event, ExercisedEvent}
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree, TreeEvent}
import com.digitalasset.platform.api.v1.event.EventOps.TreeEventOps
import com.digitalasset.platform.common.PlatformTypes.{CreateEvent, ExerciseEvent}
import com.digitalasset.platform.participant.util.LfEngineToApi
import com.digitalasset.platform.server.services.transaction.{
  TransactionFiltration,
  TransientContractRemover
}
import com.digitalasset.platform.store.entries.LedgerEntry

import scala.annotation.tailrec

object TransactionConversion {

  type Node = GenNode.WithTxValue[EventId, Lf.AbsoluteContractId]
  type Create = NodeCreate.WithTxValue[Lf.AbsoluteContractId]
  type Exercise = NodeExercises.WithTxValue[EventId, Lf.AbsoluteContractId]
  type Disclosure = Relation[EventId, Ref.Party]

  private def hasWitness(
      eventId: EventId,
      templateId: Ref.Identifier,
      filter: TransactionFilter,
      disclosure: Disclosure,
  ): Boolean =
    disclosure(eventId).exists(filter(_, templateId))

//  private def witnesses(
//      eventId: EventId,
//      templateId: Ref.Identifier,
//      filter: TransactionFilter,
//      disclosure: Disclosure): Set[Party] =
//    disclosure(eventId).filter(filter(_, templateId))

  private def flatEvent(
      filter: TransactionFilter,
      disclosure: Disclosure,
      verbose: Boolean): PartialFunction[(EventId, Node), Event] = {
    case (eventId, c: Create) if hasWitness(eventId, c.coinst.template, filter, disclosure) =>
      Event(
        Event.Event.Created(
          CreatedEvent(
            eventId = eventId,
            contractId = c.coid.coid,
            templateId = Some(LfEngineToApi.toApiIdentifier(c.coinst.template)),
            contractKey = c.key.map(
              k =>
                LfEngineToApi
                  .lfContractKeyToApiValue(verbose = verbose, k)
                  .getOrElse(sys.error("Unable to translate the key"))),
            createArguments = Some(
              LfEngineToApi
                .lfValueToApiRecord(verbose = verbose, c.coinst.arg.value)
                .getOrElse(sys.error("Expected value to be a record."))),
            witnessParties = c.stakeholders.toSeq,
            signatories = c.signatories.toSeq,
            observers = c.stakeholders.diff(c.signatories).toSeq,
            agreementText = Some(c.coinst.agreementText),
          )
        ))
    case (eventId, e: Exercise)
        if e.consuming && hasWitness(eventId, e.templateId, filter, disclosure) =>
      Event(
        Event.Event.Archived(
          ArchivedEvent(
            eventId = eventId,
            contractId = e.targetCoid.coid,
            templateId = Some(LfEngineToApi.toApiIdentifier(e.templateId)),
            witnessParties = e.stakeholders.toSeq,
          )
        ))
  }

  def ledgerEntryToFlatTransaction(
      offset: domain.LedgerOffset.Absolute,
      entry: LedgerEntry.Transaction,
      filter: domain.TransactionFilter,
      verbose: Boolean,
  ): Option[Transaction] = {

    val filteredEvents = TransientContractRemover
      .removeTransients(
        entry.transaction.iterator
          .collect(flatEvent(filter, entry.explicitDisclosure, verbose))
          .toList)

    val commandId =
      entry.commandId
        .filter(_ => entry.submittingParty.exists(filter.filtersByParty.keySet))
        .getOrElse("")

    if (filteredEvents.nonEmpty || commandId.nonEmpty) {
      Some(
        Transaction(
          transactionId = entry.transactionId,
          commandId = commandId,
          workflowId = entry.workflowId.getOrElse(""),
          effectiveAt = Some(TimestampConversion.fromInstant(entry.recordedAt)),
          events = filteredEvents,
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

    TransactionFiltration.disclosures(filter, entry.transaction).map { disclosureByNodeId =>
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

      TransactionTree(
        transactionId = entry.transactionId,
        commandId = commandId,
        workflowId = entry.workflowId.getOrElse(""),
        effectiveAt = Some(TimestampConversion.fromInstant(entry.recordedAt)),
        offset = offset.value,
        eventsById = byId,
        rootEventIds = roots
      )
    }
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
