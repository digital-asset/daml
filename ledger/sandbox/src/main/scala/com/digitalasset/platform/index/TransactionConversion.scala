// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import com.digitalasset.api.util.TimestampConversion
import com.digitalasset.daml.lf.engine
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, VersionedValue}
import com.digitalasset.daml.lf.value.{Value => Lf}
import com.digitalasset.ledger
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event, ExercisedEvent}
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree, TreeEvent}
import com.digitalasset.platform.api.v1.event.EventOps.TreeEventOps
import com.digitalasset.platform.common.PlatformTypes.{CreateEvent, ExerciseEvent}
import com.digitalasset.platform.participant.util.LfEngineToApi
import com.digitalasset.platform.server.services.transaction.TransactionFiltration.RichTransactionFilter
import com.digitalasset.platform.server.services.transaction.TransientContractRemover
import com.digitalasset.platform.store.entries.LedgerEntry

import scala.annotation.tailrec

object TransactionConversion {

  def ledgerEntryToFlatTransaction(
      offset: domain.LedgerOffset.Absolute,
      trans: LedgerEntry.Transaction,
      filter: domain.TransactionFilter,
      verbose: Boolean,
  ): Option[Transaction] = {
    val events = engine.Event.collectEvents(trans.transaction, trans.explicitDisclosure)
    val allEvents = events.roots.toSeq
      .foldLeft(List.empty[Event])((l, evId) => l ++ flattenEvents(events.events, evId, verbose))

    val filteredEvents = TransientContractRemover
      .removeTransients(allEvents)
      .flatMap(EventFilter(_)(filter).toList)

    val submitterIsSubscriber =
      trans.submittingParty.exists(filter.filtersByParty.keySet)

    if (filteredEvents.nonEmpty || submitterIsSubscriber) {
      Some(
        Transaction(
          trans.transactionId,
          trans.commandId.filter(_ => submitterIsSubscriber).getOrElse(""),
          trans.workflowId.getOrElse(""),
          Some(TimestampConversion.fromInstant(trans.recordedAt)),
          filteredEvents,
          offset.value
        ))
    } else None
  }

  def ledgerEntryToTransaction(
      offset: domain.LedgerOffset.Absolute,
      trans: LedgerEntry.Transaction,
      filter: domain.TransactionFilter,
      verbose: Boolean,
  ): Option[TransactionTree] = {

    val tx = trans.transaction
    filter.filter(tx).map { disclosureByNodeId =>
      val allEvents = engine.Event
        .collectEvents(tx, disclosureByNodeId)
      val events = allEvents.events.map {
        case (nodeId, value) =>
          (nodeId: String, value match {
            case e: ExerciseEvent[ledger.EventId @unchecked, AbsoluteContractId @unchecked] =>
              lfExerciseToApi(nodeId, e, verbose)
            case c: CreateEvent[AbsoluteContractId @unchecked] =>
              lfCreateToApi(nodeId, c, includeParentWitnesses = true, verbose)
          })
      }

      val (byId, roots) =
        removeInvisibleRoots(events, allEvents.roots.toList)

      val commandId =
        trans.commandId
          .filter(_ => trans.submittingParty.exists(filter.filtersByParty.keySet))
          .getOrElse("")

      TransactionTree(
        trans.transactionId,
        commandId,
        trans.workflowId.getOrElse(""),
        Some(TimestampConversion.fromInstant(trans.recordedAt)),
        offset.value,
        byId,
        roots
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
      includeParentWitnesses: Boolean,
      verbose: Boolean) = {
    CreatedEvent(
      eventId,
      create.contractId.coid,
      Some(LfEngineToApi.toApiIdentifier(create.templateId)),
      create.contractKey.map(
        ck =>
          LfEngineToApi.assertOrRuntimeEx(
            "translating the contract key",
            LfEngineToApi
              .lfValueToApiValue(verbose, ck.key.value))),
      Some(
        LfEngineToApi
          .lfValueToApiRecord(verbose = true, create.argument.value match {
            case rec @ Lf.ValueRecord(_, _) => rec
            case _ => throw new RuntimeException(s"Value is not an record.")
          })
          .fold(_ => throw new RuntimeException("Expected value to be a record."), identity)),
      (if (includeParentWitnesses) create.witnesses else create.stakeholders).toSeq,
      create.signatories.toSeq,
      create.observers.toSeq,
      Some(create.agreementText)
    )
  }

  private def lfCreateToApi(
      eventId: String,
      create: CreateEvent[Lf.AbsoluteContractId],
      includeParentWitnesses: Boolean,
      verbose: Boolean,
  ): TreeEvent =
    TreeEvent(
      TreeEvent.Kind.Created(lfCreateToApiCreate(eventId, create, includeParentWitnesses, verbose)))

  private def lfCreateToApiFlat(
      eventId: String,
      create: CreateEvent[Lf.AbsoluteContractId],
      includeParentWitnesses: Boolean,
      verbose: Boolean,
  ): Event =
    Event(Created(lfCreateToApiCreate(eventId, create, includeParentWitnesses, verbose)))

  private def lfExerciseToApi(
      eventId: String,
      exercise: ExerciseEvent[ledger.EventId, Lf.AbsoluteContractId],
      verbose: Boolean,
  ): TreeEvent =
    TreeEvent(
      TreeEvent.Kind.Exercised(ExercisedEvent(
        eventId,
        exercise.contractId.coid,
        Some(LfEngineToApi.toApiIdentifier(exercise.templateId)),
        exercise.choice,
        Some(
          LfEngineToApi
            .lfValueToApiValue(verbose, exercise.choiceArgument.value)
            .getOrElse(
              throw new RuntimeException("Error converting choice argument")
            )),
        exercise.actingParties.toSeq,
        exercise.isConsuming,
        exercise.witnesses.toSeq,
        exercise.children.toSeq,
        exercise.exerciseResult.map(result =>
          LfEngineToApi
            .lfValueToApiValue(verbose, result.value)
            .fold(_ => throw new RuntimeException("Error converting exercise result"), identity)),
      )))

  private def lfConsumingExerciseToApi(
      eventId: ledger.EventId,
      exercise: ExerciseEvent[ledger.EventId, Lf.AbsoluteContractId]
  ): Event =
    Event(
      Archived(
        ArchivedEvent(
          eventId,
          exercise.contractId.coid,
          Some(LfEngineToApi.toApiIdentifier(exercise.templateId)),
          exercise.stakeholders.toSeq
        )
      ))

  private def flattenEvents(
      events: Map[
        ledger.EventId,
        engine.Event[ledger.EventId, AbsoluteContractId, VersionedValue[AbsoluteContractId]]],
      root: ledger.EventId,
      verbose: Boolean): List[Event] = {
    val event = events(root)
    event match {
      case create: CreateEvent[Lf.AbsoluteContractId @unchecked] =>
        List(lfCreateToApiFlat(root, create, includeParentWitnesses = false, verbose))

      case exercise: ExerciseEvent[ledger.EventId, Lf.AbsoluteContractId] =>
        val children =
          exercise.children.iterator
            .flatMap(eventId => flattenEvents(events, eventId, verbose))
            .toList

        if (exercise.isConsuming) {
          lfConsumingExerciseToApi(root, exercise) :: children
        } else children

      case _ => Nil
    }
  }
}
