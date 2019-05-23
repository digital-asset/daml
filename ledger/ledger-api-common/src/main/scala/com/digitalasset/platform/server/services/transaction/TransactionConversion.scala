// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.transaction

import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.engine
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, VersionedValue}
import com.digitalasset.daml.lf.value.{Value => Lf}
import com.digitalasset.daml.lf.data.{Ref => LfRef}
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event, ExercisedEvent}
import com.digitalasset.ledger.api.v1.transaction.TreeEvent
import com.digitalasset.platform.api.v1.event.EventOps._
import com.digitalasset.platform.common.{PlatformTypes => P}
import com.digitalasset.platform.participant.util.LfEngineToApi
import scalaz.Tag

import scala.annotation.tailrec
import scala.collection.breakOut

trait TransactionConversion {

  type Party = String
  type EventId = String

  def genToApiTransaction(
      transaction: P.GenTransaction[EventId, AbsoluteContractId],
      explicitDisclosure: Relation[EventId, Party],
      verbose: Boolean = false): TransactionTreeNodes = {
    val disclosure: Map[EventId, Set[LfRef.Party]] =
      explicitDisclosure.mapValues(parties => parties.map(LfRef.Party.assertFromString))
    val events = engine.Event.collectEvents(transaction, disclosure)
    eventsToTransaction(events, verbose)
  }
  private def convert(ps: Set[LfRef.Party]): Seq[Party] = ps.toSeq

  def eventsToTransaction(
      allEvents: P.Events[EventId, AbsoluteContractId],
      verbose: Boolean): TransactionTreeNodes = {
    val events = allEvents.events.map {
      case (nodeId, value) =>
        (nodeId, value match {
          case e: P.ExerciseEvent[EventId, AbsoluteContractId] =>
            TreeEvent(TreeEvent.Kind.Exercised(lfExerciseToApi(nodeId, e, verbose)))
          case c: P.CreateEvent[AbsoluteContractId] =>
            TreeEvent(TreeEvent.Kind.Created(lfCreateToApi(nodeId, c, true, verbose)))
        })
    }

    removeInvisibleRoots(events, allEvents.roots.toList.sortBy(getEventIndex))
  }

  private case class InvisibleRootRemovalState(
      rootsWereReplaced: Boolean,
      eventsById: Map[String, TreeEvent],
      rootEventIds: List[String])

  // Remove root nodes that have empty witnesses and put their children in their place as roots.
  // Do this while there are roots with no witnesses.
  @tailrec
  private def removeInvisibleRoots(
      eventsById: Map[String, TreeEvent],
      rootEventIds: List[String]): TransactionTreeNodes = {

    val result =
      rootEventIds.foldRight(InvisibleRootRemovalState(false, eventsById, Nil)) {
        case (eventId, InvisibleRootRemovalState(hasInvisibleRoot, filteredEvents, newRoots)) =>
          val event = eventsById
            .getOrElse(
              eventId,
              throw new IllegalArgumentException(
                s"Root event id $eventId is not present among transaction nodes ${eventsById.keySet}"))
          val eventIsVisible = event.witnessParties.nonEmpty
          if (eventIsVisible)
            InvisibleRootRemovalState(hasInvisibleRoot, filteredEvents, eventId :: newRoots)
          else
            InvisibleRootRemovalState(
              true,
              filteredEvents - eventId,
              Tag.unsubst(event.children) ++: newRoots)
      }
    if (result.rootsWereReplaced)
      removeInvisibleRoots(result.eventsById, result.rootEventIds)
    else TransactionTreeNodes(result.eventsById, result.rootEventIds)
  }

  def lfCreateToApi(
      eventId: EventId,
      create: P.CreateEvent[Lf.AbsoluteContractId],
      includeParentWitnesses: Boolean,
      verbose: Boolean): CreatedEvent = {
    CreatedEvent(
      eventId,
      create.contractId.coid,
      Some(LfEngineToApi.toApiIdentifier(create.templateId)),
      Some(
        LfEngineToApi
          .lfValueToApiRecord(verbose, create.argument.value)
          .fold(
            err =>
              throw new RuntimeException(s"Unexpected error when converting stored contract: $err"),
            identity)),
      if (includeParentWitnesses) convert(create.witnesses)
      else convert(create.stakeholders),
      Some(create.agreementText)
    )
  }

  def lfExerciseToApi(
      eventId: EventId,
      exercise: P.ExerciseEvent[EventId, Lf.AbsoluteContractId],
      verbose: Boolean): ExercisedEvent = {
    ExercisedEvent(
      eventId,
      // TODO right now we assume throughout the codebase that the event id _is_ the contract id.
      // this is pretty nasty, we should either not assume that and just look the event id up somewhere,
      // or remove this field here altogether.
      exercise.contractId.coid,
      Some(LfEngineToApi.toApiIdentifier(exercise.templateId)),
      exercise.contractId.coid,
      exercise.choice,
      Some(
        LfEngineToApi
          .lfValueToApiValue(verbose, exercise.choiceArgument.value)
          .fold(
            err =>
              throw new RuntimeException(s"Unexpected error when converting stored contract: $err"),
            identity)),
      convert(exercise.actingParties),
      exercise.isConsuming,
      convert(exercise.witnesses),
      exercise.children.toSeq.sortBy(getEventIndex),
      exercise.exerciseResult.map(
        er =>
          LfEngineToApi
            .lfValueToApiValue(verbose, er.value)
            .fold(
              err =>
                throw new RuntimeException(
                  s"Unexpected error when converting stored contract: $err"),
              identity)),
    )
  }

  def genToFlatTransaction(
      transaction: P.GenTransaction[EventId, AbsoluteContractId],
      explicitDisclosure: Relation[EventId, Party],
      verbose: Boolean = false): List[Event] = {
    val events = engine.Event
      .collectEvents(
        transaction,
        explicitDisclosure.mapValues(parties => parties.map(LfRef.Party.assertFromString)))
    val allEvents = events.roots.toSeq
      .sortBy(getEventIndex)
      .foldLeft(List.empty[Event])((l, evId) => l ::: flattenEvents(events.events, evId, verbose))
    TransientContractRemover.removeTransients(allEvents)

  }
  private def flattenEvents(
      events: Map[
        String,
        engine.Event[EventId, AbsoluteContractId, VersionedValue[AbsoluteContractId]]],
      root: String,
      verbose: Boolean): List[Event] = {
    val event = events(root)
    event match {
      case create: P.CreateEvent[Lf.AbsoluteContractId @unchecked] =>
        List(Event(Created(lfCreateToApi(root, create, false, verbose))))

      case exercise: P.ExerciseEvent[EventId, Lf.AbsoluteContractId] =>
        val children: List[Event] =
          exercise.children.toSeq
            .sortBy(getEventIndex)
            .flatMap(eventId => flattenEvents(events, eventId, verbose))(breakOut)

        if (exercise.isConsuming) {
          Event(
            Archived(
              ArchivedEvent(
                eventId = root,
                contractId = exercise.contractId.coid,
                templateId = Some(LfEngineToApi.toApiIdentifier(exercise.templateId)),
                // do not include parent witnesses
                witnessParties = convert(exercise.stakeholders)
              ))
          ) :: children
        } else children

      case _ => Nil
    }
  }
}

object TransactionConversion extends TransactionConversion
