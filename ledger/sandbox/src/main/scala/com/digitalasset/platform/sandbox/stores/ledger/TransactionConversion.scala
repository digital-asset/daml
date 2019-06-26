// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import com.digitalasset.daml.lf.engine
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, VersionedValue}
import com.digitalasset.daml.lf.value.{Value => Lf}
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.Event.{CreateOrArchiveEvent, CreateOrExerciseEvent}
import com.digitalasset.platform.api.v1.event.EventOps.getEventIndex
import com.digitalasset.platform.common.{PlatformTypes => P}
import com.digitalasset.platform.participant.util.EventFilter.TemplateAwareFilter
import com.digitalasset.platform.server.services.transaction.TransactionFiltration.RichTransactionFilter
import com.digitalasset.platform.server.services.transaction.TransientContractRemover
import scalaz.Tag
import scalaz.syntax.tag._

import scala.annotation.tailrec
import scala.collection.breakOut

trait TransactionConversion {

  def ledgerEntryToDomainFlat(
      offset: domain.LedgerOffset.Absolute,
      trans: LedgerEntry.Transaction,
      filter: domain.TransactionFilter
  ): Option[domain.Transaction] = {
    val tx = trans.transaction.mapNodeId(domain.EventId(_))
    val events = engine.Event
      .collectEvents(tx, trans.explicitDisclosure.map { case (k, v) => domain.EventId(k) -> v })
    val allEvents = events.roots.toSeq
      .sortBy(evId => getEventIndex(evId.unwrap))
      .foldLeft(List.empty[CreateOrArchiveEvent])((l, evId) =>
        l ::: flattenEvents(events.events, evId, true))

    val eventFilter = TemplateAwareFilter(filter)
    val filteredEvents = TransientContractRemover
      .removeTransients(allEvents)
      .flatMap(eventFilter.filterCreateOrArchiveWitnesses(_).toList)

    val submitterIsSubscriber =
      trans.submittingParty.exists(eventFilter.isSubmitterSubscriber)

    if (filteredEvents.nonEmpty || submitterIsSubscriber) {
      Some(
        domain.Transaction(
          domain.TransactionId(trans.transactionId),
          if (submitterIsSubscriber) Tag.subst(trans.commandId) else None,
          trans.workflowId.map(domain.WorkflowId(_)),
          trans.recordedAt,
          filteredEvents,
          offset,
          None
        ))
    } else None
  }

  def ledgerEntryToDomainTree(
      offset: domain.LedgerOffset.Absolute,
      trans: LedgerEntry.Transaction,
      filter: domain.TransactionFilter): Option[domain.TransactionTree] = {

    val tx = trans.transaction.mapNodeId(domain.EventId(_))
    filter.filter(tx).map { disclosureByNodeId =>
      val allEvents = engine.Event
        .collectEvents(tx, disclosureByNodeId)
      val events = allEvents.events.map {
        case (nodeId, value) =>
          (nodeId, value match {
            case e: P.ExerciseEvent[domain.EventId, AbsoluteContractId] =>
              lfExerciseToDomain(nodeId, e)
            case c: P.CreateEvent[AbsoluteContractId] =>
              lfCreateToDomain(nodeId, c, true)
          })
      }

      val (byId, roots) =
        removeInvisibleRoots(
          events,
          allEvents.roots.toList
            .sortBy(evid => getEventIndex(evid.unwrap)))

      val subscriberIsSubmitter =
        trans.submittingParty.exists(TemplateAwareFilter(filter).isSubmitterSubscriber)

      domain.TransactionTree(
        domain.TransactionId(trans.transactionId),
        if (subscriberIsSubmitter) Tag.subst(trans.commandId) else None,
        Tag.subst(trans.workflowId),
        trans.recordedAt,
        offset,
        byId,
        roots,
        None
      )
    }
  }

  private case class InvisibleRootRemovalState(
      rootsWereReplaced: Boolean,
      eventsById: Map[domain.EventId, CreateOrExerciseEvent],
      rootEventIds: List[domain.EventId])

  // Remove root nodes that have empty witnesses and put their children in their place as roots.
  // Do this while there are roots with no witnesses.
  @tailrec
  private def removeInvisibleRoots(
      eventsById: Map[domain.EventId, CreateOrExerciseEvent],
      rootEventIds: List[domain.EventId])
    : (Map[domain.EventId, CreateOrExerciseEvent], List[domain.EventId]) = {

    val result =
      rootEventIds.foldRight(InvisibleRootRemovalState(rootsWereReplaced = false, eventsById, Nil)) {
        case (eventId, InvisibleRootRemovalState(hasInvisibleRoot, filteredEvents, newRoots)) =>
          val event = eventsById
            .getOrElse(
              eventId,
              throw new IllegalArgumentException(
                s"Root event id $eventId is not present among transaction nodes ${eventsById.keySet}"))
          if (event.witnessParties.nonEmpty)
            InvisibleRootRemovalState(hasInvisibleRoot, filteredEvents, eventId :: newRoots)
          else
            InvisibleRootRemovalState(
              rootsWereReplaced = true,
              filteredEvents - eventId,
              event.children ++: newRoots)
      }
    if (result.rootsWereReplaced)
      removeInvisibleRoots(result.eventsById, result.rootEventIds)
    else (result.eventsById, result.rootEventIds)
  }

  def lfCreateToDomain(
      eventId: domain.EventId,
      create: P.CreateEvent[Lf.AbsoluteContractId],
      includeParentWitnesses: Boolean,
  ): domain.Event.CreatedEvent = {
    domain.Event.CreatedEvent(
      eventId,
      domain.ContractId(create.contractId.coid),
      create.templateId,
      create.argument.value match {
        case rec @ Lf.ValueRecord(tycon, fields) => rec
        case _ => throw new RuntimeException(s"Value is not an record.")
      },
      if (includeParentWitnesses) create.witnesses
      else create.stakeholders,
      create.signatories,
      create.observers,
      create.agreementText,
      create.contractKey.map(_.key.value)
    )
  }

  def lfExerciseToDomain(
      eventId: domain.EventId,
      exercise: P.ExerciseEvent[domain.EventId, Lf.AbsoluteContractId],
  ): domain.Event.ExercisedEvent = {
    domain.Event.ExercisedEvent(
      eventId,
      // TODO right now we assume throughout the codebase that the event id _is_ the contract id.
      // this is pretty nasty, we should either not assume that and just look the event id up somewhere,
      // or remove this field here altogether.
      domain.ContractId(exercise.contractId.coid),
      exercise.templateId,
      domain.EventId(exercise.contractId.coid),
      exercise.choice,
      exercise.choiceArgument.value,
      exercise.actingParties,
      exercise.isConsuming,
      exercise.children.toList.sortBy(ev => getEventIndex(ev.unwrap)),
      exercise.witnesses,
      exercise.exerciseResult.map(_.value),
    )
  }

  def lfConsumingExerciseToDomain(
      eventId: domain.EventId,
      exercise: P.ExerciseEvent[domain.EventId, Lf.AbsoluteContractId])
    : domain.Event.ArchivedEvent = {
    domain.Event.ArchivedEvent(
      eventId = eventId,
      contractId = domain.ContractId(exercise.contractId.coid),
      templateId = exercise.templateId,
      // do not include parent witnesses
      witnessParties = exercise.stakeholders
    )
  }

  private def flattenEvents(
      events: Map[
        domain.EventId,
        engine.Event[domain.EventId, AbsoluteContractId, VersionedValue[AbsoluteContractId]]],
      root: domain.EventId,
      verbose: Boolean): List[domain.Event.CreateOrArchiveEvent] = {
    val event = events(root)
    event match {
      case create: P.CreateEvent[Lf.AbsoluteContractId @unchecked] =>
        List(lfCreateToDomain(root, create, includeParentWitnesses = false))

      case exercise: P.ExerciseEvent[domain.EventId, Lf.AbsoluteContractId] =>
        val children: List[domain.Event.CreateOrArchiveEvent] =
          exercise.children.toSeq
            .sortBy(ev => getEventIndex(ev.unwrap))
            .flatMap(eventId => flattenEvents(events, eventId, verbose))(breakOut)

        if (exercise.isConsuming) {
          lfConsumingExerciseToDomain(root, exercise) :: children
        } else children

      case _ => Nil
    }
  }
}

object TransactionConversion extends TransactionConversion
