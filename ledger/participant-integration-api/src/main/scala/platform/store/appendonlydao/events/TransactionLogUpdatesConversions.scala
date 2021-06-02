// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.time.Instant

import com.daml.ledger.api.v1
import com.daml.ledger.api.v1.transaction.{
  TransactionTree,
  TreeEvent,
  Transaction => FlatTransaction,
}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.ApiOffset
import com.daml.platform.api.v1.event.EventOps.TreeEventOps
import com.daml.platform.participant.util.LfEngineToApi
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.{CreatedEvent, ExercisedEvent}
import com.google.protobuf.timestamp.Timestamp

import scala.concurrent.{ExecutionContext, Future}

private[events] object TransactionLogUpdatesConversions {
  object ToFlatTransaction {
    def apply(
        tx: TransactionLogUpdate.Transaction,
        filter: FilterRelation,
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Future[Option[FlatTransaction]] = {
      val aux = tx.events.filter(FlatTransactionPredicate(_, filter))
      val nonTransientIds = permanent(aux)
      val events = aux.filter(ev => nonTransientIds(ev.contractId))

      events.headOption
        .collect {
          case first if first.commandId.nonEmpty || events.nonEmpty =>
            Future
              .traverse(events)(toFlatEvent(_, filter.keySet, verbose, lfValueTranslation))
              .map(_.collect { case Some(v) => v })
              .map {
                case Vector() => None
                case flatEvents =>
                  Some(
                    FlatTransaction(
                      transactionId = first.transactionId,
                      commandId = getCommandId(
                        events,
                        tx.commandId,
                        filter.keySet,
                      ), // TODO // TDT Check condition for command id
                      workflowId = first.workflowId,
                      effectiveAt = Some(instantToTimestamp(first.ledgerEffectiveTime)),
                      events = flatEvents,
                      offset = ApiOffset.toApiString(tx.offset),
                      traceContext = None,
                    )
                  )
              }
        }
        .getOrElse(Future.successful(None))
    }

    private val FlatTransactionPredicate =
      (event: TransactionLogUpdate.Event, filter: FilterRelation) =>
        if (filter.size == 1) {
          val (party, templateIds) = filter.iterator.next()
          if (templateIds.isEmpty)
            event.flatEventWitnesses.contains(party)
          else
            // Single-party request, restricted to a set of template identifiers
            event.flatEventWitnesses.contains(party) && templateIds.contains(event.templateId)
        } else {
          // Multi-party requests
          // If no party requests specific template identifiers
          val parties = filter.keySet
          if (filter.forall(_._2.isEmpty))
            event.flatEventWitnesses.intersect(parties.map(_.toString)).nonEmpty
          else {
            // If all parties request the same template identifier
            val templateIds = filter.valuesIterator.flatten.toSet
            if (filter.valuesIterator.forall(_ == templateIds)) {
              event.flatEventWitnesses.intersect(parties.map(_.toString)).nonEmpty &&
              templateIds.contains(event.templateId)
            } else {
              // If there are different template identifier but there are no wildcard parties
              val partiesAndTemplateIds = Relation.flatten(filter).toSet
              val wildcardParties = filter.filter(_._2.isEmpty).keySet
              if (wildcardParties.isEmpty) {
                partiesAndTemplateIds.exists { case (party, identifier) =>
                  event.flatEventWitnesses.contains(party) && identifier == event.templateId
                }
              } else {
                // If there are wildcard parties and different template identifiers
                partiesAndTemplateIds.exists { case (party, identifier) =>
                  event.flatEventWitnesses.contains(party) && identifier == event.templateId
                } || event.flatEventWitnesses.intersect(wildcardParties.map(_.toString)).nonEmpty
              }
            }
          }
        }

    private def permanent(events: Seq[TransactionLogUpdate.Event]): Set[ContractId] =
      events.foldLeft(Set.empty[ContractId]) {
        case (contractIds, event: TransactionLogUpdate.CreatedEvent) =>
          contractIds + event.contractId
        case (contractIds, event) if !contractIds.contains(event.contractId) =>
          contractIds + event.contractId
        case (contractIds, event) => contractIds - event.contractId
      }

    private def toFlatEvent(
        event: TransactionLogUpdate.Event,
        requestingParties: Set[Party],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Future[Option[com.daml.ledger.api.v1.event.Event]] =
      event match {
        case createdEvent: TransactionLogUpdate.CreatedEvent =>
          createdToFlatEvent(requestingParties, verbose, lfValueTranslation, createdEvent)

        case exercisedEvent: TransactionLogUpdate.ExercisedEvent if exercisedEvent.consuming =>
          Future.successful(Some(exercisedToFlatEvent(requestingParties, exercisedEvent)))

        case _ => Future.successful(None)
      }

    private def createdToFlatEvent(
        requestingParties: Set[Party],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
        createdEvent: CreatedEvent,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ) = {
      val eventualContractKey = createdEvent.contractKey
        .map(
          lfValueTranslation
            .toApiValue(
              _,
              verbose,
              "create key",
              value =>
                lfValueTranslation.enricher
                  .enrichContractKey(createdEvent.templateId, value.value),
            )
            .map(Some(_))
        )
        .getOrElse(Future.successful(None))

      val eventualCreateArguments = lfValueTranslation.toApiRecord(
        createdEvent.createArgument,
        verbose,
        "create argument",
        value =>
          lfValueTranslation.enricher
            .enrichContract(createdEvent.templateId, value.value),
      )

      for {
        maybeContractKey <- eventualContractKey
        createArguments <- eventualCreateArguments
      } yield Some(
        com.daml.ledger.api.v1.event.Event(
          com.daml.ledger.api.v1.event.Event.Event.Created(
            com.daml.ledger.api.v1.event.CreatedEvent(
              eventId = createdEvent.eventId.toLedgerString,
              contractId = createdEvent.contractId.coid,
              templateId = Some(LfEngineToApi.toApiIdentifier(createdEvent.templateId)),
              contractKey = maybeContractKey,
              createArguments = Some(createArguments),
              witnessParties = createdEvent.flatEventWitnesses
                .intersect(requestingParties.map(_.toString))
                .toSeq,
              signatories = createdEvent.createSignatories.toSeq,
              observers = createdEvent.createObservers.toSeq,
              agreementText = createdEvent.createAgreementText.orElse(Some("")),
            )
          )
        )
      )
    }

    private def exercisedToFlatEvent(
        requestingParties: Set[Party],
        exercisedEvent: ExercisedEvent,
    ) =
      v1.event.Event(
        v1.event.Event.Event.Archived(
          v1.event.ArchivedEvent(
            eventId = exercisedEvent.eventId.toLedgerString,
            contractId = exercisedEvent.contractId.coid,
            templateId = Some(LfEngineToApi.toApiIdentifier(exercisedEvent.templateId)),
            witnessParties = exercisedEvent.flatEventWitnesses
              .intersect(requestingParties.map(_.toString))
              .toSeq,
          )
        )
      )
  }

  object ToTransactionTree {
    def apply(
        tx: TransactionLogUpdate.Transaction,
        requestingParties: Set[Party],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Future[Option[TransactionTree]] = {
      val filteredForVisibility = tx.events
        .filter(TransactionTreePredicate(requestingParties))

      val treeEvents = filteredForVisibility
        .collect {
          case createdEvent: TransactionLogUpdate.CreatedEvent =>
            createdToTransactionTreeEvent(
              requestingParties,
              verbose,
              lfValueTranslation,
              createdEvent,
            )
          case exercisedEvent: TransactionLogUpdate.ExercisedEvent =>
            exercisedToTransactionTreeEvent(
              requestingParties,
              verbose,
              lfValueTranslation,
              exercisedEvent,
            )
        }

      if (treeEvents.isEmpty)
        Future.successful(Option.empty)
      else
        Future.traverse(treeEvents)(identity).map { treeEvents =>
          val visible = treeEvents.map(_.eventId)
          val visibleSet = visible.toSet
          val eventsById = treeEvents.iterator
            .map(e => e.eventId -> e.filterChildEventIds(visibleSet))
            .toMap

          // All event identifiers that appear as a child of another item in this response
          val children = eventsById.valuesIterator.flatMap(_.childEventIds).toSet

          // The roots for this request are all visible items
          // that are not a child of some other visible item
          val rootEventIds = visible.filterNot(children)

          Some(
            TransactionTree(
              transactionId = tx.transactionId,
              commandId = getCommandId(
                filteredForVisibility,
                tx.commandId,
                requestingParties,
              ), // TDT use submitters predicate to set commandId
              workflowId = tx.workflowId,
              effectiveAt = Some(instantToTimestamp(tx.effectiveAt)),
              offset = ApiOffset.toApiString(tx.offset),
              eventsById = eventsById,
              rootEventIds = rootEventIds,
              traceContext = None,
            )
          )
        }
    }

    private def exercisedToTransactionTreeEvent(
        requestingParties: Set[Party],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
        exercisedEvent: ExercisedEvent,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ) = {
      val eventualChoiceArgument = lfValueTranslation.toApiValue(
        exercisedEvent.exerciseArgument,
        verbose,
        "exercise argument",
        value =>
          lfValueTranslation.enricher
            .enrichChoiceArgument(
              exercisedEvent.templateId,
              Ref.Name.assertFromString(exercisedEvent.choice),
              value.value,
            ),
      )

      val eventualExerciseResult = exercisedEvent.exerciseResult
        .map(
          lfValueTranslation
            .toApiValue(
              _,
              verbose,
              "exercise result",
              value =>
                lfValueTranslation.enricher.enrichChoiceResult(
                  exercisedEvent.templateId,
                  Ref.Name.assertFromString(exercisedEvent.choice),
                  value.value,
                ),
            )
            .map(Some(_))
        )
        .getOrElse(Future.successful(None))

      for {
        choiceArgument <- eventualChoiceArgument
        maybeExerciseResult <- eventualExerciseResult
      } yield TreeEvent(
        TreeEvent.Kind.Exercised(
          v1.event.ExercisedEvent(
            eventId = exercisedEvent.eventId.toLedgerString,
            contractId = exercisedEvent.contractId.coid,
            templateId = Some(LfEngineToApi.toApiIdentifier(exercisedEvent.templateId)),
            choice = exercisedEvent.choice,
            choiceArgument = Some(choiceArgument),
            actingParties = exercisedEvent.actingParties.toSeq,
            consuming = exercisedEvent.consuming,
            witnessParties = exercisedEvent.treeEventWitnesses
              .intersect(requestingParties.map(_.toString))
              .toSeq,
            childEventIds = exercisedEvent.children,
            exerciseResult = maybeExerciseResult,
          )
        )
      )
    }

    private def createdToTransactionTreeEvent(
        requestingParties: Set[Party],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
        createdEvent: CreatedEvent,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ) = {
      val eventualContractKey = createdEvent.contractKey
        .map(
          lfValueTranslation
            .toApiValue(
              _,
              verbose,
              "create key",
              value =>
                lfValueTranslation.enricher
                  .enrichContractKey(createdEvent.templateId, value.value),
            )
            .map(Some(_))
        )
        .getOrElse(Future.successful(None))

      val eventualCreateArguments = lfValueTranslation.toApiRecord(
        createdEvent.createArgument,
        verbose,
        "create argument",
        value =>
          lfValueTranslation.enricher
            .enrichContract(createdEvent.templateId, value.value),
      )

      for {
        maybeContractKey <- eventualContractKey
        createArguments <- eventualCreateArguments
      } yield TreeEvent(
        TreeEvent.Kind.Created(
          com.daml.ledger.api.v1.event.CreatedEvent(
            eventId = createdEvent.eventId.toLedgerString,
            contractId = createdEvent.contractId.coid,
            templateId = Some(LfEngineToApi.toApiIdentifier(createdEvent.templateId)),
            contractKey = maybeContractKey,
            createArguments = Some(createArguments),
            witnessParties = createdEvent.treeEventWitnesses
              .intersect(requestingParties.map(_.toString))
              .toSeq,
            signatories = createdEvent.createSignatories.toSeq,
            observers = createdEvent.createObservers.toSeq,
            agreementText = createdEvent.createAgreementText.orElse(Some("")),
          )
        )
      )
    }

    private val TransactionTreePredicate: Set[Party] => TransactionLogUpdate.Event => Boolean =
      requestingParties => {
        case createdEvent: CreatedEvent =>
          createdEvent.treeEventWitnesses
            .intersect(requestingParties.map(_.toString))
            .nonEmpty
        case exercised: ExercisedEvent =>
          exercised.treeEventWitnesses
            .intersect(requestingParties.map(_.toString))
            .nonEmpty
      }
  }

  private def instantToTimestamp(t: Instant): Timestamp =
    Timestamp(seconds = t.getEpochSecond, nanos = t.getNano)

  private def getCommandId(
      events: Vector[TransactionLogUpdate.Event],
      commandId: String,
      requestingParties: Set[Party],
  ) =
    events
      .find(_.commandId != "")
      // TODO // TDT Double check condition for command id setting
      .filter(_.submitters.diff(requestingParties.map(_.toString)).isEmpty)
      .map(_ => commandId)
      .getOrElse("")
}
