// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.time.Instant

import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.transaction.{
  TransactionTree,
  TreeEvent,
  Transaction => FlatTransaction,
}
import com.daml.ledger.api.v1.{event => apiEvent}
import com.daml.logging.LoggingContext
import com.daml.platform.ApiOffset
import com.daml.platform.api.v1.event.EventOps.TreeEventOps
import com.daml.platform.store.appendonlydao.events
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.{CreatedEvent, ExercisedEvent}
import com.google.protobuf.timestamp.Timestamp

import scala.concurrent.{ExecutionContext, Future}

private[events] object TransactionLogUpdatesConversions {
  object ToFlatTransaction {
    def apply(
        tx: TransactionLogUpdate.Transaction,
        filter: FilterRelation,
        wildcardParties: Set[String],
        templateSpecificParties: Map[events.Identifier, Set[String]],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Future[Option[FlatTransaction]] = {
      val transactionEvents = tx.events
      val filtered = transactionEvents.filter(
        FlatTransactionPredicate(_, wildcardParties, templateSpecificParties)
      )

      filtered.headOption
        .map { first =>
          val events = removeTransient(filtered)
          val requestingParties = filter.keySet.map(_.toString)

          Future
            .traverse(events)(toFlatEvent(_, requestingParties, verbose, lfValueTranslation))
            .map(_.collect { case Some(value) => value })
            .map { flatEvents =>
              // Allows emitting flat transactions with no events, a use-case needed
              // for the functioning of DAML triggers.
              // (more details in https://github.com/digital-asset/daml/issues/6975)
              if (flatEvents.nonEmpty || first.commandId.nonEmpty) {
                Some(
                  FlatTransaction(
                    transactionId = first.transactionId,
                    commandId = getCommandId(events, requestingParties),
                    workflowId = first.workflowId,
                    effectiveAt = Some(instantToTimestamp(first.ledgerEffectiveTime)),
                    events = flatEvents,
                    offset = ApiOffset.toApiString(tx.offset),
                  )
                )
              } else None
            }
        }
        .getOrElse(Future.successful(None))
    }

    private def removeTransient(aux: Vector[TransactionLogUpdate.Event]) = {
      val permanent = aux.foldLeft(Set.empty[ContractId]) {
        case (contractIds, event) if !contractIds(event.contractId) =>
          contractIds + event.contractId
        case (contractIds, event: ExercisedEvent) if event.consuming =>
          contractIds - event.contractId
        case (contractId, _) =>
          throw new RuntimeException(s"Unexpected non-consuming event for contractId $contractId")
      }
      aux.filter(ev => permanent(ev.contractId))
    }

    private val FlatTransactionPredicate =
      (
          event: TransactionLogUpdate.Event,
          wildcardParties: Set[String],
          templateSpecificParties: Map[events.Identifier, Set[String]],
      ) => {

        val stakeholdersMatchingParties =
          event.flatEventWitnesses.exists(wildcardParties)

        stakeholdersMatchingParties || templateSpecificParties
          .get(event.templateId)
          .exists(_.exists(event.flatEventWitnesses))
      }

    private def toFlatEvent(
        event: TransactionLogUpdate.Event,
        requestingParties: Set[String],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Future[Option[apiEvent.Event]] =
      event match {
        case createdEvent: TransactionLogUpdate.CreatedEvent =>
          createdToFlatEvent(requestingParties, verbose, lfValueTranslation, createdEvent)
            .map(Some(_))

        case exercisedEvent: TransactionLogUpdate.ExercisedEvent if exercisedEvent.consuming =>
          Future.successful(Some(exercisedToFlatEvent(requestingParties, exercisedEvent)))

        case _ => Future.successful(None)
      }

    private def createdToFlatEvent(
        requestingParties: Set[String],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
        createdEvent: CreatedEvent,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Future[Event] = {
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
      } yield {
        apiEvent.Event(
          apiEvent.Event.Event.Created(
            apiEvent.CreatedEvent(
              eventId = createdEvent.eventIdLedgerString,
              contractId = createdEvent.contractId.coid,
              templateId = Some(createdEvent.templateIdApi),
              contractKey = maybeContractKey,
              createArguments = Some(createArguments),
              witnessParties = requestingParties.view.filter(createdEvent.flatEventWitnesses).toSeq,
              signatories = createdEvent.createSignatories.toSeq,
              observers = createdEvent.createObservers.toSeq,
              agreementText = createdEvent.createAgreementText.orElse(Some("")),
            )
          )
        )
      }
    }

    private def exercisedToFlatEvent(
        requestingParties: Set[String],
        exercisedEvent: ExercisedEvent,
    ): apiEvent.Event =
      apiEvent.Event(
        apiEvent.Event.Event.Archived(
          apiEvent.ArchivedEvent(
            eventId = exercisedEvent.eventIdLedgerString,
            contractId = exercisedEvent.contractId.coid,
            templateId = Some(exercisedEvent.templateIdApi),
            witnessParties = requestingParties.view.filter(exercisedEvent.flatEventWitnesses).toSeq,
          )
        )
      )
  }

  object ToTransactionTree {
    def apply(
        tx: TransactionLogUpdate.Transaction,
        requestingParties: Set[String],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Future[Option[TransactionTree]] = {
      val filteredForVisibility = tx.events
        .filter(TransactionTreePredicate(requestingParties))

      if (filteredForVisibility.isEmpty) Future.successful(None)
      else
        Future
          .traverse(filteredForVisibility)(
            toTransactionTreeEvent(requestingParties, verbose, lfValueTranslation)
          )
          .map { treeEvents =>
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
                commandId = getCommandId(filteredForVisibility, requestingParties),
                workflowId = tx.workflowId,
                effectiveAt = Some(instantToTimestamp(tx.effectiveAt)),
                offset = ApiOffset.toApiString(tx.offset),
                eventsById = eventsById,
                rootEventIds = rootEventIds,
              )
            )
          }
    }

    private def toTransactionTreeEvent(
        requestingParties: Set[String],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
    )(event: TransactionLogUpdate.Event)(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Future[TreeEvent] =
      event match {
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

    private def exercisedToTransactionTreeEvent(
        requestingParties: Set[String],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
        exercisedEvent: ExercisedEvent,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ) = {
      val choiceArgumentEnricher = (value: Value) =>
        lfValueTranslation.enricher
          .enrichChoiceArgument(
            exercisedEvent.templateId,
            exercisedEvent.choiceRefName,
            value.value,
          )

      val eventualChoiceArgument = lfValueTranslation.toApiValue(
        exercisedEvent.exerciseArgument,
        verbose,
        "exercise argument",
        choiceArgumentEnricher,
      )

      val eventualExerciseResult = exercisedEvent.exerciseResult
        .map { exerciseResult =>
          val choiceResultEnricher = (value: Value) =>
            lfValueTranslation.enricher.enrichChoiceResult(
              exercisedEvent.templateId,
              exercisedEvent.choiceRefName,
              value.value,
            )

          lfValueTranslation
            .toApiValue(
              value = exerciseResult,
              verbose = verbose,
              attribute = "exercise result",
              enrich = choiceResultEnricher,
            )
            .map(Some(_))
        }
        .getOrElse(Future.successful(None))

      for {
        choiceArgument <- eventualChoiceArgument
        maybeExerciseResult <- eventualExerciseResult
      } yield TreeEvent(
        TreeEvent.Kind.Exercised(
          apiEvent.ExercisedEvent(
            eventId = exercisedEvent.eventIdLedgerString,
            contractId = exercisedEvent.contractId.coid,
            templateId = Some(exercisedEvent.templateIdApi),
            choice = exercisedEvent.choice,
            choiceArgument = Some(choiceArgument),
            actingParties = exercisedEvent.actingParties.toSeq,
            consuming = exercisedEvent.consuming,
            witnessParties = requestingParties.view.filter(exercisedEvent.treeEventWitnesses).toSeq,
            childEventIds = exercisedEvent.children,
            exerciseResult = maybeExerciseResult,
          )
        )
      )
    }

    private def createdToTransactionTreeEvent(
        requestingParties: Set[String],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
        createdEvent: CreatedEvent,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ) = {

      val eventualContractKey = createdEvent.contractKey
        .map { contractKey =>
          val contractKeyEnricher = (value: Value) =>
            lfValueTranslation.enricher.enrichContractKey(createdEvent.templateId, value.value)

          lfValueTranslation
            .toApiValue(
              value = contractKey,
              verbose = verbose,
              attribute = "create key",
              enrich = contractKeyEnricher,
            )
            .map(Some(_))
        }
        .getOrElse(Future.successful(None))

      val contractEnricher = (value: Value) =>
        lfValueTranslation.enricher.enrichContract(createdEvent.templateId, value.value)

      val eventualCreateArguments = lfValueTranslation.toApiRecord(
        value = createdEvent.createArgument,
        verbose = verbose,
        attribute = "create argument",
        enrich = contractEnricher,
      )

      for {
        maybeContractKey <- eventualContractKey
        createArguments <- eventualCreateArguments
      } yield TreeEvent(
        TreeEvent.Kind.Created(
          apiEvent.CreatedEvent(
            eventId = createdEvent.eventIdLedgerString,
            contractId = createdEvent.contractId.coid,
            templateId = Some(createdEvent.templateIdApi),
            contractKey = maybeContractKey,
            createArguments = Some(createArguments),
            witnessParties = requestingParties.view.filter(createdEvent.treeEventWitnesses).toSeq,
            signatories = createdEvent.createSignatories.toSeq,
            observers = createdEvent.createObservers.toSeq,
            agreementText = createdEvent.createAgreementText.orElse(Some("")),
          )
        )
      )
    }

    private val TransactionTreePredicate: Set[String] => TransactionLogUpdate.Event => Boolean =
      requestingParties => {
        case createdEvent: CreatedEvent => requestingParties.exists(createdEvent.treeEventWitnesses)
        case exercised: ExercisedEvent => requestingParties.exists(exercised.treeEventWitnesses)
      }
  }

  private def instantToTimestamp(t: Instant): Timestamp =
    Timestamp(seconds = t.getEpochSecond, nanos = t.getNano)

  private def getCommandId(
      flatTransactionEvents: Vector[TransactionLogUpdate.Event],
      requestingParties: Set[String],
  ) =
    flatTransactionEvents
      .collectFirst {
        case event if requestingParties.exists(event.submitters) =>
          event.commandId
      }
      .getOrElse("")
}
