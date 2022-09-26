// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.api.util.TimestampConversion
import com.daml.api.util.TimestampConversion.fromInstant
import com.daml.ledger.api.v1.contract_metadata.ContractMetadata
import com.daml.ledger.api.v1.transaction.{
  TransactionTree,
  TreeEvent,
  Transaction => FlatTransaction,
}
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.ledger.api.v1.{event => apiEvent}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, Party}
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext
import com.daml.platform.api.v1.event.EventOps.TreeEventOps
import com.daml.platform.participant.util.LfEngineToApi
import com.daml.platform.store.dao.EventProjectionProperties
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.{CreatedEvent, ExercisedEvent}
import com.daml.platform.{ApiOffset, TemplatePartiesFilter, Value}
import com.google.protobuf.timestamp.Timestamp

import scala.concurrent.{ExecutionContext, Future}
import com.google.protobuf.ByteString

private[events] object TransactionLogUpdatesConversions {
  object ToFlatTransaction {
    def filter(
        wildcardParties: Set[Party],
        templateSpecificParties: Map[Identifier, Set[Party]],
        requestingParties: Set[Party],
    ): TransactionLogUpdate => Option[
      TransactionLogUpdate.TransactionAccepted
    ] = {
      case transaction: TransactionLogUpdate.TransactionAccepted =>
        val flatTransactionEvents = transaction.events.collect {
          case createdEvent: TransactionLogUpdate.CreatedEvent => createdEvent
          case exercisedEvent: TransactionLogUpdate.ExercisedEvent if exercisedEvent.consuming =>
            exercisedEvent
        }
        val filteredFlatEvents = flatTransactionEvents
          .filter(flatTransactionPredicate(wildcardParties, templateSpecificParties))
        val commandId = getCommandId(filteredFlatEvents, requestingParties)
        val nonTransient = removeTransient(filteredFlatEvents)
        // Allows emitting flat transactions with no events, a use-case needed
        // for the functioning of Daml triggers.
        // (more details in https://github.com/digital-asset/daml/issues/6975)
        Option.when(nonTransient.nonEmpty || commandId.nonEmpty)(
          transaction.copy(
            commandId = commandId,
            events = nonTransient,
          )
        )
      case _: TransactionLogUpdate.TransactionRejected => None
    }

    def toGetTransactionsResponse(
        filter: TemplatePartiesFilter,
        eventProjectionProperties: EventProjectionProperties,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): TransactionLogUpdate.TransactionAccepted => Future[GetTransactionsResponse] =
      toFlatTransaction(
        _,
        filter,
        eventProjectionProperties,
        lfValueTranslation,
      )
        .map(transaction => GetTransactionsResponse(Seq(transaction)))

    def toGetFlatTransactionResponse(
        transactionLogUpdate: TransactionLogUpdate,
        requestingParties: Set[Party],
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Future[Option[GetFlatTransactionResponse]] =
      filter(requestingParties, Map.empty, requestingParties)(transactionLogUpdate)
        .map(transactionAccepted =>
          toFlatTransaction(
            transactionAccepted = transactionAccepted,
            filter = TemplatePartiesFilter(Map.empty, requestingParties),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              witnessTemplateIdFilter = requestingParties.map(_ -> Set.empty[Ref.Identifier]).toMap,
            ),
            lfValueTranslation = lfValueTranslation,
          )
        )
        .map(_.map(flatTransaction => Some(GetFlatTransactionResponse(Some(flatTransaction)))))
        .getOrElse(Future.successful(None))

    private def toFlatTransaction(
        transactionAccepted: TransactionLogUpdate.TransactionAccepted,
        filter: TemplatePartiesFilter,
        eventProjectionProperties: EventProjectionProperties,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Future[FlatTransaction] =
      Future.delegate {
        Future
          .traverse(transactionAccepted.events)(event =>
            toFlatEvent(
              event,
              filter.allFilterParties,
              eventProjectionProperties,
              lfValueTranslation,
            )
          )
          .map(flatEvents =>
            FlatTransaction(
              transactionId = transactionAccepted.transactionId,
              commandId = transactionAccepted.commandId,
              workflowId = transactionAccepted.workflowId,
              effectiveAt = Some(timestampToTimestamp(transactionAccepted.effectiveAt)),
              events = flatEvents,
              offset = ApiOffset.toApiString(transactionAccepted.offset),
            )
          )
      }

    private def removeTransient(aux: Vector[TransactionLogUpdate.Event]) = {
      val permanent = aux.foldLeft(Set.empty[ContractId]) {
        case (contractIds, event) if !contractIds(event.contractId) =>
          contractIds + event.contractId
        case (contractIds, event: ExercisedEvent) if event.consuming =>
          contractIds - event.contractId
        case (contractIds, _) =>
          val prettyCids = contractIds.iterator.map(_.coid).mkString(", ")
          throw new RuntimeException(s"Unexpected non-consuming event for contractIds $prettyCids")
      }
      aux.filter(ev => permanent(ev.contractId))
    }

    private def flatTransactionPredicate(
        wildcardParties: Set[Party],
        templateSpecificParties: Map[Identifier, Set[Party]],
    )(event: TransactionLogUpdate.Event) = {
      val stakeholdersMatchingParties =
        event.flatEventWitnesses.exists(wildcardParties)

      stakeholdersMatchingParties || templateSpecificParties
        .get(event.templateId)
        .exists(_.exists(event.flatEventWitnesses))
    }

    private def toFlatEvent(
        event: TransactionLogUpdate.Event,
        requestingParties: Set[Party],
        eventProjectionProperties: EventProjectionProperties,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Future[apiEvent.Event] =
      event match {
        case createdEvent: TransactionLogUpdate.CreatedEvent =>
          createdToApiCreatedEvent(
            requestingParties,
            eventProjectionProperties,
            lfValueTranslation,
            createdEvent,
            _.flatEventWitnesses,
          ).map(apiCreatedEvent => apiEvent.Event(apiEvent.Event.Event.Created(apiCreatedEvent)))

        case exercisedEvent: TransactionLogUpdate.ExercisedEvent if exercisedEvent.consuming =>
          Future.successful(exercisedToFlatEvent(requestingParties, exercisedEvent))

        case _ => Future.failed(new RuntimeException("Not a flat transaction event"))
      }

    private def exercisedToFlatEvent(
        requestingParties: Set[Party],
        exercisedEvent: ExercisedEvent,
    ): apiEvent.Event =
      apiEvent.Event(
        apiEvent.Event.Event.Archived(
          apiEvent.ArchivedEvent(
            eventId = exercisedEvent.eventId.toLedgerString,
            contractId = exercisedEvent.contractId.coid,
            templateId = Some(LfEngineToApi.toApiIdentifier(exercisedEvent.templateId)),
            witnessParties =
              requestingParties.iterator.filter(exercisedEvent.flatEventWitnesses).toSeq,
          )
        )
      )
  }

  object ToTransactionTree {
    def filter(
        requestingParties: Set[Party]
    ): TransactionLogUpdate => Option[TransactionLogUpdate.TransactionAccepted] = {
      case transaction: TransactionLogUpdate.TransactionAccepted =>
        val filteredForVisibility =
          transaction.events.filter(transactionTreePredicate(requestingParties))

        Option.when(filteredForVisibility.nonEmpty)(
          transaction.copy(events = filteredForVisibility)
        )
      case _: TransactionLogUpdate.TransactionRejected => None
    }

    def toGetTransactionResponse(
        transactionLogUpdate: TransactionLogUpdate,
        requestingParties: Set[Party],
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Future[Option[GetTransactionResponse]] =
      filter(requestingParties)(transactionLogUpdate)
        .map(tx =>
          toTransactionTree(
            transactionAccepted = tx,
            requestingParties,
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              witnessTemplateIdFilter = requestingParties.map(_ -> Set.empty[Ref.Identifier]).toMap,
            ),
            lfValueTranslation = lfValueTranslation,
          )
        )
        .map(_.map(transactionTree => Some(GetTransactionResponse(Some(transactionTree)))))
        .getOrElse(Future.successful(None))

    def toGetTransactionTreesResponse(
        requestingParties: Set[Party],
        eventProjectionProperties: EventProjectionProperties,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): TransactionLogUpdate.TransactionAccepted => Future[GetTransactionTreesResponse] =
      toTransactionTree(_, requestingParties, eventProjectionProperties, lfValueTranslation)
        .map(txTree => GetTransactionTreesResponse(Seq(txTree)))

    private def toTransactionTree(
        transactionAccepted: TransactionLogUpdate.TransactionAccepted,
        requestingParties: Set[Party],
        eventProjectionProperties: EventProjectionProperties,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Future[TransactionTree] =
      Future.delegate {
        Future
          .traverse(transactionAccepted.events)(event =>
            toTransactionTreeEvent(
              requestingParties,
              eventProjectionProperties,
              lfValueTranslation,
            )(event)
          )
          .map { treeEvents =>
            val visible = treeEvents.map(_.eventId)
            val visibleOrder = visible.view.zipWithIndex.toMap
            val eventsById = treeEvents.iterator
              .map(e =>
                e.eventId -> e
                  .filterChildEventIds(visibleOrder.contains)
                  // childEventIds need to be returned in the event order in the original transaction.
                  // Unfortunately, we did not store them ordered in the past so we have to sort it to recover this order.
                  // The order is determined by the order of the events, which follows the event order of the original transaction.
                  .sortChildEventIdsBy(visibleOrder)
              )
              .toMap

            // All event identifiers that appear as a child of another item in this response
            val children = eventsById.valuesIterator.flatMap(_.childEventIds).toSet

            // The roots for this request are all visible items
            // that are not a child of some other visible item
            val rootEventIds = visible.filterNot(children)

            TransactionTree(
              transactionId = transactionAccepted.transactionId,
              commandId = getCommandId(transactionAccepted.events, requestingParties),
              workflowId = transactionAccepted.workflowId,
              effectiveAt = Some(timestampToTimestamp(transactionAccepted.effectiveAt)),
              offset = ApiOffset.toApiString(transactionAccepted.offset),
              eventsById = eventsById,
              rootEventIds = rootEventIds,
            )
          }
      }

    private def toTransactionTreeEvent(
        requestingParties: Set[Party],
        eventProjectionProperties: EventProjectionProperties,
        lfValueTranslation: LfValueTranslation,
    )(event: TransactionLogUpdate.Event)(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Future[TreeEvent] =
      event match {
        case createdEvent: TransactionLogUpdate.CreatedEvent =>
          createdToApiCreatedEvent(
            requestingParties,
            eventProjectionProperties,
            lfValueTranslation,
            createdEvent,
            _.treeEventWitnesses,
          ).map(apiCreatedEvent => TreeEvent(TreeEvent.Kind.Created(apiCreatedEvent)))

        case exercisedEvent: TransactionLogUpdate.ExercisedEvent =>
          exercisedToTransactionTreeEvent(
            requestingParties,
            eventProjectionProperties.verbose,
            lfValueTranslation,
            exercisedEvent,
          )
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
      val choiceArgumentEnricher = (value: Value) =>
        lfValueTranslation.enricher
          .enrichChoiceArgument(
            exercisedEvent.templateId,
            exercisedEvent.interfaceId,
            Ref.Name.assertFromString(exercisedEvent.choice),
            value.unversioned,
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
              exercisedEvent.interfaceId,
              Ref.Name.assertFromString(exercisedEvent.choice),
              value.unversioned,
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
            eventId = exercisedEvent.eventId.toLedgerString,
            contractId = exercisedEvent.contractId.coid,
            templateId = Some(LfEngineToApi.toApiIdentifier(exercisedEvent.templateId)),
            interfaceId = exercisedEvent.interfaceId.map(LfEngineToApi.toApiIdentifier),
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

    private def transactionTreePredicate(
        requestingParties: Set[Party]
    ): TransactionLogUpdate.Event => Boolean = {
      case createdEvent: CreatedEvent => requestingParties.exists(createdEvent.treeEventWitnesses)
      case exercised: ExercisedEvent => requestingParties.exists(exercised.treeEventWitnesses)
      case _ => false
    }
  }

  private def createdToApiCreatedEvent(
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
      createdEvent: CreatedEvent,
      createdWitnesses: CreatedEvent => Set[Party],
  )(implicit
      loggingContext: LoggingContext,
      executionContext: ExecutionContext,
  ): Future[apiEvent.CreatedEvent] = {
    lfValueTranslation
      .toApiContractData(
        value = createdEvent.createArgument,
        key = createdEvent.contractKey,
        templateId = createdEvent.templateId,
        witnesses = requestingParties.view.filter(createdWitnesses(createdEvent)).toSet,
        eventProjectionProperties = eventProjectionProperties,
      )
      .map(apiContractData =>
        apiEvent.CreatedEvent(
          eventId = createdEvent.eventId.toLedgerString,
          contractId = createdEvent.contractId.coid,
          templateId = Some(LfEngineToApi.toApiIdentifier(createdEvent.templateId)),
          contractKey = apiContractData.contractKey,
          createArguments = apiContractData.createArguments,
          interfaceViews = apiContractData.interfaceViews,
          witnessParties = requestingParties.view.filter(createdWitnesses(createdEvent)).toSeq,
          signatories = createdEvent.createSignatories.toSeq,
          observers = createdEvent.createObservers.toSeq,
          agreementText = createdEvent.createAgreementText.orElse(Some("")),
          metadata = Some(
            ContractMetadata(
              createdAt = Some(TimestampConversion.fromLf(createdEvent.ledgerEffectiveTime)),
              contractKeyHash =
                createdEvent.createKeyHash.fold(ByteString.EMPTY)(_.bytes.toByteString),
              // TODO ED: Store driver metadata in the database
              driverMetadata = ByteString.EMPTY,
            )
          ),
        )
      )
  }

  private def timestampToTimestamp(t: com.daml.lf.data.Time.Timestamp): Timestamp =
    fromInstant(t.toInstant)

  private def getCommandId(
      flatTransactionEvents: Vector[TransactionLogUpdate.Event],
      requestingParties: Set[Party],
  ) =
    flatTransactionEvents
      .collectFirst {
        case event if requestingParties.exists(event.submitters) =>
          event.commandId
      }
      .getOrElse("")
}
