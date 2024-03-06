// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.reassignment.{
  AssignedEvent as ApiAssignedEvent,
  Reassignment as ApiReassignment,
  UnassignedEvent as ApiUnassignedEvent,
}
import com.daml.ledger.api.v2.transaction.{
  Transaction as FlatTransaction,
  TransactionTree,
  TreeEvent,
}
import com.daml.ledger.api.v2.update_service.{
  GetTransactionResponse,
  GetTransactionTreeResponse,
  GetUpdateTreesResponse,
  GetUpdatesResponse,
}
import com.daml.ledger.api.v2.event as apiEvent
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, Party}
import com.daml.lf.transaction.{FatContractInstance, GlobalKeyWithMaintainers, Node}
import com.daml.lf.value.Value.ContractId
import com.digitalasset.canton.ledger.api.util.{LfEngineToApi, TimestampConversion}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.store.ScalaPbStreamingOptimizations.*
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate.{
  CreatedEvent,
  ExercisedEvent,
}
import com.digitalasset.canton.platform.store.utils.EventOps.TreeEventOps
import com.digitalasset.canton.platform.{ApiOffset, TemplatePartiesFilter, Value}
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext, Traced}
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

private[events] object TransactionLogUpdatesConversions {
  object ToFlatTransaction {
    def filter(
        wildcardParties: Set[Party],
        templateSpecificParties: Map[Identifier, Set[Party]],
        requestingParties: Set[Party],
    ): Traced[TransactionLogUpdate] => Option[Traced[TransactionLogUpdate]] = traced =>
      traced.traverse {
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
        case u: TransactionLogUpdate.ReassignmentAccepted =>
          Option.when(
            u.reassignmentInfo.hostedStakeholders.exists(party =>
              wildcardParties(party) || templateSpecificParties
                .get(u.reassignment match {
                  case TransactionLogUpdate.ReassignmentAccepted.Unassigned(unassign) =>
                    unassign.templateId
                  case TransactionLogUpdate.ReassignmentAccepted.Assigned(createdEvent) =>
                    createdEvent.templateId
                })
                .exists(_ contains party)
            )
          )(u)
      }

    def toGetTransactionsResponse(
        filter: TemplatePartiesFilter,
        eventProjectionProperties: EventProjectionProperties,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContextWithTrace,
        executionContext: ExecutionContext,
    ): Traced[TransactionLogUpdate] => Future[GetUpdatesResponse] = {
      case traced @ Traced(transactionAccepted: TransactionLogUpdate.TransactionAccepted) =>
        toFlatTransaction(
          transactionAccepted,
          filter,
          eventProjectionProperties,
          lfValueTranslation,
          traced.traceContext,
        )
          .map(transaction =>
            GetUpdatesResponse(GetUpdatesResponse.Update.Transaction(transaction))
              .withPrecomputedSerializedSize()
          )

      case traced @ Traced(reassignmentAccepted: TransactionLogUpdate.ReassignmentAccepted) =>
        toReassignment(
          reassignmentAccepted,
          filter.allFilterParties,
          eventProjectionProperties,
          lfValueTranslation,
          traced.traceContext,
        )
          .map(reassignment =>
            GetUpdatesResponse(GetUpdatesResponse.Update.Reassignment(reassignment))
              .withPrecomputedSerializedSize()
          )

      case illegal => throw new IllegalStateException(s"$illegal is not expected here")
    }

    def toGetFlatTransactionResponse(
        transactionLogUpdate: Traced[TransactionLogUpdate],
        requestingParties: Set[Party],
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContextWithTrace,
        executionContext: ExecutionContext,
    ): Future[Option[GetTransactionResponse]] =
      filter(requestingParties, Map.empty, requestingParties)(transactionLogUpdate)
        .collect {
          case traced @ Traced(transactionAccepted: TransactionLogUpdate.TransactionAccepted) =>
            toFlatTransaction(
              transactionAccepted = transactionAccepted,
              filter = TemplatePartiesFilter(Map.empty, requestingParties),
              eventProjectionProperties = EventProjectionProperties(
                verbose = true,
                wildcardWitnesses = requestingParties.map(_.toString),
              ),
              lfValueTranslation = lfValueTranslation,
              traceContext = traced.traceContext,
            )
        }
        .map(_.map(flatTransaction => Some(GetTransactionResponse(Some(flatTransaction)))))
        .getOrElse(Future.successful(None))

    private def toFlatTransaction(
        transactionAccepted: TransactionLogUpdate.TransactionAccepted,
        filter: TemplatePartiesFilter,
        eventProjectionProperties: EventProjectionProperties,
        lfValueTranslation: LfValueTranslation,
        traceContext: TraceContext,
    )(implicit
        loggingContext: LoggingContextWithTrace,
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
              updateId = transactionAccepted.transactionId,
              commandId = transactionAccepted.commandId,
              workflowId = transactionAccepted.workflowId,
              effectiveAt = Some(TimestampConversion.fromLf(transactionAccepted.effectiveAt)),
              events = flatEvents,
              offset = ApiOffset.toApiString(transactionAccepted.offset),
              domainId = transactionAccepted.domainId.getOrElse(""),
              traceContext = SerializableTraceContext(traceContext).toDamlProtoOpt,
              recordTime = Some(TimestampConversion.fromLf(transactionAccepted.recordTime)),
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
        loggingContext: LoggingContextWithTrace,
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
    ): Traced[TransactionLogUpdate] => Option[Traced[TransactionLogUpdate]] = traced =>
      traced.traverse {
        case transaction: TransactionLogUpdate.TransactionAccepted =>
          val filteredForVisibility =
            transaction.events.filter(transactionTreePredicate(requestingParties))

          Option.when(filteredForVisibility.nonEmpty)(
            transaction.copy(events = filteredForVisibility)
          )
        case _: TransactionLogUpdate.TransactionRejected => None
        case u: TransactionLogUpdate.ReassignmentAccepted =>
          Option.when(
            u.reassignmentInfo.hostedStakeholders.exists(requestingParties)
          )(u)
      }

    def toGetTransactionResponse(
        transactionLogUpdate: Traced[TransactionLogUpdate],
        requestingParties: Set[Party],
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContextWithTrace,
        executionContext: ExecutionContext,
    ): Future[Option[GetTransactionTreeResponse]] =
      filter(requestingParties)(transactionLogUpdate)
        .collect { case traced @ Traced(tx: TransactionLogUpdate.TransactionAccepted) =>
          toTransactionTree(
            transactionAccepted = tx,
            requestingParties,
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              wildcardWitnesses = requestingParties.map(_.toString),
            ),
            lfValueTranslation = lfValueTranslation,
            traceContext = traced.traceContext,
          )
        }
        .map(_.map(transactionTree => Some(GetTransactionTreeResponse(Some(transactionTree)))))
        .getOrElse(Future.successful(None))

    def toGetTransactionTreesResponse(
        requestingParties: Set[Party],
        eventProjectionProperties: EventProjectionProperties,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContextWithTrace,
        executionContext: ExecutionContext,
    ): Traced[TransactionLogUpdate] => Future[GetUpdateTreesResponse] = {
      case traced @ Traced(transactionAccepted: TransactionLogUpdate.TransactionAccepted) =>
        toTransactionTree(
          transactionAccepted,
          requestingParties,
          eventProjectionProperties,
          lfValueTranslation,
          traced.traceContext,
        )
          .map(txTree =>
            GetUpdateTreesResponse(GetUpdateTreesResponse.Update.TransactionTree(txTree))
              .withPrecomputedSerializedSize()
          )
      case traced @ Traced(reassignmentAccepted: TransactionLogUpdate.ReassignmentAccepted) =>
        toReassignment(
          reassignmentAccepted,
          requestingParties,
          eventProjectionProperties,
          lfValueTranslation,
          traced.traceContext,
        )
          .map(reassignment =>
            GetUpdateTreesResponse(GetUpdateTreesResponse.Update.Reassignment(reassignment))
              .withPrecomputedSerializedSize()
          )

      case illegal => throw new IllegalStateException(s"$illegal is not expected here")
    }

    private def toTransactionTree(
        transactionAccepted: TransactionLogUpdate.TransactionAccepted,
        requestingParties: Set[Party],
        eventProjectionProperties: EventProjectionProperties,
        lfValueTranslation: LfValueTranslation,
        traceContext: TraceContext,
    )(implicit
        loggingContext: LoggingContextWithTrace,
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
            val visibleSet = visible.toSet
            val eventsById = treeEvents.iterator
              .map(e => e.eventId -> e.filterChildEventIds(visibleSet))
              .toMap

            // All event identifiers that appear as a child of another item in this response
            val children = eventsById.valuesIterator.flatMap(_.childEventIds).toSet

            // The roots for this request are all visible items
            // that are not a child of some other visible item
            val rootEventIds = visible.filterNot(children)

            TransactionTree(
              updateId = transactionAccepted.transactionId,
              commandId = getCommandId(transactionAccepted.events, requestingParties),
              workflowId = transactionAccepted.workflowId,
              effectiveAt = Some(TimestampConversion.fromLf(transactionAccepted.effectiveAt)),
              offset = ApiOffset.toApiString(transactionAccepted.offset),
              eventsById = eventsById,
              rootEventIds = rootEventIds,
              domainId = transactionAccepted.domainId.getOrElse(""),
              traceContext = SerializableTraceContext(traceContext).toDamlProtoOpt,
              recordTime = Some(TimestampConversion.fromLf(transactionAccepted.recordTime)),
            )
          }
      }

    private def toTransactionTreeEvent(
        requestingParties: Set[Party],
        eventProjectionProperties: EventProjectionProperties,
        lfValueTranslation: LfValueTranslation,
    )(event: TransactionLogUpdate.Event)(implicit
        loggingContext: LoggingContextWithTrace,
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
        loggingContext: LoggingContextWithTrace,
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
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[apiEvent.CreatedEvent] = {

    def getFatContractInstance: Option[Right[Nothing, FatContractInstance]] =
      createdEvent.driverMetadata
        .filter(_.nonEmpty)
        .map(driverMetadataBytes =>
          Right(
            FatContractInstance.fromCreateNode(
              Node.Create(
                coid = createdEvent.contractId,
                templateId = createdEvent.templateId,
                packageName = createdEvent.packageName,
                arg = createdEvent.createArgument.unversioned,
                signatories = createdEvent.createSignatories,
                stakeholders = createdEvent.createSignatories ++ createdEvent.createObservers,
                keyOpt = createdEvent.createKey.flatMap(k =>
                  createdEvent.createKeyMaintainers.map(GlobalKeyWithMaintainers(k, _))
                ),
                version = createdEvent.createArgument.version,
              ),
              createTime = createdEvent.ledgerEffectiveTime,
              cantonData = driverMetadataBytes,
            )
          )
        )

    lfValueTranslation
      .toApiContractData(
        value = createdEvent.createArgument,
        key = createdEvent.contractKey,
        templateId = createdEvent.templateId,
        witnesses = requestingParties.view.filter(createdWitnesses(createdEvent)).toSet,
        eventProjectionProperties = eventProjectionProperties,
        fatContractInstance = getFatContractInstance,
      )
      .map(apiContractData =>
        apiEvent.CreatedEvent(
          eventId = createdEvent.eventId.toLedgerString,
          contractId = createdEvent.contractId.coid,
          templateId = Some(LfEngineToApi.toApiIdentifier(createdEvent.templateId)),
          packageName = createdEvent.packageName,
          contractKey = apiContractData.contractKey,
          createArguments = apiContractData.createArguments,
          createdEventBlob = apiContractData.createdEventBlob.getOrElse(ByteString.EMPTY),
          interfaceViews = apiContractData.interfaceViews,
          witnessParties = requestingParties.view.filter(createdWitnesses(createdEvent)).toSeq,
          signatories = createdEvent.createSignatories.toSeq,
          observers = createdEvent.createObservers.toSeq,
          createdAt = Some(TimestampConversion.fromLf(createdEvent.ledgerEffectiveTime)),
        )
      )
  }

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

  private def toReassignment(
      reassignmentAccepted: TransactionLogUpdate.ReassignmentAccepted,
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
      traceContext: TraceContext,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[ApiReassignment] = {
    val stringRequestingParties = requestingParties.map(_.toString)
    val info = reassignmentAccepted.reassignmentInfo
    (reassignmentAccepted.reassignment match {
      case TransactionLogUpdate.ReassignmentAccepted.Assigned(createdEvent) =>
        createdToApiCreatedEvent(
          requestingParties = requestingParties,
          eventProjectionProperties = eventProjectionProperties,
          lfValueTranslation = lfValueTranslation,
          createdEvent = createdEvent,
          createdWitnesses = _.flatEventWitnesses,
        ).map(createdEvent =>
          ApiReassignment.Event.AssignedEvent(
            ApiAssignedEvent(
              source = info.sourceDomain.unwrap.toProtoPrimitive,
              target = info.targetDomain.unwrap.toProtoPrimitive,
              unassignId = info.unassignId.toMicros.toString,
              submitter = info.submitter.getOrElse(""),
              reassignmentCounter = info.reassignmentCounter,
              createdEvent = Some(createdEvent),
            )
          )
        )

      case TransactionLogUpdate.ReassignmentAccepted.Unassigned(unassign) =>
        Future.successful(
          ApiReassignment.Event.UnassignedEvent(
            ApiUnassignedEvent(
              source = info.sourceDomain.unwrap.toProtoPrimitive,
              target = info.targetDomain.unwrap.toProtoPrimitive,
              unassignId = info.unassignId.toMicros.toString,
              submitter = info.submitter.getOrElse(""),
              reassignmentCounter = info.reassignmentCounter,
              contractId = unassign.contractId.coid,
              templateId = Some(LfEngineToApi.toApiIdentifier(unassign.templateId)),
              assignmentExclusivity =
                unassign.assignmentExclusivity.map(TimestampConversion.fromLf),
              witnessParties = reassignmentAccepted.reassignmentInfo.hostedStakeholders
                .filter(requestingParties),
            )
          )
        )
    }).map(event =>
      ApiReassignment(
        updateId = reassignmentAccepted.updateId,
        commandId = reassignmentAccepted.completionDetails
          .filter(_.submitters.exists(stringRequestingParties))
          .flatMap(_.completionStreamResponse.completion)
          .map(_.commandId)
          .getOrElse(""),
        workflowId = reassignmentAccepted.workflowId,
        offset = ApiOffset.toApiString(reassignmentAccepted.offset),
        event = event,
        traceContext = SerializableTraceContext(traceContext).toDamlProtoOpt,
        recordTime = Some(TimestampConversion.fromLf(reassignmentAccepted.recordTime)),
      )
    )
  }

}
