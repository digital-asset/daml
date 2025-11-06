// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event as apiEvent
import com.daml.ledger.api.v2.reassignment.ReassignmentEvent.Event.{
  Assigned as ApiAssigned,
  Unassigned as ApiUnassigned,
}
import com.daml.ledger.api.v2.reassignment.{
  AssignedEvent as ApiAssignedEvent,
  Reassignment as ApiReassignment,
  ReassignmentEvent as ApiReassignmentEvent,
  UnassignedEvent as ApiUnassignedEvent,
}
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
import com.daml.ledger.api.v2.transaction.Transaction as FlatTransaction
import com.daml.ledger.api.v2.update_service.{GetUpdateResponse, GetUpdatesResponse}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.ledger.api.util.{LfEngineToApi, TimestampConversion}
import com.digitalasset.canton.ledger.api.{ParticipantAuthorizationFormat, TransactionShape}
import com.digitalasset.canton.ledger.participant.state.Reassignment
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.store.ScalaPbStreamingOptimizations.*
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties
import com.digitalasset.canton.platform.store.dao.events.EventsTable.TransactionConversions.toTopologyEvent
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate.{
  CreatedEvent,
  ExercisedEvent,
}
import com.digitalasset.canton.platform.{
  InternalTransactionFormat,
  InternalUpdateFormat,
  PackageId as LfPackageId,
  TemplatePartiesFilter,
  Value,
}
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.SerializableTraceContextExtension
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.daml.lf.data.Ref.{IdentifierConverter, NameTypeConRef, Party}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.transaction.{
  CreationTime,
  FatContractInstance,
  GlobalKeyWithMaintainers,
  Node,
}

import scala.concurrent.{ExecutionContext, Future}

private[events] object TransactionLogUpdatesConversions {

  def filter(
      internalUpdateFormat: InternalUpdateFormat
  ): TransactionLogUpdate => Option[TransactionLogUpdate] = {
    case transaction: TransactionLogUpdate.TransactionAccepted =>
      internalUpdateFormat.includeTransactions.flatMap { transactionFormat =>
        val transactionEvents = transaction.events.collect {
          case createdEvent: TransactionLogUpdate.CreatedEvent => createdEvent
          case exercisedEvent: TransactionLogUpdate.ExercisedEvent
              if exercisedEvent.consuming || transactionFormat.transactionShape == LedgerEffects =>
            exercisedEvent
        }
        val filteredEvents = transactionEvents
          .filter(transactionPredicate(transactionFormat))

        transactionFormat.transactionShape match {
          case AcsDelta =>
            val commandId = getCommandId(
              filteredEvents,
              transactionFormat.internalEventFormat.templatePartiesFilter.allFilterParties,
            )
            Option.when(filteredEvents.nonEmpty)(
              transaction.copy(
                commandId = commandId,
                events = filteredEvents,
              )(transaction.traceContext)
            )

          case LedgerEffects =>
            Option.when(filteredEvents.nonEmpty)(
              transaction.copy(
                events = filteredEvents
              )(transaction.traceContext)
            )
        }
      }

    case _: TransactionLogUpdate.TransactionRejected => None

    case u: TransactionLogUpdate.ReassignmentAccepted =>
      internalUpdateFormat.includeReassignments.flatMap { reassignmentFormat =>
        val filteredReassignments = u.reassignment.iterator.filter { r =>
          partiesMatchFilter(
            reassignmentFormat.templatePartiesFilter,
            u.reassignment.iterator
              .map(r => r.templateId.toFullIdentifier(r.packageName).toNameTypeConRef)
              .toSet,
          )(r.stakeholders)
        }
        NonEmpty
          .from(filteredReassignments.toSeq)
          .map(rs => u.copy(reassignment = Reassignment.Batch(rs))(u.traceContext))
      }

    case u: TransactionLogUpdate.TopologyTransactionEffective =>
      internalUpdateFormat.includeTopologyEvents
        .flatMap(_.participantAuthorizationFormat)
        .flatMap { participantAuthorizationFormat =>
          val filteredEvents =
            u.events.filter(topologyEventPredicate(participantAuthorizationFormat))
          Option.when(filteredEvents.nonEmpty)(
            u.copy(events = filteredEvents)(u.traceContext)
          )
        }
  }

  def toGetUpdatesResponse(
      internalUpdateFormat: InternalUpdateFormat,
      lfValueTranslation: LfValueTranslation,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): TransactionLogUpdate => Future[GetUpdatesResponse] = {
    case transactionAccepted: TransactionLogUpdate.TransactionAccepted =>
      val internalTransactionFormat = internalUpdateFormat.includeTransactions
        .getOrElse(
          throw new IllegalStateException(
            "Transaction cannot be converted as there is no transaction format specified in update format"
          )
        )
      toTransaction(
        transactionAccepted,
        internalTransactionFormat,
        lfValueTranslation,
        transactionAccepted.traceContext,
      )
        .map(transaction =>
          GetUpdatesResponse(GetUpdatesResponse.Update.Transaction(transaction))
            .withPrecomputedSerializedSize()
        )

    case reassignmentAccepted: TransactionLogUpdate.ReassignmentAccepted =>
      val reassignmentInternalEventFormat = internalUpdateFormat.includeReassignments
        .getOrElse(
          throw new IllegalStateException(
            "Reassignment cannot be converted as there is no reassignment specified in update format"
          )
        )
      toReassignment(
        reassignmentAccepted,
        reassignmentInternalEventFormat.templatePartiesFilter.allFilterParties,
        reassignmentInternalEventFormat.eventProjectionProperties,
        lfValueTranslation,
        reassignmentAccepted.traceContext,
      )
        .map(reassignment =>
          GetUpdatesResponse(GetUpdatesResponse.Update.Reassignment(reassignment))
            .withPrecomputedSerializedSize()
        )

    case topologyTransaction: TransactionLogUpdate.TopologyTransactionEffective =>
      toTopologyTransaction(topologyTransaction).map(transaction =>
        GetUpdatesResponse(GetUpdatesResponse.Update.TopologyTransaction(transaction))
          .withPrecomputedSerializedSize()
      )

    case illegal => throw new IllegalStateException(s"$illegal is not expected here")
  }

  def toGetUpdateResponse(
      transactionLogUpdate: TransactionLogUpdate,
      internalUpdateFormat: InternalUpdateFormat,
      lfValueTranslation: LfValueTranslation,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[Option[GetUpdateResponse]] =
    filter(internalUpdateFormat)(transactionLogUpdate)
      .collect {
        case transactionAccepted: TransactionLogUpdate.TransactionAccepted =>
          val internalTransactionFormat = internalUpdateFormat.includeTransactions
            .getOrElse(
              throw new IllegalStateException(
                "Transaction cannot be converted as there is no transaction format specified in update format"
              )
            )
          toTransaction(
            transactionAccepted,
            internalTransactionFormat,
            lfValueTranslation,
            transactionAccepted.traceContext,
          )
            .map(transaction =>
              GetUpdateResponse(GetUpdateResponse.Update.Transaction(transaction))
                .withPrecomputedSerializedSize()
            )

        case reassignmentAccepted: TransactionLogUpdate.ReassignmentAccepted =>
          val reassignmentInternalEventFormat = internalUpdateFormat.includeReassignments
            .getOrElse(
              throw new IllegalStateException(
                "Reassignment cannot be converted as there is no reassignment specified in update format"
              )
            )
          toReassignment(
            reassignmentAccepted,
            reassignmentInternalEventFormat.templatePartiesFilter.allFilterParties,
            reassignmentInternalEventFormat.eventProjectionProperties,
            lfValueTranslation,
            reassignmentAccepted.traceContext,
          )
            .map(reassignment =>
              GetUpdateResponse(GetUpdateResponse.Update.Reassignment(reassignment))
                .withPrecomputedSerializedSize()
            )

        case topologyTransaction: TransactionLogUpdate.TopologyTransactionEffective =>
          toTopologyTransaction(topologyTransaction).map(transaction =>
            GetUpdateResponse(GetUpdateResponse.Update.TopologyTransaction(transaction))
              .withPrecomputedSerializedSize()
          )
      }
      .map(_.map(Some(_)))
      .getOrElse(Future.successful(None))

  private def toTransaction(
      transactionAccepted: TransactionLogUpdate.TransactionAccepted,
      internalTransactionFormat: InternalTransactionFormat,
      lfValueTranslation: LfValueTranslation,
      traceContext: TraceContext,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[FlatTransaction] =
    Future.delegate {
      MonadUtil
        .sequentialTraverse(transactionAccepted.events)(event =>
          toEvent(
            event,
            internalTransactionFormat,
            lfValueTranslation,
          )
        )
        .map(events =>
          FlatTransaction(
            updateId = transactionAccepted.updateId,
            commandId = transactionAccepted.commandId,
            workflowId = transactionAccepted.workflowId,
            effectiveAt = Some(TimestampConversion.fromLf(transactionAccepted.effectiveAt)),
            events = events,
            offset = transactionAccepted.offset.unwrap,
            synchronizerId = transactionAccepted.synchronizerId,
            traceContext = SerializableTraceContext(traceContext).toDamlProtoOpt,
            recordTime = Some(TimestampConversion.fromLf(transactionAccepted.recordTime)),
            externalTransactionHash = transactionAccepted.externalTransactionHash.map(_.unwrap),
          )
        )
    }

  private def transactionPredicate(
      transactionFormat: InternalTransactionFormat
  )(event: TransactionLogUpdate.Event): Boolean =
    partiesMatchFilter(
      transactionFormat.internalEventFormat.templatePartiesFilter,
      Set(event.templateId.toFullIdentifier(event.packageName).toNameTypeConRef),
    )(event.witnesses(transactionFormat.transactionShape))

  private def topologyEventPredicate(
      participantAuthorizationFormat: ParticipantAuthorizationFormat
  )(event: TransactionLogUpdate.PartyToParticipantAuthorization): Boolean =
    matchPartyInSet(event.party)(participantAuthorizationFormat.parties)

  private def toEvent(
      event: TransactionLogUpdate.Event,
      internalTransactionFormat: InternalTransactionFormat,
      lfValueTranslation: LfValueTranslation,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[apiEvent.Event] = {
    val requestingParties =
      internalTransactionFormat.internalEventFormat.templatePartiesFilter.allFilterParties

    event match {
      case createdEvent: TransactionLogUpdate.CreatedEvent =>
        createdToApiCreatedEvent(
          requestingParties,
          internalTransactionFormat.internalEventFormat.eventProjectionProperties,
          lfValueTranslation,
          createdEvent,
          _.witnesses(internalTransactionFormat.transactionShape),
        ).map(apiCreatedEvent => apiEvent.Event(apiEvent.Event.Event.Created(apiCreatedEvent)))

      case exercisedEvent: TransactionLogUpdate.ExercisedEvent =>
        exercisedToEvent(
          requestingParties,
          exercisedEvent,
          internalTransactionFormat.transactionShape,
          internalTransactionFormat.internalEventFormat.eventProjectionProperties,
          lfValueTranslation,
        )
    }
  }

  private def partiesMatchFilter(
      filter: TemplatePartiesFilter,
      templateIds: Set[NameTypeConRef],
  )(parties: Set[Party]) = {
    val matchesByWildcard: Boolean =
      filter.templateWildcardParties match {
        case Some(include) => parties.exists(p => include(p))
        case None => parties.nonEmpty // the witnesses should not be empty
      }

    def matchesByTemplateId(templateId: NameTypeConRef): Boolean =
      filter.relation.get(templateId) match {
        case Some(Some(include)) => parties.exists(include)
        case Some(None) => parties.nonEmpty // party wildcard
        case None => false // templateId is not in the filter
      }

    matchesByWildcard || templateIds.exists(matchesByTemplateId)
  }

  private def exercisedToEvent(
      requestingParties: Option[Set[Party]],
      exercisedEvent: ExercisedEvent,
      transactionShape: TransactionShape,
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[apiEvent.Event] =
    transactionShape match {
      case AcsDelta if !exercisedEvent.consuming =>
        Future.failed(
          new IllegalStateException(
            "Non consuming exercise cannot be rendered for ACS delta shape"
          )
        )

      case AcsDelta =>
        val witnessParties = requestingParties match {
          case Some(parties) =>
            parties.iterator.filter(exercisedEvent.flatEventWitnesses).toSeq
          // party-wildcard
          case None => exercisedEvent.flatEventWitnesses.toSeq
        }
        Future.successful(
          apiEvent.Event(
            apiEvent.Event.Event.Archived(
              apiEvent.ArchivedEvent(
                offset = exercisedEvent.eventOffset.unwrap,
                nodeId = exercisedEvent.nodeId,
                contractId = exercisedEvent.contractId.coid,
                templateId = Some(LfEngineToApi.toApiIdentifier(exercisedEvent.templateId)),
                packageName = exercisedEvent.packageName,
                witnessParties = witnessParties,
                implementedInterfaces = lfValueTranslation.implementedInterfaces(
                  eventProjectionProperties,
                  witnessParties.toSet,
                  exercisedEvent.templateId.toFullIdentifier(
                    exercisedEvent.packageName
                  ),
                ),
              )
            )
          )
        )

      case LedgerEffects =>
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
          eventProjectionProperties.verbose,
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
                verbose = eventProjectionProperties.verbose,
                attribute = "exercise result",
                enrich = choiceResultEnricher,
              )
              .map(Some(_))
          }
          .getOrElse(Future.successful(None))

        for {
          choiceArgument <- eventualChoiceArgument
          maybeExerciseResult <- eventualExerciseResult
          witnessParties = requestingParties
            .fold(exercisedEvent.treeEventWitnesses)(
              _.filter(exercisedEvent.treeEventWitnesses)
            )
            .toSeq
          flatEventWitnesses = requestingParties
            .fold(exercisedEvent.flatEventWitnesses)(
              _.filter(exercisedEvent.flatEventWitnesses)
            )
            .toSeq
        } yield apiEvent.Event(
          apiEvent.Event.Event.Exercised(
            apiEvent.ExercisedEvent(
              offset = exercisedEvent.eventOffset.unwrap,
              nodeId = exercisedEvent.nodeId,
              contractId = exercisedEvent.contractId.coid,
              templateId = Some(LfEngineToApi.toApiIdentifier(exercisedEvent.templateId)),
              packageName = exercisedEvent.packageName,
              interfaceId = exercisedEvent.interfaceId.map(LfEngineToApi.toApiIdentifier),
              choice = exercisedEvent.choice,
              choiceArgument = Some(choiceArgument),
              actingParties = exercisedEvent.actingParties.toSeq,
              consuming = exercisedEvent.consuming,
              witnessParties = witnessParties,
              lastDescendantNodeId = exercisedEvent.lastDescendantNodeId,
              exerciseResult = maybeExerciseResult,
              implementedInterfaces =
                if (exercisedEvent.consuming)
                  lfValueTranslation.implementedInterfaces(
                    eventProjectionProperties,
                    witnessParties.toSet,
                    exercisedEvent.templateId.toFullIdentifier(
                      exercisedEvent.packageName
                    ),
                  )
                else Nil,
              acsDelta = flatEventWitnesses.nonEmpty,
            )
          )
        )
    }

  private def createdToApiCreatedEvent(
      requestingPartiesO: Option[Set[Party]],
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
      createdEvent: CreatedEvent,
      createdWitnesses: CreatedEvent => Set[Party],
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[apiEvent.CreatedEvent] = {
    val createNode = Node.Create(
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
    )
    createdToApiCreatedEvent(
      requestingPartiesO = requestingPartiesO,
      eventProjectionProperties = eventProjectionProperties,
      lfValueTranslation = lfValueTranslation,
      create = createNode,
      ledgerEffectiveTime = createdEvent.ledgerEffectiveTime,
      offset = createdEvent.eventOffset,
      nodeId = createdEvent.nodeId,
      authenticationData = createdEvent.authenticationData,
      representativePackageId = createdEvent.representativePackageId,
      createdEventWitnesses = createdWitnesses(createdEvent),
      flatEventWitnesses = createdEvent.flatEventWitnesses,
    )
  }

  private def createdToApiCreatedEvent(
      requestingPartiesO: Option[Set[Party]],
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
      create: Node.Create,
      ledgerEffectiveTime: Timestamp,
      offset: Offset,
      nodeId: Int,
      authenticationData: Bytes,
      representativePackageId: LfPackageId,
      createdEventWitnesses: Set[Party],
      flatEventWitnesses: Set[Party],
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[apiEvent.CreatedEvent] = {

    val fatContractInstance: FatContractInstance =
      FatContractInstance.fromCreateNode(
        create,
        CreationTime.CreatedAt(ledgerEffectiveTime),
        authenticationData,
      )

    val witnesses = requestingPartiesO
      .fold(createdEventWitnesses)(_.view.filter(createdEventWitnesses).toSet)
      .map(_.toString)

    val acsDelta =
      requestingPartiesO.fold(flatEventWitnesses.view)(_.view.filter(flatEventWitnesses)).nonEmpty

    lfValueTranslation.toApiCreatedEvent(
      eventProjectionProperties = eventProjectionProperties,
      fatContractInstance = fatContractInstance,
      offset = offset.unwrap,
      nodeId = nodeId,
      representativePackageId = representativePackageId,
      witnesses = witnesses,
      acsDelta = acsDelta,
    )
  }

  private def matchPartyInSet(party: Party)(optSet: Option[Set[Party]]) =
    optSet match {
      case Some(filterParties) => filterParties.contains(party)
      case None => true
    }

  private def getCommandId(
      flatTransactionEvents: Vector[TransactionLogUpdate.Event],
      requestingPartiesO: Option[Set[Party]],
  ) =
    flatTransactionEvents
      .collectFirst {
        case event if requestingPartiesO.fold(true)(_.exists(event.submitters)) =>
          event.commandId
      }
      .getOrElse("")

  private def toReassignment(
      reassignmentAccepted: TransactionLogUpdate.ReassignmentAccepted,
      requestingParties: Option[Set[Party]],
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
      traceContext: TraceContext,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): Future[ApiReassignment] = {
    val stringRequestingParties = requestingParties.map(_.map(_.toString))
    val info = reassignmentAccepted.reassignmentInfo

    (MonadUtil
      .sequentialTraverse(reassignmentAccepted.reassignment.toSeq) {
        case assigned: Reassignment.Assign =>
          createdToApiCreatedEvent(
            requestingPartiesO = requestingParties,
            eventProjectionProperties = eventProjectionProperties,
            lfValueTranslation = lfValueTranslation,
            create = assigned.createNode,
            ledgerEffectiveTime = assigned.ledgerEffectiveTime,
            offset = reassignmentAccepted.offset,
            nodeId = assigned.nodeId,
            authenticationData = assigned.contractAuthenticationData,
            // TODO(#28301): Use the assignment representative package ID when available
            representativePackageId = assigned.createNode.templateId.packageId,
            createdEventWitnesses = assigned.createNode.stakeholders,
            flatEventWitnesses = assigned.createNode.stakeholders,
          ).map(createdEvent =>
            ApiReassignmentEvent(
              ApiAssigned(
                ApiAssignedEvent(
                  source = info.sourceSynchronizer.unwrap.toProtoPrimitive,
                  target = info.targetSynchronizer.unwrap.toProtoPrimitive,
                  reassignmentId = info.reassignmentId.toProtoPrimitive,
                  submitter = info.submitter.getOrElse(""),
                  reassignmentCounter = assigned.reassignmentCounter,
                  createdEvent = Some(createdEvent),
                )
              )
            )
          )

        case unassigned: Reassignment.Unassign =>
          val stakeholders = unassigned.stakeholders
          Future.successful(
            ApiReassignmentEvent(
              ApiUnassigned(
                ApiUnassignedEvent(
                  offset = reassignmentAccepted.offset.unwrap,
                  source = info.sourceSynchronizer.unwrap.toProtoPrimitive,
                  target = info.targetSynchronizer.unwrap.toProtoPrimitive,
                  reassignmentId = info.reassignmentId.toProtoPrimitive,
                  submitter = info.submitter.getOrElse(""),
                  reassignmentCounter = unassigned.reassignmentCounter,
                  contractId = unassigned.contractId.coid,
                  templateId = Some(LfEngineToApi.toApiIdentifier(unassigned.templateId)),
                  packageName = unassigned.packageName,
                  assignmentExclusivity =
                    unassigned.assignmentExclusivity.map(TimestampConversion.fromLf),
                  witnessParties = requestingParties.fold(stakeholders)(stakeholders.filter).toSeq,
                  nodeId = unassigned.nodeId,
                )
              )
            )
          )
      })
      .map(events =>
        ApiReassignment(
          updateId = reassignmentAccepted.updateId,
          commandId = reassignmentAccepted.completionStreamResponse
            .flatMap(_.completionResponse.completion)
            .filter(completion => stringRequestingParties.fold(true)(completion.actAs.exists))
            .map(_.commandId)
            .getOrElse(""),
          workflowId = reassignmentAccepted.workflowId,
          offset = reassignmentAccepted.offset.unwrap,
          events = events,
          traceContext = SerializableTraceContext(traceContext).toDamlProtoOpt,
          recordTime = Some(TimestampConversion.fromLf(reassignmentAccepted.recordTime)),
          synchronizerId = reassignmentAccepted.synchronizerId,
        )
      )
  }

  private def toTopologyTransaction(
      topologyTransaction: TransactionLogUpdate.TopologyTransactionEffective
  ): Future[TopologyTransaction] = Future.successful {
    TopologyTransaction(
      updateId = topologyTransaction.updateId,
      offset = topologyTransaction.offset.unwrap,
      synchronizerId = topologyTransaction.synchronizerId,
      recordTime = Some(TimestampConversion.fromLf(topologyTransaction.effectiveTime)),
      events = topologyTransaction.events.map(event =>
        toTopologyEvent(
          partyId = event.party,
          participantId = event.participant,
          authorizationEvent = event.authorizationEvent,
        )
      ),
      traceContext = SerializableTraceContext(topologyTransaction.traceContext).toDamlProtoOpt,
    )
  }
}
