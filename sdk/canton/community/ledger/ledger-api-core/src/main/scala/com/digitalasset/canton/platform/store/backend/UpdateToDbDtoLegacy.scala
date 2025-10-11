// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.MetricsContext.{withExtraMetricLabels, withOptionalMetricLabels}
import com.daml.platform.v1.index.StatusDetails
import com.digitalasset.canton.data.DeduplicationPeriod.{DeduplicationDuration, DeduplicationOffset}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationEvent,
  TopologyEvent,
}
import com.digitalasset.canton.ledger.participant.state.Update.TransactionAccepted.RepresentativePackageIds
import com.digitalasset.canton.ledger.participant.state.{CompletionInfo, Reassignment, Update}
import com.digitalasset.canton.metrics.{IndexerMetrics, LedgerApiServerMetrics}
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.indexer.TransactionTraversalUtils
import com.digitalasset.canton.platform.indexer.TransactionTraversalUtils.NodeInfo
import com.digitalasset.canton.platform.store.backend.Conversions.{
  authorizationEventInt,
  participantPermissionInt,
}
import com.digitalasset.canton.platform.store.dao.JdbcLedgerDao
import com.digitalasset.canton.platform.store.dao.events.*
import com.digitalasset.canton.protocol.UpdateId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.SerializableTraceContext
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.SerializableTraceContextExtension
import com.digitalasset.daml.lf.data.Ref.PackageRef
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.transaction.Node.Action
import io.grpc.Status

import java.util.UUID

object UpdateToDbDtoLegacy {
  import Update.*

  def apply(
      participantId: Ref.ParticipantId,
      translation: LfValueSerialization,
      compressionStrategy: CompressionStrategy,
      metrics: LedgerApiServerMetrics,
  )(implicit mc: MetricsContext): Offset => Update => Iterator[DbDto] = { offset => tracedUpdate =>
    val serializedTraceContext =
      SerializableTraceContext(tracedUpdate.traceContext).toDamlProto.toByteArray
    tracedUpdate match {
      case u: CommandRejected =>
        commandRejectedToDbDto(
          metrics = metrics,
          offset = offset,
          serializedTraceContext = serializedTraceContext,
          commandRejected = u,
        )

      case u: PartyAddedToParticipant =>
        partyAddedToParticipantToDbDto(
          metrics = metrics,
          participantId = participantId,
          offset = offset,
          partyAddedToParticipant = u,
        )

      case u: TopologyTransactionEffective =>
        topologyTransactionToDbDto(
          metrics = metrics,
          participantId = participantId,
          offset = offset,
          serializedTraceContext = serializedTraceContext,
          topologyTransaction = u,
        )

      case u: TransactionAccepted =>
        transactionAcceptedToDbDto(
          translation = translation,
          compressionStrategy = compressionStrategy,
          metrics = metrics,
          offset = offset,
          serializedTraceContext = serializedTraceContext,
          transactionAccepted = u,
        )

      case u: ReassignmentAccepted =>
        reassignmentAcceptedToDbDto(
          translation = translation,
          compressionStrategy = compressionStrategy,
          metrics = metrics,
          offset = offset,
          serializedTraceContext = serializedTraceContext,
          reassignmentAccepted = u,
        )

      case u: SequencerIndexMoved =>
        // nothing to persist, this is only a synthetic DbDto to facilitate updating the StringInterning
        Iterator(DbDto.SequencerIndexMoved(u.synchronizerId))

      case _: EmptyAcsPublicationRequired => Iterator.empty
      case _: LogicalSynchronizerUpgradeTimeReached => Iterator.empty

      case _: CommitRepair =>
        Iterator.empty
    }
  }

  private def commandRejectedToDbDto(
      metrics: LedgerApiServerMetrics,
      offset: Offset,
      serializedTraceContext: Array[Byte],
      commandRejected: CommandRejected,
  )(implicit mc: MetricsContext): Iterator[DbDto] = {
    withExtraMetricLabels(
      IndexerMetrics.Labels.grpcCode -> Status
        .fromCodeValue(commandRejected.reasonTemplate.code)
        .getCode
        .name(),
      IndexerMetrics.Labels.userId -> commandRejected.completionInfo.userId,
    ) { implicit mc: MetricsContext =>
      incrementCounterForEvent(
        metrics.indexer,
        IndexerMetrics.Labels.eventType.transaction,
        IndexerMetrics.Labels.status.rejected,
      )
    }
    val messageUuid = commandRejected match {
      case _: SequencedCommandRejected => None
      case unSequenced: UnSequencedCommandRejected => Some(unSequenced.messageUuid)
    }
    Iterator(
      commandCompletion(
        offset = offset,
        recordTime = commandRejected.recordTime.toLf,
        updateId = None,
        completionInfo = commandRejected.completionInfo,
        synchronizerId = commandRejected.synchronizerId,
        messageUuid = messageUuid,
        serializedTraceContext = serializedTraceContext,
        isTransaction =
          true, // please note from usage point of view (deduplication) rejections are always used both for transactions and reassignments at the moment.
      ).copy(
        rejection_status_code = Some(commandRejected.reasonTemplate.code),
        rejection_status_message = Some(commandRejected.reasonTemplate.message),
        rejection_status_details =
          Some(StatusDetails.of(commandRejected.reasonTemplate.status.details).toByteArray),
      )
    )
  }

  private def partyAddedToParticipantToDbDto(
      metrics: LedgerApiServerMetrics,
      participantId: Ref.ParticipantId,
      offset: Offset,
      partyAddedToParticipant: PartyAddedToParticipant,
  )(implicit mc: MetricsContext): Iterator[DbDto] = {
    incrementCounterForEvent(
      metrics.indexer,
      IndexerMetrics.Labels.eventType.partyAllocation,
      IndexerMetrics.Labels.status.accepted,
    )
    Iterator(
      DbDto.PartyEntry(
        ledger_offset = offset.unwrap,
        recorded_at = partyAddedToParticipant.recordTime.toMicros,
        submission_id = partyAddedToParticipant.submissionId,
        party = Some(partyAddedToParticipant.party),
        typ = JdbcLedgerDao.acceptType,
        rejection_reason = None,
        is_local = Some(partyAddedToParticipant.participantId == participantId),
      )
    )
  }

  private def topologyTransactionToDbDto(
      metrics: LedgerApiServerMetrics,
      participantId: Ref.ParticipantId,
      offset: Offset,
      serializedTraceContext: Array[Byte],
      topologyTransaction: TopologyTransactionEffective,
  )(implicit mc: MetricsContext): Iterator[DbDto] = {
    incrementCounterForEvent(
      metrics.indexer,
      IndexerMetrics.Labels.eventType.topologyTransaction,
      IndexerMetrics.Labels.status.accepted,
    )

    val transactionMeta = DbDto.TransactionMeta(
      update_id = topologyTransaction.updateId.toProtoPrimitive.toByteArray,
      event_offset = offset.unwrap,
      publication_time = 0, // this is filled later
      record_time = topologyTransaction.recordTime.toMicros,
      synchronizer_id = topologyTransaction.synchronizerId,
      event_sequential_id_first = 0, // this is filled later
      event_sequential_id_last = 0, // this is filled later
    )

    val events = topologyTransaction.events.iterator.flatMap {
      case TopologyEvent.PartyToParticipantAuthorization(party, participant, authorizationEvent) =>
        import com.digitalasset.canton.platform.apiserver.services.admin.PartyAllocation
        val eventPartyToParticipant = Iterator(
          DbDto.EventPartyToParticipant(
            event_sequential_id = 0, // this is filled later
            event_offset = offset.unwrap,
            update_id = topologyTransaction.updateId.toProtoPrimitive.toByteArray,
            party_id = party,
            participant_id = participant,
            participant_permission = participantPermissionInt(authorizationEvent),
            participant_authorization_event = authorizationEventInt(authorizationEvent),
            synchronizer_id = topologyTransaction.synchronizerId,
            record_time = topologyTransaction.recordTime.toMicros,
            trace_context = serializedTraceContext,
          )
        )
        val partyEntry = Seq(authorizationEvent)
          .collect { case active: AuthorizationEvent.ActiveAuthorization => active }
          .map(_ =>
            DbDto.PartyEntry(
              ledger_offset = offset.unwrap,
              recorded_at = topologyTransaction.recordTime.toMicros,
              submission_id = Some(
                PartyAllocation.TrackerKey(party, participant, authorizationEvent).submissionId
              ),
              party = Some(party),
              typ = JdbcLedgerDao.acceptType,
              rejection_reason = None,
              is_local = Some(participant == participantId),
            )
          )
          .iterator
        eventPartyToParticipant ++ partyEntry
    }

    // TransactionMeta DTO must come last in this sequence
    // because in a later stage the preceding events
    // will be assigned consecutive event sequential ids
    // and transaction meta is assigned sequential ids of its first and last event
    events ++ Seq(transactionMeta)
  }

  private def transactionAcceptedToDbDto(
      translation: LfValueSerialization,
      compressionStrategy: CompressionStrategy,
      metrics: LedgerApiServerMetrics,
      offset: Offset,
      serializedTraceContext: Array[Byte],
      transactionAccepted: TransactionAccepted,
  )(implicit mc: MetricsContext): Iterator[DbDto] = {
    withOptionalMetricLabels(
      IndexerMetrics.Labels.userId -> transactionAccepted.completionInfoO.map(
        _.userId
      )
    ) { implicit mc: MetricsContext =>
      incrementCounterForEvent(
        metrics.indexer,
        IndexerMetrics.Labels.eventType.transaction,
        IndexerMetrics.Labels.status.accepted,
      )
    }

    val transactionMeta = DbDto.TransactionMeta(
      update_id = transactionAccepted.updateId.toProtoPrimitive.toByteArray,
      event_offset = offset.unwrap,
      publication_time = 0, // this is filled later
      record_time = transactionAccepted.recordTime.toMicros,
      synchronizer_id = transactionAccepted.synchronizerId,
      event_sequential_id_first = 0, // this is filled later
      event_sequential_id_last = 0, // this is filled later
    )

    val events: Iterator[DbDto] = TransactionTraversalUtils
      .executionOrderTraversalForIngestion(
        transactionAccepted.transaction.transaction
      )
      .iterator
      .flatMap {
        case NodeInfo(nodeId, create: Create, _) =>
          createNodeToDbDto(
            compressionStrategy = compressionStrategy,
            translation = translation,
            offset = offset,
            serializedTraceContext = serializedTraceContext,
            transactionAccepted = transactionAccepted,
            nodeId = nodeId,
            create = create,
          )

        case NodeInfo(nodeId, exercise: Exercise, lastDescendantNodeId) =>
          exerciseNodeToDbDto(
            compressionStrategy = compressionStrategy,
            translation = translation,
            offset = offset,
            serializedTraceContext = serializedTraceContext,
            transactionAccepted = transactionAccepted,
            nodeId = nodeId,
            exercise = exercise,
            lastDescendantNodeId = lastDescendantNodeId,
          )

        case _ =>
          Iterator.empty // It is okay to collect: blinding info is already there, we are free at hand to filter out the fetch and lookup nodes here already
      }

    val completions =
      for {
        completionInfo <- transactionAccepted.completionInfoO
      } yield commandCompletion(
        offset = offset,
        recordTime = transactionAccepted.recordTime.toLf,
        updateId = Some(transactionAccepted.updateId),
        completionInfo = completionInfo,
        synchronizerId = transactionAccepted.synchronizerId,
        messageUuid = None,
        serializedTraceContext = serializedTraceContext,
        isTransaction = true,
      )

    // TransactionMeta DTO must come last in this sequence
    // because in a later stage the preceding events
    // will be assigned consecutive event sequential ids
    // and transaction meta is assigned sequential ids of its first and last event
    events ++ completions ++ Seq(transactionMeta)
  }

  def templateIdWithPackageName(node: Action): String =
    node.templateId.copy(pkg = PackageRef.Name(node.packageName)).toString

  def templateIdWithPackageName(reassignment: Reassignment): String =
    reassignment.templateId.copy(pkg = PackageRef.Name(reassignment.packageName)).toString

  private def createNodeToDbDto(
      compressionStrategy: CompressionStrategy,
      translation: LfValueSerialization,
      offset: Offset,
      serializedTraceContext: Array[Byte],
      transactionAccepted: TransactionAccepted,
      nodeId: NodeId,
      create: Create,
  ): Iterator[DbDto] = {
    val templateId = templateIdWithPackageName(create)
    val flatWitnesses: Set[String] =
      if (transactionAccepted.isAcsDelta(create.coid))
        create.stakeholders.map(_.toString)
      else
        Set.empty
    val (createArgument, createKeyValue) = translation.serialize(create)
    val treeWitnesses =
      transactionAccepted.blindingInfo.disclosure.getOrElse(nodeId, Set.empty).map(_.toString)
    val treeWitnessesWithoutFlatWitnesses = treeWitnesses.diff(flatWitnesses)
    val representativePackageId = transactionAccepted.representativePackageIds match {
      case RepresentativePackageIds.SameAsContractPackageId =>
        create.templateId.packageId
      case RepresentativePackageIds.DedicatedRepresentativePackageIds(representativePackageIds) =>
        representativePackageIds.getOrElse(
          create.coid,
          throw new IllegalStateException(
            s"Missing representative package id for contract $create.coid"
          ),
        )
    }
    Iterator(
      DbDto.EventCreate(
        event_offset = offset.unwrap,
        update_id = transactionAccepted.updateId.toProtoPrimitive.toByteArray,
        ledger_effective_time = transactionAccepted.transactionMeta.ledgerEffectiveTime.micros,
        command_id = transactionAccepted.completionInfoO.map(_.commandId),
        workflow_id = transactionAccepted.transactionMeta.workflowId,
        user_id = transactionAccepted.completionInfoO.map(_.userId),
        submitters = transactionAccepted.completionInfoO.map(_.actAs.toSet),
        node_id = nodeId.index,
        contract_id = create.coid,
        template_id = templateId,
        package_id = create.templateId.packageId.toString,
        representative_package_id = representativePackageId.toString,
        flat_event_witnesses = flatWitnesses,
        tree_event_witnesses = treeWitnesses,
        create_argument =
          compressionStrategy.createArgumentCompressionLegacy.compress(createArgument),
        create_signatories = create.signatories.map(_.toString),
        create_observers = create.stakeholders.diff(create.signatories).map(_.toString),
        create_key_value = createKeyValue
          .map(compressionStrategy.createKeyValueCompressionLegacy.compress),
        create_key_maintainers = create.keyOpt.map(_.maintainers.map(_.toString)),
        create_key_hash = create.keyOpt.map(_.globalKey.hash.bytes.toHexString),
        create_argument_compression = compressionStrategy.createArgumentCompressionLegacy.id,
        create_key_value_compression =
          compressionStrategy.createKeyValueCompressionLegacy.id.filter(_ =>
            createKeyValue.isDefined
          ),
        event_sequential_id = 0, // this is filled later
        authentication_data = transactionAccepted.contractAuthenticationData
          .get(create.coid)
          .map(_.toByteArray)
          .getOrElse(
            throw new IllegalStateException(
              s"missing authentication data for contract ${create.coid}"
            )
          ),
        synchronizer_id = transactionAccepted.synchronizerId,
        trace_context = serializedTraceContext,
        record_time = transactionAccepted.recordTime.toMicros,
        external_transaction_hash =
          transactionAccepted.externalTransactionHash.map(_.unwrap.toByteArray),
        internal_contract_id = transactionAccepted.internalContractIds.getOrElse(
          create.coid,
          throw new IllegalStateException(
            s"missing internal contract id for contract ${create.coid}"
          ),
        ),
      )
    ) ++ withFirstMarked(
      flatWitnesses,
      (party, first) =>
        DbDto.IdFilterCreateStakeholder(
          event_sequential_id = 0, // this is filled later
          template_id = templateId,
          party_id = party,
          first_per_sequential_id = first,
        ),
    ) ++ withFirstMarked(
      treeWitnessesWithoutFlatWitnesses,
      (party, first) =>
        DbDto.IdFilterCreateNonStakeholderInformee(
          event_sequential_id = 0, // this is filled later
          template_id = templateId,
          party_id = party,
          first_per_sequential_id = first,
        ),
    )
  }

  private def exerciseNodeToDbDto(
      compressionStrategy: CompressionStrategy,
      translation: LfValueSerialization,
      offset: Offset,
      serializedTraceContext: Array[Byte],
      transactionAccepted: TransactionAccepted,
      nodeId: NodeId,
      exercise: Exercise,
      lastDescendantNodeId: NodeId,
  ): Iterator[DbDto] = {
    val (exerciseArgument, exerciseResult, createKeyValue) =
      translation.serialize(exercise)
    val stakeholders = exercise.stakeholders.map(_.toString)
    val treeWitnesses =
      transactionAccepted.blindingInfo.disclosure.getOrElse(nodeId, Set.empty).map(_.toString)
    val flatWitnesses =
      if (exercise.consuming && transactionAccepted.isAcsDelta(exercise.targetCoid))
        stakeholders
      else
        Set.empty[String]
    val treeWitnessesWithoutFlatWitnesses = treeWitnesses.diff(flatWitnesses)
    val templateId = templateIdWithPackageName(exercise)
    Iterator(
      DbDto.EventExercise(
        consuming = exercise.consuming,
        event_offset = offset.unwrap,
        update_id = transactionAccepted.updateId.toProtoPrimitive.toByteArray,
        ledger_effective_time = transactionAccepted.transactionMeta.ledgerEffectiveTime.micros,
        command_id = transactionAccepted.completionInfoO.map(_.commandId),
        workflow_id = transactionAccepted.transactionMeta.workflowId,
        user_id = transactionAccepted.completionInfoO.map(_.userId),
        submitters = transactionAccepted.completionInfoO.map(_.actAs.toSet),
        node_id = nodeId.index,
        contract_id = exercise.targetCoid,
        template_id = templateId,
        package_id = exercise.templateId.packageId.toString,
        flat_event_witnesses = flatWitnesses,
        tree_event_witnesses = treeWitnesses,
        exercise_choice = exercise.qualifiedChoiceName.choiceName,
        exercise_choice_interface_id = exercise.qualifiedChoiceName.interfaceId.map(_.toString),
        exercise_argument =
          compressionStrategy.consumingExerciseArgumentCompression.compress(exerciseArgument),
        exercise_result = exerciseResult
          .map(compressionStrategy.consumingExerciseResultCompression.compress),
        exercise_actors = exercise.actingParties.map(_.toString),
        exercise_last_descendant_node_id = lastDescendantNodeId.index,
        exercise_argument_compression = compressionStrategy.consumingExerciseArgumentCompression.id,
        exercise_result_compression = compressionStrategy.consumingExerciseResultCompression.id,
        event_sequential_id = 0, // this is filled later
        synchronizer_id = transactionAccepted.synchronizerId,
        trace_context = serializedTraceContext,
        record_time = transactionAccepted.recordTime.toMicros,
        external_transaction_hash =
          transactionAccepted.externalTransactionHash.map(_.unwrap.toByteArray),
        deactivated_event_sequential_id = None, // this is filled later
      )
    ) ++ {
      if (exercise.consuming) {
        withFirstMarked(
          flatWitnesses,
          (party, first) =>
            DbDto.IdFilterConsumingStakeholder(
              event_sequential_id = 0, // this is filled later
              template_id = templateId,
              party_id = party,
              first_per_sequential_id = first,
            ),
        ) ++ withFirstMarked(
          treeWitnessesWithoutFlatWitnesses,
          (party, first) =>
            DbDto.IdFilterConsumingNonStakeholderInformee(
              event_sequential_id = 0, // this is filled later
              template_id = templateId,
              party_id = party,
              first_per_sequential_id = first,
            ),
        )
      } else {
        withFirstMarked(
          treeWitnesses,
          (informee, first) =>
            DbDto.IdFilterNonConsumingInformee(
              event_sequential_id = 0, // this is filled later
              template_id = templateId,
              party_id = informee,
              first_per_sequential_id = first,
            ),
        )
      }
    }
  }

  private def reassignmentAcceptedToDbDto(
      translation: LfValueSerialization,
      compressionStrategy: CompressionStrategy,
      metrics: LedgerApiServerMetrics,
      offset: Offset,
      serializedTraceContext: Array[Byte],
      reassignmentAccepted: ReassignmentAccepted,
  )(implicit mc: MetricsContext): Iterator[DbDto] = {
    withOptionalMetricLabels(
      IndexerMetrics.Labels.userId -> reassignmentAccepted.optCompletionInfo.map(
        _.userId
      )
    ) { implicit mc: MetricsContext =>
      incrementCounterForEvent(
        metrics.indexer,
        IndexerMetrics.Labels.eventType.reassignment,
        IndexerMetrics.Labels.status.accepted,
      )
    }

    val events: Iterator[DbDto] = reassignmentAccepted.reassignment.iterator.flatMap {
      case unassign: Reassignment.Unassign =>
        unassignToDbDto(
          offset = offset,
          serializedTraceContext = serializedTraceContext,
          reassignmentAccepted = reassignmentAccepted,
          unassign = unassign,
        )

      case assign: Reassignment.Assign =>
        assignToDbDto(
          translation = translation,
          compressionStrategy = compressionStrategy,
          offset = offset,
          serializedTraceContext = serializedTraceContext,
          reassignmentAccepted = reassignmentAccepted,
          assign = assign,
        )
    }

    val completions: Option[DbDto] =
      for {
        completionInfo <- reassignmentAccepted.optCompletionInfo
      } yield commandCompletion(
        offset = offset,
        recordTime = reassignmentAccepted.recordTime.toLf,
        updateId = Some(reassignmentAccepted.updateId),
        completionInfo = completionInfo,
        synchronizerId = reassignmentAccepted.synchronizerId,
        messageUuid = None,
        serializedTraceContext = serializedTraceContext,
        isTransaction = false,
      )

    val transactionMeta = DbDto.TransactionMeta(
      update_id = reassignmentAccepted.updateId.toProtoPrimitive.toByteArray,
      event_offset = offset.unwrap,
      publication_time = 0, // this is filled later
      record_time = reassignmentAccepted.recordTime.toMicros,
      synchronizer_id = reassignmentAccepted.synchronizerId,
      event_sequential_id_first = 0, // this is filled later
      event_sequential_id_last = 0, // this is filled later
    )

    // TransactionMeta DTO must come last in this sequence
    // because in a later stage the preceding events
    // will be assigned consecutive event sequential ids
    // and transaction meta is assigned sequential ids of its first and last event
    events ++ completions.iterator ++ Iterator(transactionMeta)
  }

  private def unassignToDbDto(
      offset: Offset,
      serializedTraceContext: Array[Byte],
      reassignmentAccepted: ReassignmentAccepted,
      unassign: Reassignment.Unassign,
  ): Iterator[DbDto] = {
    val flatEventWitnesses = unassign.stakeholders.map(_.toString)
    Iterator(
      DbDto.EventUnassign(
        event_offset = offset.unwrap,
        update_id = reassignmentAccepted.updateId.toProtoPrimitive.toByteArray,
        command_id = reassignmentAccepted.optCompletionInfo.map(_.commandId),
        workflow_id = reassignmentAccepted.workflowId,
        submitter = reassignmentAccepted.reassignmentInfo.submitter,
        node_id = unassign.nodeId,
        contract_id = unassign.contractId,
        template_id = templateIdWithPackageName(unassign),
        package_id = unassign.templateId.packageId.toString,
        flat_event_witnesses = flatEventWitnesses.toSet,
        event_sequential_id = 0L, // this is filled later
        source_synchronizer_id = reassignmentAccepted.reassignmentInfo.sourceSynchronizer.unwrap,
        target_synchronizer_id = reassignmentAccepted.reassignmentInfo.targetSynchronizer.unwrap,
        reassignment_id = reassignmentAccepted.reassignmentInfo.reassignmentId.toBytes.toByteArray,
        reassignment_counter = unassign.reassignmentCounter,
        assignment_exclusivity = unassign.assignmentExclusivity.map(_.micros),
        trace_context = serializedTraceContext,
        record_time = reassignmentAccepted.recordTime.toMicros,
        deactivated_event_sequential_id = None, // this is filled later
      )
    ) ++ withFirstMarked(
      flatEventWitnesses,
      (party, first) =>
        DbDto.IdFilterUnassignStakeholder(
          0L, // this is filled later
          templateIdWithPackageName(unassign),
          party,
          first_per_sequential_id = first,
        ),
    )
  }

  private def assignToDbDto(
      translation: LfValueSerialization,
      compressionStrategy: CompressionStrategy,
      offset: Offset,
      serializedTraceContext: Array[Byte],
      reassignmentAccepted: ReassignmentAccepted,
      assign: Reassignment.Assign,
  ): Iterator[DbDto] = {
    val (createArgument, createKeyValue) = translation.serialize(assign.createNode)
    val templateId = templateIdWithPackageName(assign)
    val flatEventWitnesses = assign.createNode.stakeholders.map(_.toString)
    Iterator(
      DbDto.EventAssign(
        event_offset = offset.unwrap,
        update_id = reassignmentAccepted.updateId.toProtoPrimitive.toByteArray,
        command_id = reassignmentAccepted.optCompletionInfo.map(_.commandId),
        workflow_id = reassignmentAccepted.workflowId,
        submitter = reassignmentAccepted.reassignmentInfo.submitter,
        node_id = assign.nodeId,
        contract_id = assign.createNode.coid,
        template_id = templateId,
        package_id = assign.createNode.templateId.packageId.toString,
        flat_event_witnesses = flatEventWitnesses,
        create_argument = createArgument,
        create_signatories = assign.createNode.signatories.map(_.toString),
        create_observers = assign.createNode.stakeholders
          .diff(assign.createNode.signatories)
          .map(_.toString),
        create_key_value = createKeyValue
          .map(compressionStrategy.createKeyValueCompressionLegacy.compress),
        create_key_maintainers = assign.createNode.keyOpt.map(_.maintainers.map(_.toString)),
        create_key_hash = assign.createNode.keyOpt.map(_.globalKey.hash.bytes.toHexString),
        create_argument_compression = compressionStrategy.createArgumentCompressionLegacy.id,
        create_key_value_compression =
          compressionStrategy.createKeyValueCompressionLegacy.id.filter(_ =>
            createKeyValue.isDefined
          ),
        event_sequential_id = 0L, // this is filled later
        ledger_effective_time = assign.ledgerEffectiveTime.micros,
        authentication_data = assign.contractAuthenticationData.toByteArray,
        source_synchronizer_id = reassignmentAccepted.reassignmentInfo.sourceSynchronizer.unwrap,
        target_synchronizer_id = reassignmentAccepted.reassignmentInfo.targetSynchronizer.unwrap,
        reassignment_id = reassignmentAccepted.reassignmentInfo.reassignmentId.toBytes.toByteArray,
        reassignment_counter = assign.reassignmentCounter,
        trace_context = serializedTraceContext,
        record_time = reassignmentAccepted.recordTime.toMicros,
        internal_contract_id = reassignmentAccepted.internalContractIds.getOrElse(
          assign.createNode.coid,
          throw new IllegalStateException(
            s"missing internal contract id for contract ${assign.createNode.coid}"
          ),
        ),
      )
    ) ++ withFirstMarked(
      flatEventWitnesses,
      (party, first) =>
        DbDto.IdFilterAssignStakeholder(
          0L, // this is filled later
          templateId,
          party,
          first_per_sequential_id = first,
        ),
    )
  }

  private def incrementCounterForEvent(
      metrics: IndexerMetrics,
      eventType: String,
      status: String,
  )(implicit
      mc: MetricsContext
  ): Unit =
    withExtraMetricLabels(
      IndexerMetrics.Labels.eventType.key -> eventType,
      IndexerMetrics.Labels.status.key -> status,
    ) { implicit mc =>
      metrics.eventsMeter.mark()
    }

  private def commandCompletion(
      offset: Offset,
      recordTime: Time.Timestamp,
      updateId: Option[UpdateId],
      completionInfo: CompletionInfo,
      synchronizerId: SynchronizerId,
      messageUuid: Option[UUID],
      isTransaction: Boolean,
      serializedTraceContext: Array[Byte],
  ): DbDto.CommandCompletion = {
    val (deduplicationOffset, deduplicationDurationSeconds, deduplicationDurationNanos) =
      completionInfo.optDeduplicationPeriod
        .map {
          case DeduplicationOffset(offset) =>
            (
              Some(offset.fold(0L)(_.unwrap)),
              None,
              None,
            )
          case DeduplicationDuration(duration) =>
            (None, Some(duration.getSeconds), Some(duration.getNano))
        }
        .getOrElse((None, None, None))

    DbDto.CommandCompletion(
      completion_offset = offset.unwrap,
      record_time = recordTime.micros,
      publication_time = 0L, // will be filled later
      user_id = completionInfo.userId,
      submitters = completionInfo.actAs.toSet,
      command_id = completionInfo.commandId,
      update_id = updateId.map(_.toProtoPrimitive.toByteArray),
      rejection_status_code = None,
      rejection_status_message = None,
      rejection_status_details = None,
      submission_id = completionInfo.submissionId,
      deduplication_offset = deduplicationOffset,
      deduplication_duration_seconds = deduplicationDurationSeconds,
      deduplication_duration_nanos = deduplicationDurationNanos,
      synchronizer_id = synchronizerId,
      message_uuid = messageUuid.map(_.toString),
      is_transaction = isTransaction,
      trace_context = serializedTraceContext,
    )
  }

  private def withFirstMarked(
      parties: Set[String],
      create: (String, Boolean) => DbDto,
  ): Seq[DbDto] =
    parties.iterator.zipWithIndex.map { case (party, idx) =>
      create(party, idx == 0)
    }.toSeq
}
