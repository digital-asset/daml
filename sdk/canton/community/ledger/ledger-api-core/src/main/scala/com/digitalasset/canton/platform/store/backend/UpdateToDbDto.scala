// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.MetricsContext.{withExtraMetricLabels, withOptionalMetricLabels}
import com.daml.platform.v1.index.StatusDetails
import com.digitalasset.canton.data.DeduplicationPeriod.{DeduplicationDuration, DeduplicationOffset}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.{
  CompletionInfo,
  Reassignment,
  RequestIndex,
  Update,
}
import com.digitalasset.canton.metrics.{IndexedUpdatesMetrics, LedgerApiServerMetrics}
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.indexer.TransactionTraversalUtils
import com.digitalasset.canton.platform.store.dao.JdbcLedgerDao
import com.digitalasset.canton.platform.store.dao.events.*
import com.digitalasset.canton.tracing.{SerializableTraceContext, Traced}
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.ledger.EventId
import io.grpc.Status

object UpdateToDbDto {

  def apply(
      participantId: Ref.ParticipantId,
      translation: LfValueSerialization,
      compressionStrategy: CompressionStrategy,
      metrics: LedgerApiServerMetrics,
  )(implicit mc: MetricsContext): Offset => Traced[Update] => Iterator[DbDto] = {
    offset => tracedUpdate =>
      import Update.*
      val serializedTraceContext =
        SerializableTraceContext(tracedUpdate.traceContext).toDamlProto.toByteArray
      tracedUpdate.value match {
        case u: CommandRejected =>
          withExtraMetricLabels(
            IndexedUpdatesMetrics.Labels.grpcCode -> Status
              .fromCodeValue(u.reasonTemplate.code)
              .getCode
              .name(),
            IndexedUpdatesMetrics.Labels.applicationId -> u.completionInfo.applicationId,
          ) { implicit mc: MetricsContext =>
            incrementCounterForEvent(
              metrics.indexerEvents,
              IndexedUpdatesMetrics.Labels.eventType.transaction,
              IndexedUpdatesMetrics.Labels.status.rejected,
            )
          }
          Iterator(
            commandCompletion(
              offset = offset,
              recordTime = u.recordTime,
              transactionId = None,
              completionInfo = u.completionInfo,
              domainId = u.domainId.toProtoPrimitive,
              requestIndex = u.domainIndex.flatMap(_.requestIndex),
              serializedTraceContext = serializedTraceContext,
              isTransaction =
                true, // please note from usage point of view (deduplication) rejections are always used both for transactions and reassignments at the moment.
            ).copy(
              rejection_status_code = Some(u.reasonTemplate.code),
              rejection_status_message = Some(u.reasonTemplate.message),
              rejection_status_details =
                Some(StatusDetails.of(u.reasonTemplate.status.details).toByteArray),
            )
          )

        case _: Init => Iterator()

        case u: PartyAddedToParticipant =>
          incrementCounterForEvent(
            metrics.indexerEvents,
            IndexedUpdatesMetrics.Labels.eventType.partyAllocation,
            IndexedUpdatesMetrics.Labels.status.accepted,
          )
          Iterator(
            DbDto.PartyEntry(
              ledger_offset = offset.toHexString,
              recorded_at = u.recordTime.micros,
              submission_id = u.submissionId,
              party = Some(u.party),
              display_name = Option(u.displayName),
              typ = JdbcLedgerDao.acceptType,
              rejection_reason = None,
              is_local = Some(u.participantId == participantId),
            )
          )

        case u: PartyAllocationRejected =>
          incrementCounterForEvent(
            metrics.indexerEvents,
            IndexedUpdatesMetrics.Labels.eventType.partyAllocation,
            IndexedUpdatesMetrics.Labels.status.rejected,
          )
          Iterator(
            DbDto.PartyEntry(
              ledger_offset = offset.toHexString,
              recorded_at = u.recordTime.micros,
              submission_id = Some(u.submissionId),
              party = None,
              display_name = None,
              typ = JdbcLedgerDao.rejectType,
              rejection_reason = Some(u.rejectionReason),
              is_local = None,
            )
          )

        case u: TransactionAccepted =>
          withOptionalMetricLabels(
            IndexedUpdatesMetrics.Labels.applicationId -> u.completionInfoO.map(_.applicationId)
          ) { implicit mc: MetricsContext =>
            incrementCounterForEvent(
              metrics.indexerEvents,
              IndexedUpdatesMetrics.Labels.eventType.transaction,
              IndexedUpdatesMetrics.Labels.status.accepted,
            )
          }
          val blinding = u.blindingInfo
          val preorderTraversal =
            TransactionTraversalUtils.preorderTraversalForIngestion(u.transaction.transaction)

          val domainId = u.domainId.toProtoPrimitive
          val transactionMeta = DbDto.TransactionMeta(
            transaction_id = u.transactionId,
            event_offset = offset.toHexString,
            publication_time = 0, // this is filled later
            record_time = u.recordTime.micros,
            domain_id = domainId,
            event_sequential_id_first = 0, // this is filled later
            event_sequential_id_last = 0, // this is filled later
          )
          val events: Iterator[DbDto] = preorderTraversal.iterator
            .flatMap {
              case (nodeId, create: Create) =>
                val eventId = EventId(u.transactionId, nodeId)
                val templateId = create.templateId.toString
                val stakeholders = create.stakeholders.map(_.toString)
                val (createArgument, createKeyValue) = translation.serialize(create)
                val informees = blinding.disclosure.getOrElse(nodeId, Set.empty).map(_.toString)
                val nonStakeholderInformees = informees.diff(stakeholders)
                Iterator(
                  DbDto.EventCreate(
                    event_offset = offset.toHexString,
                    transaction_id = u.transactionId,
                    ledger_effective_time = u.transactionMeta.ledgerEffectiveTime.micros,
                    command_id = u.completionInfoO.map(_.commandId),
                    workflow_id = u.transactionMeta.workflowId,
                    application_id = u.completionInfoO.map(_.applicationId),
                    submitters = u.completionInfoO.map(_.actAs.toSet),
                    node_index = nodeId.index,
                    event_id = eventId.toLedgerString,
                    contract_id = create.coid.coid,
                    template_id = templateId,
                    package_name = create.packageName,
                    package_version = create.packageVersion.map(_.toString()),
                    flat_event_witnesses = stakeholders,
                    tree_event_witnesses = informees,
                    create_argument =
                      compressionStrategy.createArgumentCompression.compress(createArgument),
                    create_signatories = create.signatories.map(_.toString),
                    create_observers = create.stakeholders.diff(create.signatories).map(_.toString),
                    create_key_value = createKeyValue
                      .map(compressionStrategy.createKeyValueCompression.compress),
                    create_key_maintainers = create.keyOpt.map(_.maintainers.map(_.toString)),
                    create_key_hash = create.keyOpt.map(_.globalKey.hash.bytes.toHexString),
                    create_argument_compression = compressionStrategy.createArgumentCompression.id,
                    create_key_value_compression =
                      compressionStrategy.createKeyValueCompression.id.filter(_ =>
                        createKeyValue.isDefined
                      ),
                    event_sequential_id = 0, // this is filled later
                    driver_metadata =
                      // Allow None as the original participant might be running
                      // with a version predating the introduction of contract driver metadata
                      u.contractMetadata.get(create.coid).map(_.toByteArray),
                    domain_id = domainId,
                    trace_context = serializedTraceContext,
                    record_time = u.recordTime.micros,
                  )
                ) ++ stakeholders.iterator.map(
                  DbDto.IdFilterCreateStakeholder(
                    event_sequential_id = 0, // this is filled later
                    template_id = templateId,
                    _,
                  )
                ) ++ nonStakeholderInformees.iterator.map(
                  DbDto.IdFilterCreateNonStakeholderInformee(
                    event_sequential_id = 0, // this is filled later
                    _,
                  )
                )

              case (nodeId, exercise: Exercise) =>
                val eventId = EventId(u.transactionId, nodeId)
                val (exerciseArgument, exerciseResult, createKeyValue) =
                  translation.serialize(eventId, exercise)
                val stakeholders = exercise.stakeholders.map(_.toString)
                val informees = blinding.disclosure.getOrElse(nodeId, Set.empty).map(_.toString)
                val flatWitnesses = if (exercise.consuming) stakeholders else Set.empty[String]
                val nonStakeholderInformees = informees.diff(stakeholders)
                val templateId = exercise.templateId.toString
                Iterator(
                  DbDto.EventExercise(
                    consuming = exercise.consuming,
                    event_offset = offset.toHexString,
                    transaction_id = u.transactionId,
                    ledger_effective_time = u.transactionMeta.ledgerEffectiveTime.micros,
                    command_id = u.completionInfoO.map(_.commandId),
                    workflow_id = u.transactionMeta.workflowId,
                    application_id = u.completionInfoO.map(_.applicationId),
                    submitters = u.completionInfoO.map(_.actAs.toSet),
                    node_index = nodeId.index,
                    event_id = EventId(u.transactionId, nodeId).toLedgerString,
                    contract_id = exercise.targetCoid.coid,
                    template_id = templateId,
                    package_name = exercise.packageName,
                    flat_event_witnesses = flatWitnesses,
                    tree_event_witnesses = informees,
                    create_key_value = createKeyValue
                      .map(compressionStrategy.createKeyValueCompression.compress),
                    exercise_choice = exercise.qualifiedChoiceName.toString,
                    exercise_argument =
                      compressionStrategy.exerciseArgumentCompression.compress(exerciseArgument),
                    exercise_result = exerciseResult
                      .map(compressionStrategy.exerciseResultCompression.compress),
                    exercise_actors = exercise.actingParties.map(_.toString),
                    exercise_child_event_ids = exercise.children.iterator
                      .map(EventId(u.transactionId, _).toLedgerString.toString)
                      .toVector,
                    create_key_value_compression = compressionStrategy.createKeyValueCompression.id,
                    exercise_argument_compression =
                      compressionStrategy.exerciseArgumentCompression.id,
                    exercise_result_compression = compressionStrategy.exerciseResultCompression.id,
                    event_sequential_id = 0, // this is filled later
                    domain_id = domainId,
                    trace_context = serializedTraceContext,
                    record_time = u.recordTime.micros,
                  )
                ) ++ {
                  if (exercise.consuming) {
                    stakeholders.iterator.map(stakeholder =>
                      DbDto.IdFilterConsumingStakeholder(
                        event_sequential_id = 0, // this is filled later
                        template_id = templateId,
                        party_id = stakeholder,
                      )
                    ) ++ nonStakeholderInformees.iterator.map(stakeholder =>
                      DbDto.IdFilterConsumingNonStakeholderInformee(
                        event_sequential_id = 0, // this is filled later
                        party_id = stakeholder,
                      )
                    )
                  } else {
                    informees.iterator.map(informee =>
                      DbDto.IdFilterNonConsumingInformee(
                        event_sequential_id = 0, // this is filled later
                        party_id = informee,
                      )
                    )
                  }
                }
              case _ =>
                Iterator.empty // It is okay to collect: blinding info is already there, we are free at hand to filter out the fetch and lookup nodes here already
            }

          val completions =
            u.completionInfoO.iterator.map(
              commandCompletion(
                offset = offset,
                recordTime = u.recordTime,
                transactionId = Some(u.transactionId),
                _,
                domainId = domainId,
                requestIndex = u.domainIndex.flatMap(_.requestIndex),
                serializedTraceContext = serializedTraceContext,
                isTransaction = true,
              )
            )

          // TransactionMeta DTO must come last in this sequence
          // because in a later stage the preceding events
          // will be assigned consecutive event sequential ids
          // and transaction meta is assigned sequential ids of its first and last event
          events ++ completions ++ Seq(transactionMeta)

        case u: ReassignmentAccepted =>
          withOptionalMetricLabels(
            IndexedUpdatesMetrics.Labels.applicationId -> u.optCompletionInfo.map(_.applicationId)
          ) { implicit mc: MetricsContext =>
            incrementCounterForEvent(
              metrics.indexerEvents,
              IndexedUpdatesMetrics.Labels.eventType.reassignment,
              IndexedUpdatesMetrics.Labels.status.accepted,
            )
          }
          val events = u.reassignment match {
            case unassign: Reassignment.Unassign =>
              val flatEventWitnesses = unassign.stakeholders.map(_.toString)
              val templateId = unassign.templateId.toString
              Iterator(
                DbDto.EventUnassign(
                  event_offset = offset.toHexString,
                  update_id = u.updateId,
                  command_id = u.optCompletionInfo.map(_.commandId),
                  workflow_id = u.workflowId,
                  submitter = u.reassignmentInfo.submitter,
                  contract_id = unassign.contractId.coid,
                  template_id = templateId,
                  package_name = unassign.packageName,
                  flat_event_witnesses = flatEventWitnesses.toSet,
                  event_sequential_id = 0L, // this is filled later
                  source_domain_id = u.reassignmentInfo.sourceDomain.unwrap.toProtoPrimitive,
                  target_domain_id = u.reassignmentInfo.targetDomain.unwrap.toProtoPrimitive,
                  unassign_id = u.reassignmentInfo.unassignId.toMicros.toString,
                  reassignment_counter = u.reassignmentInfo.reassignmentCounter,
                  assignment_exclusivity = unassign.assignmentExclusivity.map(_.micros),
                  trace_context = serializedTraceContext,
                  record_time = u.recordTime.micros,
                )
              ) ++ flatEventWitnesses.map(
                DbDto.IdFilterUnassignStakeholder(
                  0L, // this is filled later
                  templateId,
                  _,
                )
              )
            case assign: Reassignment.Assign =>
              val templateId = assign.createNode.templateId.toString
              val flatEventWitnesses =
                assign.createNode.stakeholders.map(_.toString)
              val (createArgument, createKeyValue) = translation.serialize(assign.createNode)
              Iterator(
                DbDto.EventAssign(
                  event_offset = offset.toHexString,
                  update_id = u.updateId,
                  command_id = u.optCompletionInfo.map(_.commandId),
                  workflow_id = u.workflowId,
                  submitter = u.reassignmentInfo.submitter,
                  contract_id = assign.createNode.coid.coid,
                  template_id = templateId,
                  package_name = assign.createNode.packageName,
                  package_version = assign.createNode.packageVersion.map(_.toString()),
                  flat_event_witnesses = flatEventWitnesses,
                  create_argument = createArgument,
                  create_signatories = assign.createNode.signatories.map(_.toString),
                  create_observers = assign.createNode.stakeholders
                    .diff(assign.createNode.signatories)
                    .map(_.toString),
                  create_key_value = createKeyValue
                    .map(compressionStrategy.createKeyValueCompression.compress),
                  create_key_maintainers =
                    assign.createNode.keyOpt.map(_.maintainers.map(_.toString)),
                  create_key_hash =
                    assign.createNode.keyOpt.map(_.globalKey.hash.bytes.toHexString),
                  create_argument_compression = compressionStrategy.createArgumentCompression.id,
                  create_key_value_compression =
                    compressionStrategy.createKeyValueCompression.id.filter(_ =>
                      createKeyValue.isDefined
                    ),
                  event_sequential_id = 0L, // this is filled later
                  ledger_effective_time = assign.ledgerEffectiveTime.micros,
                  driver_metadata = assign.contractMetadata.toByteArray,
                  source_domain_id = u.reassignmentInfo.sourceDomain.unwrap.toProtoPrimitive,
                  target_domain_id = u.reassignmentInfo.targetDomain.unwrap.toProtoPrimitive,
                  unassign_id = u.reassignmentInfo.unassignId.toMicros.toString,
                  reassignment_counter = u.reassignmentInfo.reassignmentCounter,
                  trace_context = serializedTraceContext,
                  record_time = u.recordTime.micros,
                )
              ) ++ flatEventWitnesses.map(
                DbDto.IdFilterAssignStakeholder(
                  0L, // this is filled later
                  templateId,
                  _,
                )
              )
          }

          val domainId = u.reassignment match {
            case _: Reassignment.Unassign =>
              u.reassignmentInfo.sourceDomain.unwrap.toProtoPrimitive
            case _: Reassignment.Assign =>
              u.reassignmentInfo.targetDomain.unwrap.toProtoPrimitive
          }
          val completions = u.optCompletionInfo.iterator.map(
            commandCompletion(
              offset = offset,
              recordTime = u.recordTime,
              transactionId = Some(u.updateId),
              _,
              domainId = domainId,
              requestIndex = u.domainIndex.flatMap(_.requestIndex),
              serializedTraceContext = serializedTraceContext,
              isTransaction = false,
            )
          )

          val transactionMeta = DbDto.TransactionMeta(
            transaction_id = u.updateId,
            event_offset = offset.toHexString,
            publication_time = 0, // this is filled later
            record_time = u.recordTime.micros,
            domain_id = domainId,
            event_sequential_id_first = 0, // this is filled later
            event_sequential_id_last = 0, // this is filled later
          )

          // TransactionMeta DTO must come last in this sequence
          // because in a later stage the preceding events
          // will be assigned consecutive event sequential ids
          // and transaction meta is assigned sequential ids of its first and last event
          events ++ completions ++ Seq(transactionMeta)

        case u: SequencerIndexMoved =>
          // nothing to persist, this is only a synthetic DbDto to facilitate updating the StringInterning
          Iterator(DbDto.SequencerIndexMoved(u.domainId.toProtoPrimitive))

        case _: CommitRepair =>
          Iterator.empty
      }
  }

  private def incrementCounterForEvent(
      metrics: IndexedUpdatesMetrics,
      eventType: String,
      status: String,
  )(implicit
      mc: MetricsContext
  ): Unit =
    withExtraMetricLabels(
      IndexedUpdatesMetrics.Labels.eventType.key -> eventType,
      IndexedUpdatesMetrics.Labels.status.key -> status,
    ) { implicit mc =>
      metrics.eventsMeter.mark()
    }
  private def commandCompletion(
      offset: Offset,
      recordTime: Time.Timestamp,
      transactionId: Option[Ref.TransactionId],
      completionInfo: CompletionInfo,
      domainId: String,
      requestIndex: Option[RequestIndex],
      isTransaction: Boolean,
      serializedTraceContext: Array[Byte],
  ): DbDto.CommandCompletion = {
    val (deduplicationOffset, deduplicationDurationSeconds, deduplicationDurationNanos) =
      completionInfo.optDeduplicationPeriod
        .map {
          case DeduplicationOffset(offset) =>
            (Some(offset.toHexString), None, None)
          case DeduplicationDuration(duration) =>
            (None, Some(duration.getSeconds), Some(duration.getNano))
        }
        .getOrElse((None, None, None))

    DbDto.CommandCompletion(
      completion_offset = offset.toHexString,
      record_time = recordTime.micros,
      publication_time = 0L, // will be filled later
      application_id = completionInfo.applicationId,
      submitters = completionInfo.actAs.toSet,
      command_id = completionInfo.commandId,
      transaction_id = transactionId,
      rejection_status_code = None,
      rejection_status_message = None,
      rejection_status_details = None,
      submission_id = completionInfo.submissionId,
      deduplication_offset = deduplicationOffset,
      deduplication_duration_seconds = deduplicationDurationSeconds,
      deduplication_duration_nanos = deduplicationDurationNanos,
      deduplication_start = None,
      domain_id = domainId,
      message_uuid = completionInfo.messageUuid.map(_.toString),
      request_sequencer_counter = requestIndex.flatMap(_.sequencerCounter).map(_.unwrap),
      is_transaction = isTransaction,
      trace_context = serializedTraceContext,
    )
  }
}
