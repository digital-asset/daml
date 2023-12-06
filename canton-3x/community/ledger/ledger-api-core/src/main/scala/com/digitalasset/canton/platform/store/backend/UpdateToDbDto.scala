// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.lf.data.{Ref, Time}
import com.daml.lf.engine.Blinding
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.Transaction.ChildrenRecursion
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.MetricsContext.{withExtraMetricLabels, withOptionalMetricLabels}
import com.daml.platform.index.index.StatusDetails
import com.digitalasset.canton.ledger.api.DeduplicationPeriod.{
  DeduplicationDuration,
  DeduplicationOffset,
}
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.v2.{CompletionInfo, Reassignment, Update}
import com.digitalasset.canton.metrics.{IndexedUpdatesMetrics, Metrics}
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.store.dao.JdbcLedgerDao
import com.digitalasset.canton.platform.store.dao.events.*
import com.digitalasset.canton.tracing.{SerializableTraceContext, Traced}
import io.grpc.Status

import java.util.UUID

object UpdateToDbDto {

  def apply(
      participantId: Ref.ParticipantId,
      translation: LfValueSerialization,
      compressionStrategy: CompressionStrategy,
      metrics: Metrics,
      multiDomainEnabled: Boolean,
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
              metrics.daml.indexerEvents,
              IndexedUpdatesMetrics.Labels.eventType.transaction,
              IndexedUpdatesMetrics.Labels.status.rejected,
            )
          }
          val domainId = Option.when(multiDomainEnabled)(u.domainId.toProtoPrimitive)
          Iterator(
            commandCompletion(
              offset = offset,
              recordTime = u.recordTime,
              transactionId = None,
              completionInfo = u.completionInfo,
              domainId = domainId,
              serializedTraceContext = serializedTraceContext,
            ).copy(
              rejection_status_code = Some(u.reasonTemplate.code),
              rejection_status_message = Some(u.reasonTemplate.message),
              rejection_status_details =
                Some(StatusDetails.of(u.reasonTemplate.status.details).toByteArray),
            )
          )

        case u: ConfigurationChanged =>
          incrementCounterForEvent(
            metrics.daml.indexerEvents,
            IndexedUpdatesMetrics.Labels.eventType.configurationChange,
            IndexedUpdatesMetrics.Labels.status.accepted,
          )
          Iterator(
            DbDto.ConfigurationEntry(
              ledger_offset = offset.toHexString,
              recorded_at = u.recordTime.micros,
              submission_id = u.submissionId,
              typ = JdbcLedgerDao.acceptType,
              configuration = Configuration.encode(u.newConfiguration).toByteArray,
              rejection_reason = None,
            )
          )

        case u: ConfigurationChangeRejected =>
          incrementCounterForEvent(
            metrics.daml.indexerEvents,
            IndexedUpdatesMetrics.Labels.eventType.configurationChange,
            IndexedUpdatesMetrics.Labels.status.rejected,
          )
          Iterator(
            DbDto.ConfigurationEntry(
              ledger_offset = offset.toHexString,
              recorded_at = u.recordTime.micros,
              submission_id = u.submissionId,
              typ = JdbcLedgerDao.rejectType,
              configuration = Configuration.encode(u.proposedConfiguration).toByteArray,
              rejection_reason = Some(u.rejectionReason),
            )
          )

        case u: PartyAddedToParticipant =>
          incrementCounterForEvent(
            metrics.daml.indexerEvents,
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
            metrics.daml.indexerEvents,
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

        case u: PublicPackageUpload =>
          incrementCounterForEvent(
            metrics.daml.indexerEvents,
            IndexedUpdatesMetrics.Labels.eventType.packageUpload,
            IndexedUpdatesMetrics.Labels.status.accepted,
          )
          val uploadId = u.submissionId.getOrElse(UUID.randomUUID().toString)
          val packages = u.archives.iterator.map { archive =>
            DbDto.Package(
              package_id = archive.getHash,
              upload_id = uploadId,
              source_description = u.sourceDescription,
              package_size = archive.getPayload.size.toLong,
              known_since = u.recordTime.micros,
              ledger_offset = offset.toHexString,
              _package = archive.toByteArray,
            )
          }
          val packageEntries = u.submissionId.iterator.map(submissionId =>
            DbDto.PackageEntry(
              ledger_offset = offset.toHexString,
              recorded_at = u.recordTime.micros,
              submission_id = Some(submissionId),
              typ = JdbcLedgerDao.acceptType,
              rejection_reason = None,
            )
          )
          packages ++ packageEntries

        case u: PublicPackageUploadRejected =>
          incrementCounterForEvent(
            metrics.daml.indexerEvents,
            IndexedUpdatesMetrics.Labels.eventType.packageUpload,
            IndexedUpdatesMetrics.Labels.status.rejected,
          )
          Iterator(
            DbDto.PackageEntry(
              ledger_offset = offset.toHexString,
              recorded_at = u.recordTime.micros,
              submission_id = Some(u.submissionId),
              typ = JdbcLedgerDao.rejectType,
              rejection_reason = Some(u.rejectionReason),
            )
          )

        case u: TransactionAccepted =>
          withOptionalMetricLabels(
            IndexedUpdatesMetrics.Labels.applicationId -> u.completionInfoO.map(_.applicationId)
          ) { implicit mc: MetricsContext =>
            incrementCounterForEvent(
              metrics.daml.indexerEvents,
              IndexedUpdatesMetrics.Labels.eventType.transaction,
              IndexedUpdatesMetrics.Labels.status.accepted,
            )
          }
          val blinding = u.blindingInfoO.getOrElse(Blinding.blind(u.transaction))
          // TODO(i12283) LLP: Extract in common functionality together with duplicated code in [[InMemoryStateUpdater]]
          val preorderTraversal = u.transaction
            .foldInExecutionOrder(List.empty[(NodeId, Node)])(
              exerciseBegin =
                (acc, nid, node) => ((nid -> node) :: acc, ChildrenRecursion.DoRecurse),
              // Rollback nodes are not included in the indexer
              rollbackBegin = (acc, _, _) => (acc, ChildrenRecursion.DoNotRecurse),
              leaf = (acc, nid, node) => (nid -> node) :: acc,
              exerciseEnd = (acc, _, _) => acc,
              rollbackEnd = (acc, _, _) => acc,
            )
            .reverse

          val transactionMeta = DbDto.TransactionMeta(
            transaction_id = u.transactionId,
            event_offset = offset.toHexString,
            event_sequential_id_first = 0, // this is filled later
            event_sequential_id_last = 0, // this is filled later
          )
          val domainId = Option.when(multiDomainEnabled)(u.domainId.toProtoPrimitive)
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
                    event_offset = Some(offset.toHexString),
                    transaction_id = Some(u.transactionId),
                    ledger_effective_time = Some(u.transactionMeta.ledgerEffectiveTime.micros),
                    command_id = u.completionInfoO.map(_.commandId),
                    workflow_id = u.transactionMeta.workflowId,
                    application_id = u.completionInfoO.map(_.applicationId),
                    submitters = u.completionInfoO.map(_.actAs.toSet),
                    node_index = Some(nodeId.index),
                    event_id = Some(eventId.toLedgerString),
                    contract_id = create.coid.coid,
                    template_id = Some(templateId),
                    flat_event_witnesses = stakeholders,
                    tree_event_witnesses = informees,
                    create_argument = Some(createArgument)
                      .map(compressionStrategy.createArgumentCompression.compress),
                    create_signatories = Some(create.signatories.map(_.toString)),
                    create_observers =
                      Some(create.stakeholders.diff(create.signatories).map(_.toString)),
                    create_agreement_text = Some(create.agreementText).filter(_.nonEmpty),
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
                    event_offset = Some(offset.toHexString),
                    transaction_id = Some(u.transactionId),
                    ledger_effective_time = Some(u.transactionMeta.ledgerEffectiveTime.micros),
                    command_id = u.completionInfoO.map(_.commandId),
                    workflow_id = u.transactionMeta.workflowId,
                    application_id = u.completionInfoO.map(_.applicationId),
                    submitters = u.completionInfoO.map(_.actAs.toSet),
                    node_index = Some(nodeId.index),
                    event_id = Some(EventId(u.transactionId, nodeId).toLedgerString),
                    contract_id = exercise.targetCoid.coid,
                    template_id = Some(templateId),
                    flat_event_witnesses = flatWitnesses,
                    tree_event_witnesses = informees,
                    create_key_value = createKeyValue
                      .map(compressionStrategy.createKeyValueCompression.compress),
                    exercise_choice = Some(exercise.qualifiedChoiceName.toString),
                    exercise_argument = Some(exerciseArgument)
                      .map(compressionStrategy.exerciseArgumentCompression.compress),
                    exercise_result = exerciseResult
                      .map(compressionStrategy.exerciseResultCompression.compress),
                    exercise_actors = Some(exercise.actingParties.map(_.toString)),
                    exercise_child_event_ids = Some(
                      exercise.children.iterator
                        .map(EventId(u.transactionId, _).toLedgerString.toString)
                        .toVector
                    ),
                    create_key_value_compression = compressionStrategy.createKeyValueCompression.id,
                    exercise_argument_compression =
                      compressionStrategy.exerciseArgumentCompression.id,
                    exercise_result_compression = compressionStrategy.exerciseResultCompression.id,
                    event_sequential_id = 0, // this is filled later
                    domain_id = domainId,
                    trace_context = serializedTraceContext,
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

          val divulgedContractIndex = u.divulgedContracts
            .map(divulgedContract => divulgedContract.contractId -> divulgedContract)
            .toMap
          val divulgences = blinding.divulgence.iterator.collect {
            // only store divulgence events, which are divulging to parties
            case (contractId, visibleToParties) if visibleToParties.nonEmpty =>
              val contractInst = divulgedContractIndex.get(contractId).map(_.contractInst)
              DbDto.EventDivulgence(
                event_offset = Some(offset.toHexString),
                command_id = u.completionInfoO.map(_.commandId),
                workflow_id = u.transactionMeta.workflowId,
                application_id = u.completionInfoO.map(_.applicationId),
                submitters = u.completionInfoO.map(_.actAs.toSet),
                contract_id = contractId.coid,
                template_id = contractInst.map(_.unversioned.template.toString),
                tree_event_witnesses = visibleToParties.map(_.toString),
                create_argument = contractInst
                  .map(_.map(_.arg))
                  .map(translation.serialize(contractId, _))
                  .map(compressionStrategy.createArgumentCompression.compress),
                create_argument_compression = compressionStrategy.createArgumentCompression.id,
                event_sequential_id = 0, // this is filled later
                domain_id = domainId,
              )
          }

          val completions =
            u.completionInfoO.iterator.map(
              commandCompletion(
                offset = offset,
                recordTime = u.recordTime,
                transactionId = Some(u.transactionId),
                _,
                domainId = domainId,
                serializedTraceContext = serializedTraceContext,
              )
            )

          // TransactionMeta DTO must come last in this sequence
          // because in a later stage the preceding events
          // will be assigned consecutive event sequential ids
          // and transaction meta is assigned sequential ids of its first and last event
          events ++ divulgences ++ completions ++ Seq(transactionMeta)

        case u: ReassignmentAccepted if multiDomainEnabled =>
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
                  flat_event_witnesses = flatEventWitnesses.toSet,
                  event_sequential_id = 0L, // this is filled later
                  source_domain_id = u.reassignmentInfo.sourceDomain.unwrap.toProtoPrimitive,
                  target_domain_id = u.reassignmentInfo.targetDomain.unwrap.toProtoPrimitive,
                  unassign_id = u.reassignmentInfo.unassignId.toMicros.toString,
                  reassignment_counter = u.reassignmentInfo.reassignmentCounter,
                  assignment_exclusivity = unassign.assignmentExclusivity.map(_.micros),
                  trace_context = serializedTraceContext,
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
                  flat_event_witnesses = flatEventWitnesses,
                  create_argument = createArgument,
                  create_signatories = assign.createNode.signatories.map(_.toString),
                  create_observers = assign.createNode.stakeholders
                    .diff(assign.createNode.signatories)
                    .map(_.toString),
                  create_agreement_text = Some(assign.createNode.agreementText).filter(_.nonEmpty),
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
                )
              ) ++ flatEventWitnesses.map(
                DbDto.IdFilterAssignStakeholder(
                  0L, // this is filled later
                  templateId,
                  _,
                )
              )
          }

          val completions = u.optCompletionInfo.iterator.map(
            commandCompletion(
              offset = offset,
              recordTime = u.recordTime,
              transactionId = Some(u.updateId),
              _,
              domainId = u.reassignment match {
                case _: Reassignment.Unassign =>
                  Some(u.reassignmentInfo.sourceDomain.unwrap.toProtoPrimitive)
                case _: Reassignment.Assign =>
                  Some(u.reassignmentInfo.targetDomain.unwrap.toProtoPrimitive)
              },
              serializedTraceContext = serializedTraceContext,
            )
          )

          val transactionMeta = DbDto.TransactionMeta(
            transaction_id = u.updateId,
            event_offset = offset.toHexString,
            event_sequential_id_first = 0, // this is filled later
            event_sequential_id_last = 0, // this is filled later
          )

          // TransactionMeta DTO must come last in this sequence
          // because in a later stage the preceding events
          // will be assigned consecutive event sequential ids
          // and transaction meta is assigned sequential ids of its first and last event
          events ++ completions ++ Seq(transactionMeta)

        case _: ReassignmentAccepted => Iterator.empty
      }
  }

  private def incrementCounterForEvent(
      metrics: IndexedUpdatesMetrics,
      eventType: String,
      status: String,
  )(implicit
      mc: MetricsContext
  ): Unit = {
    withExtraMetricLabels(
      IndexedUpdatesMetrics.Labels.eventType.key -> eventType,
      IndexedUpdatesMetrics.Labels.status.key -> status,
    ) { implicit mc =>
      metrics.eventsMeter.mark()
    }
  }
  private def commandCompletion(
      offset: Offset,
      recordTime: Time.Timestamp,
      transactionId: Option[Ref.TransactionId],
      completionInfo: CompletionInfo,
      domainId: Option[String],
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
      trace_context = serializedTraceContext,
    )
  }
}
