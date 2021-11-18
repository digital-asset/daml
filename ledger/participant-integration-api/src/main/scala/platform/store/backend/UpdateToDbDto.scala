// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.util.UUID
import com.daml.ledger.api.DeduplicationPeriod.{DeduplicationDuration, DeduplicationOffset}
import com.daml.ledger.api.domain
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.CompletionInfo
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.engine.Blinding
import com.daml.lf.ledger.EventId
import com.daml.platform.index.index.StatusDetails
import com.daml.platform.store.appendonlydao.{DeduplicationKeyMaker, JdbcLedgerDao}
import com.daml.platform.store.appendonlydao.events._

object UpdateToDbDto {

  def apply(
      participantId: Ref.ParticipantId,
      translation: LfValueSerialization,
      compressionStrategy: CompressionStrategy,
  ): Offset => state.Update => Iterator[DbDto] = { offset =>
    import state.Update._
    {
      case u: CommandRejected =>
        Iterator(
          commandCompletion(offset, u.recordTime, transactionId = None, u.completionInfo).copy(
            rejection_status_code = Some(u.reasonTemplate.code),
            rejection_status_message = Some(u.reasonTemplate.message),
            rejection_status_details =
              Some(StatusDetails.of(u.reasonTemplate.status.details).toByteArray),
          ),
          DbDto.CommandDeduplication(
            DeduplicationKeyMaker.make(
              domain.CommandId(u.completionInfo.commandId),
              u.completionInfo.actAs,
            )
          ),
        )

      case u: ConfigurationChanged =>
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
        // TODO append-only:
        //   Covering this functionality with unit test is important, since at the time of writing kvutils ledgers purge RollBack nodes already on WriteService, so conformance testing is impossible
        //   Unit tests also need to cover the full semantic contract regarding fetch and lookup node removal as well
        //   Investigate possibility to encapsulate this logic in a common place
        val blinding = u.blindingInfo.getOrElse(Blinding.blind(u.transaction))
        val preorderTraversal = u.transaction
          .foldInExecutionOrder(List.empty[(NodeId, Node)])(
            exerciseBegin = (acc, nid, node) => ((nid -> node) :: acc, true),
            // Rollback nodes are not included in the indexer
            rollbackBegin = (acc, _, _) => (acc, false),
            leaf = (acc, nid, node) => (nid -> node) :: acc,
            exerciseEnd = (acc, _, _) => acc,
            rollbackEnd = (acc, _, _) => acc,
          )
          .reverse

        val events: Iterator[DbDto] = preorderTraversal.iterator
          .flatMap {
            case (nodeId, create: Create) =>
              val eventId = EventId(u.transactionId, nodeId)
              val templateId = create.templateId.toString
              val stakeholders = create.stakeholders.map(_.toString)
              val (createArgument, createKeyValue) = translation.serialize(eventId, create)
              Iterator(
                DbDto.EventCreate(
                  event_offset = Some(offset.toHexString),
                  transaction_id = Some(u.transactionId),
                  ledger_effective_time = Some(u.transactionMeta.ledgerEffectiveTime.micros),
                  command_id = u.optCompletionInfo.map(_.commandId),
                  workflow_id = u.transactionMeta.workflowId,
                  application_id = u.optCompletionInfo.map(_.applicationId),
                  submitters = u.optCompletionInfo.map(_.actAs.toSet),
                  node_index = Some(nodeId.index),
                  event_id = Some(eventId.toLedgerString),
                  contract_id = create.coid.coid,
                  template_id = Some(templateId),
                  flat_event_witnesses = stakeholders,
                  tree_event_witnesses =
                    blinding.disclosure.getOrElse(nodeId, Set.empty).map(_.toString),
                  create_argument = Some(createArgument)
                    .map(compressionStrategy.createArgumentCompression.compress),
                  create_signatories = Some(create.signatories.map(_.toString)),
                  create_observers =
                    Some(create.stakeholders.diff(create.signatories).map(_.toString)),
                  create_agreement_text = Some(create.agreementText).filter(_.nonEmpty),
                  create_key_value = createKeyValue
                    .map(compressionStrategy.createKeyValueCompression.compress),
                  create_key_hash = create.key
                    .map(convertLfValueKey(create.templateId, _))
                    .map(_.hash.bytes.toHexString),
                  create_argument_compression = compressionStrategy.createArgumentCompression.id,
                  create_key_value_compression =
                    compressionStrategy.createKeyValueCompression.id.filter(_ =>
                      createKeyValue.isDefined
                    ),
                  event_sequential_id = 0, // this is filled later
                )
              ) ++ stakeholders.iterator.map(
                DbDto.CreateFilter(
                  event_sequential_id = 0, // this is filled later
                  template_id = templateId,
                  _,
                )
              )

            case (nodeId, exercise: Exercise) =>
              val eventId = EventId(u.transactionId, nodeId)
              val (exerciseArgument, exerciseResult, createKeyValue) =
                translation.serialize(eventId, exercise)
              Iterator(
                DbDto.EventExercise(
                  consuming = exercise.consuming,
                  event_offset = Some(offset.toHexString),
                  transaction_id = Some(u.transactionId),
                  ledger_effective_time = Some(u.transactionMeta.ledgerEffectiveTime.micros),
                  command_id = u.optCompletionInfo.map(_.commandId),
                  workflow_id = u.transactionMeta.workflowId,
                  application_id = u.optCompletionInfo.map(_.applicationId),
                  submitters = u.optCompletionInfo.map(_.actAs.toSet),
                  node_index = Some(nodeId.index),
                  event_id = Some(EventId(u.transactionId, nodeId).toLedgerString),
                  contract_id = exercise.targetCoid.coid,
                  template_id = Some(exercise.templateId.toString),
                  flat_event_witnesses =
                    if (exercise.consuming) exercise.stakeholders.map(_.toString) else Set.empty,
                  tree_event_witnesses =
                    blinding.disclosure.getOrElse(nodeId, Set.empty).map(_.toString),
                  create_key_value = createKeyValue
                    .map(compressionStrategy.createKeyValueCompression.compress),
                  exercise_choice = Some(exercise.choiceId),
                  exercise_argument = Some(exerciseArgument)
                    .map(compressionStrategy.exerciseArgumentCompression.compress),
                  exercise_result = exerciseResult
                    .map(compressionStrategy.exerciseResultCompression.compress),
                  exercise_actors = Some(exercise.actingParties.map(_.toString)),
                  exercise_child_event_ids = Some(
                    exercise.children.iterator
                      .map(EventId(u.transactionId, _).toLedgerString.toString)
                      .toSet
                  ),
                  create_key_value_compression = compressionStrategy.createKeyValueCompression.id,
                  exercise_argument_compression =
                    compressionStrategy.exerciseArgumentCompression.id,
                  exercise_result_compression = compressionStrategy.exerciseResultCompression.id,
                  event_sequential_id = 0, // this is filled later
                )
              )

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
              command_id = u.optCompletionInfo.map(_.commandId),
              workflow_id = u.transactionMeta.workflowId,
              application_id = u.optCompletionInfo.map(_.applicationId),
              submitters = u.optCompletionInfo.map(_.actAs.toSet),
              contract_id = contractId.coid,
              template_id = contractInst.map(_.unversioned.template.toString),
              tree_event_witnesses = visibleToParties.map(_.toString),
              create_argument = contractInst
                .map(_.map(_.arg))
                .map(translation.serialize(contractId, _))
                .map(compressionStrategy.createArgumentCompression.compress),
              create_argument_compression = compressionStrategy.createArgumentCompression.id,
              event_sequential_id = 0, // this is filled later
            )
        }

        val completions =
          u.optCompletionInfo.iterator.map(
            commandCompletion(offset, u.recordTime, Some(u.transactionId), _)
          )

        events ++ divulgences ++ completions
    }
  }

  private def commandCompletion(
      offset: Offset,
      recordTime: Time.Timestamp,
      transactionId: Option[Ref.TransactionId],
      completionInfo: CompletionInfo,
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
    )
  }
}
