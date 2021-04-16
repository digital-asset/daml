// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import java.util.UUID

import com.daml.ledger.api.domain
import com.daml.ledger.participant.state.v1.{Configuration, Offset, ParticipantId, Update}
import com.daml.lf.engine.Blinding
import com.daml.lf.ledger.EventId
import com.daml.platform.indexer.parallel
import com.daml.platform.store.Conversions
import com.daml.platform.store.appendonlydao.events._
import com.daml.platform.store.appendonlydao.JdbcLedgerDao

// TODO append-only: target to separation per update-type to it's own function + unit tests
object UpdateToDBDTOV1 {

  def apply(
      participantId: ParticipantId,
      translation: LfValueTranslation,
      compressionStrategy: CompressionStrategy,
  ): Offset => Update => Iterator[DBDTOV1] = { offset =>
    {
      case u: Update.CommandRejected =>
        // TODO append-only: we might want to tune up deduplications so it is also a temporal query
        Iterator(
          new DBDTOV1.CommandCompletion(
            completion_offset = offset.toByteArray,
            record_time = u.recordTime.toInstant,
            application_id = u.submitterInfo.applicationId,
            submitters = u.submitterInfo.actAs.toSet,
            command_id = u.submitterInfo.commandId,
            transaction_id = None,
            status_code = Some(Conversions.participantRejectionReasonToErrorCode(u.reason).value()),
            status_message = Some(u.reason.description),
          ),
          new DBDTOV1.CommandDeduplication(
            JdbcLedgerDao.deduplicationKey(
              domain.CommandId(u.submitterInfo.commandId),
              u.submitterInfo.actAs,
            )
          ),
        )

      case u: Update.ConfigurationChanged =>
        Iterator(
          new DBDTOV1.ConfigurationEntry(
            ledger_offset = offset.toByteArray,
            recorded_at = u.recordTime.toInstant,
            submission_id = u.submissionId,
            typ = JdbcLedgerDao.acceptType,
            configuration = Configuration.encode(u.newConfiguration).toByteArray,
            rejection_reason = None,
          )
        )

      case u: Update.ConfigurationChangeRejected =>
        Iterator(
          new DBDTOV1.ConfigurationEntry(
            ledger_offset = offset.toByteArray,
            recorded_at = u.recordTime.toInstant,
            submission_id = u.submissionId,
            typ = JdbcLedgerDao.rejectType,
            configuration = Configuration.encode(u.proposedConfiguration).toByteArray,
            rejection_reason = Some(u.rejectionReason),
          )
        )

      case u: Update.PartyAddedToParticipant =>
        Iterator(
          new DBDTOV1.PartyEntry(
            ledger_offset = offset.toByteArray,
            recorded_at = u.recordTime.toInstant,
            submission_id = u.submissionId,
            party = Some(u.party),
            display_name = Some(u.displayName),
            typ = JdbcLedgerDao.acceptType,
            rejection_reason = None,
            is_local = Some(u.participantId == participantId),
          ),
          new DBDTOV1.Party(
            party = u.party,
            display_name = Some(u.displayName),
            explicit = true,
            ledger_offset = Some(offset.toByteArray),
            is_local = u.participantId == participantId,
          ),
        )

      case u: Update.PartyAllocationRejected =>
        Iterator(
          new DBDTOV1.PartyEntry(
            ledger_offset = offset.toByteArray,
            recorded_at = u.recordTime.toInstant,
            submission_id = Some(u.submissionId),
            party = None,
            display_name = None,
            typ = JdbcLedgerDao.rejectType,
            rejection_reason = Some(u.rejectionReason),
            is_local = None,
          )
        )

      case u: Update.PublicPackageUpload =>
        val uploadId = u.submissionId.getOrElse(UUID.randomUUID().toString)
        val packages = u.archives.iterator.map { archive =>
          new DBDTOV1.Package(
            package_id = archive.getHash,
            upload_id = uploadId,
            source_description = u.sourceDescription,
            size = archive.getPayload.size.toLong,
            known_since = u.recordTime.toInstant,
            ledger_offset = offset.toByteArray,
            _package = archive.toByteArray,
          )
        }
        val packageEntries = u.submissionId.iterator.map(submissionId =>
          new DBDTOV1.PackageEntry(
            ledger_offset = offset.toByteArray,
            recorded_at = u.recordTime.toInstant,
            submission_id = Some(submissionId),
            typ = JdbcLedgerDao.acceptType,
            rejection_reason = None,
          )
        )
        packages ++ packageEntries

      case u: Update.PublicPackageUploadRejected =>
        Iterator(
          new DBDTOV1.PackageEntry(
            ledger_offset = offset.toByteArray,
            recorded_at = u.recordTime.toInstant,
            submission_id = Some(u.submissionId),
            typ = JdbcLedgerDao.rejectType,
            rejection_reason = Some(u.rejectionReason),
          )
        )

      case u: Update.TransactionAccepted =>
        val blinding = u.blindingInfo.getOrElse(Blinding.blind(u.transaction))
        val preorderTraversal = u.transaction
          .fold(List.empty[(NodeId, Node)]) { case (xs, x) =>
            x :: xs
          }
          .reverse

        val events: Iterator[DBDTOV1] = preorderTraversal.iterator
          .collect { // It is okay to collect: blinding info is already there, we are free at hand to filter out the fetch and lookup nodes here already
            case (nodeId, create: Create) =>
              val eventId = EventId(u.transactionId, nodeId)
              val (createArgument, createKeyValue) = translation.serialize(eventId, create)
              new DBDTOV1.EventCreate(
                event_offset = Some(offset.toByteArray),
                transaction_id = Some(u.transactionId),
                ledger_effective_time = Some(u.transactionMeta.ledgerEffectiveTime.toInstant),
                command_id = u.optSubmitterInfo.map(_.commandId),
                workflow_id = u.transactionMeta.workflowId,
                application_id = u.optSubmitterInfo.map(_.applicationId),
                submitters = u.optSubmitterInfo.map(_.actAs.toSet),
                node_index = Some(nodeId.index),
                event_id = Some(eventId.toLedgerString),
                contract_id = create.coid.coid,
                template_id = Some(create.coinst.template.toString),
                flat_event_witnesses = create.stakeholders.map(_.toString),
                tree_event_witnesses =
                  blinding.disclosure.getOrElse(nodeId, Set.empty).map(_.toString),
                create_argument = Some(createArgument)
                  .map(compressionStrategy.createArgumentCompression.compress),
                create_signatories = Some(create.signatories.map(_.toString)),
                create_observers =
                  Some(create.stakeholders.diff(create.signatories).map(_.toString)),
                create_agreement_text = Some(create.coinst.agreementText).filter(_.nonEmpty),
                create_key_value = createKeyValue
                  .map(compressionStrategy.createKeyValueCompression.compress),
                create_key_hash = create.key
                  .map(convertLfValueKey(create.templateId, _))
                  .map(_.hash.bytes.toByteArray),
                create_argument_compression = compressionStrategy.createArgumentCompression.id,
                create_key_value_compression =
                  compressionStrategy.createKeyValueCompression.id.filter(_ =>
                    createKeyValue.isDefined
                  ),
              )

            case (nodeId, exercise: Exercise) =>
              val eventId = EventId(u.transactionId, nodeId)
              val (exerciseArgument, exerciseResult, _) =
                translation.serialize(eventId, exercise)
              new parallel.DBDTOV1.EventExercise(
                consuming = exercise.consuming,
                event_offset = Some(offset.toByteArray),
                transaction_id = Some(u.transactionId),
                ledger_effective_time = Some(u.transactionMeta.ledgerEffectiveTime.toInstant),
                command_id = u.optSubmitterInfo.map(_.commandId),
                workflow_id = u.transactionMeta.workflowId,
                application_id = u.optSubmitterInfo.map(_.applicationId),
                submitters = u.optSubmitterInfo.map(_.actAs.toSet),
                node_index = Some(nodeId.index),
                event_id = Some(EventId(u.transactionId, nodeId).toLedgerString),
                contract_id = exercise.targetCoid.coid,
                template_id = Some(exercise.templateId.toString),
                flat_event_witnesses =
                  if (exercise.consuming) exercise.stakeholders.map(_.toString) else Set.empty,
                tree_event_witnesses =
                  blinding.disclosure.getOrElse(nodeId, Set.empty).map(_.toString),
                // TODO append-only: createKeyValue is not stored, check whether we need it for upcoming ledger API additions
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
                exercise_argument_compression = compressionStrategy.exerciseArgumentCompression.id,
                exercise_result_compression = compressionStrategy.exerciseResultCompression.id,
              )
          }

        val divulgedContractIndex = u.divulgedContracts
          .map(divulgedContract => divulgedContract.contractId -> divulgedContract)
          .toMap
        val divulgences = blinding.divulgence.iterator.map { case (contractId, visibleToParties) =>
          val contractInst = divulgedContractIndex.get(contractId).map(_.contractInst)
          new DBDTOV1.EventDivulgence(
            event_offset = Some(offset.toByteArray),
            command_id = u.optSubmitterInfo.map(_.commandId),
            workflow_id = u.transactionMeta.workflowId,
            application_id = u.optSubmitterInfo.map(_.applicationId),
            submitters = u.optSubmitterInfo.map(_.actAs.toSet),
            contract_id = contractId.coid,
            template_id = contractInst.map(_.template.toString),
            tree_event_witnesses = visibleToParties.map(_.toString),
            create_argument = contractInst
              .map(_.arg)
              .map(translation.serialize(contractId, _))
              .map(compressionStrategy.createArgumentCompression.compress),
            create_argument_compression = compressionStrategy.createArgumentCompression.id,
          )
        }

        val completions = u.optSubmitterInfo.iterator.map { submitterInfo =>
          new DBDTOV1.CommandCompletion(
            completion_offset = offset.toByteArray,
            record_time = u.recordTime.toInstant,
            application_id = submitterInfo.applicationId,
            submitters = submitterInfo.actAs.toSet,
            command_id = submitterInfo.commandId,
            transaction_id = Some(u.transactionId),
            status_code = None,
            status_message = None,
          )
        }

        events ++ divulgences ++ completions
    }
  }

}
