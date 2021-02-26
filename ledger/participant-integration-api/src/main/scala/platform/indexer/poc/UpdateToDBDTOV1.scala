// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.poc

import java.util.UUID

import com.daml.ledger.api.domain
import com.daml.ledger.participant.state.v1.{Configuration, Offset, ParticipantId, Update}
import com.daml.lf.engine.Blinding
import com.daml.lf.ledger.EventId
import com.daml.platform.store.dao.events._
import com.daml.platform.store.dao.{CommandCompletionsTable, JdbcLedgerDao}

// TODO target to separation per update-type to it's own function + unit tests
object UpdateToDBDTOV1 {

  def apply(
      participantId: ParticipantId,
      translation: LfValueTranslation,
  ): Offset => Update => Iterator[DBDTOV1] = { offset =>
    {
      case u: Update.CommandRejected =>
        // TODO we might want to tune up deduplications so it is also a temporal query @simon@?
        val (statusCode, statusMessage) = CommandCompletionsTable.toStatus(u.reason)
        Iterator(
          new DBDTOV1.CommandCompletion(
            completion_offset = offset.toByteArray,
            record_time = u.recordTime.toInstant,
            application_id = u.submitterInfo.applicationId,
            submitters = u.submitterInfo.actAs.toSet,
            command_id = u.submitterInfo.commandId,
            transaction_id = None,
            status_code = Some(statusCode),
            status_message = Some(statusMessage),
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
            x :: xs //FIXME why not _ :: _ ?! COME ON! ;(
          }
          .reverse

        val events = preorderTraversal.iterator.map {
          case (nodeId, create: Create) =>
            val eventId = EventId(u.transactionId, nodeId)
            val (createArgument, createKeyValue) = translation.serialize(eventId, create)
            new DBDTOV1.Event(
              event_kind = 10,
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
              create_argument = Some(createArgument),
              create_signatories = Some(create.signatories.map(_.toString)),
              create_observers = Some(create.stakeholders.diff(create.signatories).map(_.toString)),
              create_agreement_text = Some(create.coinst.agreementText).filter(_.nonEmpty),
              create_key_value = createKeyValue,
              create_key_hash = create.key
                .map(convertLfValueKey(create.templateId, _))
                .map(_.hash.bytes.toByteArray),
              exercise_choice = None,
              exercise_argument = None,
              exercise_result = None,
              exercise_actors = None,
              exercise_child_event_ids = None,
            )

          case (nodeId, exercise: Exercise) =>
            val eventId = EventId(u.transactionId, nodeId)
            val (exerciseArgument, exerciseResult) = translation.serialize(eventId, exercise)
            new DBDTOV1.Event(
              event_kind = if (exercise.consuming) 20 else 25,
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
              create_argument = None,
              create_signatories = None,
              create_observers = None,
              create_agreement_text = None,
              create_key_value = None,
              create_key_hash = None,
              exercise_choice = Some(exercise.choiceId),
              exercise_argument = Some(exerciseArgument),
              exercise_result = exerciseResult,
              exercise_actors = Some(exercise.actingParties.map(_.toString)),
              exercise_child_event_ids = Some(
                exercise.children.iterator
                  .map(EventId(u.transactionId, _).toLedgerString.toString)
                  .toSet
              ),
            )

          case (nodeId, _) => throw new UnexpectedNodeException(nodeId, u.transactionId)
        }

        val divulgedContractIndex = u.divulgedContracts
          .map(divulgedContract => divulgedContract.contractId -> divulgedContract)
          .toMap
        val divulgences = blinding.divulgence.iterator.map { case (contractId, visibleToParties) =>
          val contractInst = divulgedContractIndex.get(contractId).map(_.contractInst)
          new DBDTOV1.Event(
            event_kind = 0,
            event_offset = None,
            transaction_id = None,
            ledger_effective_time = None,
            command_id = u.optSubmitterInfo.map(_.commandId),
            workflow_id = u.transactionMeta.workflowId,
            application_id = u.optSubmitterInfo.map(_.applicationId),
            submitters = u.optSubmitterInfo.map(_.actAs.toSet),
            node_index = None,
            event_id = None,
            contract_id = contractId.coid,
            template_id = contractInst.map(_.template.toString),
            flat_event_witnesses = Set.empty,
            tree_event_witnesses = visibleToParties.map(_.toString),
            create_argument = contractInst.map(_.arg).map(translation.serialize(contractId, _)),
            create_signatories = None,
            create_observers = None,
            create_agreement_text = None,
            create_key_value = None,
            create_key_hash = None,
            exercise_choice = None,
            exercise_argument = None,
            exercise_result = None,
            exercise_actors = None,
            exercise_child_event_ids = None,
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
