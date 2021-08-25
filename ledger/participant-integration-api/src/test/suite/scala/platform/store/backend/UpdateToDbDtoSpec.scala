// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.time.Duration

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.domain
import com.daml.ledger.api.v1.event.{CreatedEvent, ExercisedEvent}
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.crypto
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.BlindingInfo
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.platform.index.index.StatusDetails
import com.daml.platform.store.appendonlydao.JdbcLedgerDao
import com.daml.platform.store.appendonlydao.events.Raw.TreeEvent
import com.daml.platform.store.appendonlydao.events.{
  CompressionStrategy,
  ContractId,
  Create,
  Exercise,
  FieldCompressionStrategy,
  LfValueSerialization,
  Raw,
}
import com.daml.platform.store.dao.DeduplicationKeyMaker
import com.google.protobuf.ByteString
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status
import org.scalactic.TripleEquals._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}

class UpdateToDbDtoSpec extends AnyWordSpec with Matchers {

  import UpdateToDbDtoSpec._

  "UpdateToDbDto" should {

    "handle ConfigurationChanged" in {
      val update = state.Update.ConfigurationChanged(
        someRecordTime,
        someSubmissionId,
        someParticipantId,
        someConfiguration,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.ConfigurationEntry(
          ledger_offset = someOffset.toHexString,
          recorded_at = update.recordTime.toInstant,
          submission_id = update.submissionId,
          typ = JdbcLedgerDao.acceptType,
          configuration = Configuration.encode(update.newConfiguration).toByteArray,
          rejection_reason = None,
        )
      )
    }

    "handle ConfigurationChangeRejected" in {
      val rejectionReason = "Test rejection reason"
      val update = state.Update.ConfigurationChangeRejected(
        someRecordTime,
        someSubmissionId,
        someParticipantId,
        someConfiguration,
        rejectionReason,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.ConfigurationEntry(
          ledger_offset = someOffset.toHexString,
          recorded_at = someRecordTime.toInstant,
          submission_id = someSubmissionId,
          typ = JdbcLedgerDao.rejectType,
          configuration = Configuration.encode(someConfiguration).toByteArray,
          rejection_reason = Some(rejectionReason),
        )
      )
    }

    "handle PartyAddedToParticipant (local party)" in {
      val displayName = "Test party"
      val update = state.Update.PartyAddedToParticipant(
        someParty,
        displayName,
        someParticipantId,
        someRecordTime,
        Some(someSubmissionId),
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.PartyEntry(
          ledger_offset = someOffset.toHexString,
          recorded_at = someRecordTime.toInstant,
          submission_id = Some(someSubmissionId),
          party = Some(someParty),
          display_name = Some(displayName),
          typ = JdbcLedgerDao.acceptType,
          rejection_reason = None,
          is_local = Some(true),
        )
      )
    }

    "handle PartyAddedToParticipant (remote party)" in {
      val displayName = "Test party"
      val update = state.Update.PartyAddedToParticipant(
        someParty,
        displayName,
        otherParticipantId,
        someRecordTime,
        None,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.PartyEntry(
          ledger_offset = someOffset.toHexString,
          recorded_at = someRecordTime.toInstant,
          submission_id = None,
          party = Some(someParty),
          display_name = Some(displayName),
          typ = JdbcLedgerDao.acceptType,
          rejection_reason = None,
          is_local = Some(false),
        )
      )
    }

    "handle PartyAllocationRejected" in {
      val rejectionReason = "Test party rejection reason"
      val update = state.Update.PartyAllocationRejected(
        someSubmissionId,
        someParticipantId,
        someRecordTime,
        rejectionReason,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.PartyEntry(
          ledger_offset = someOffset.toHexString,
          recorded_at = someRecordTime.toInstant,
          submission_id = Some(someSubmissionId),
          party = None,
          display_name = None,
          typ = JdbcLedgerDao.rejectType,
          rejection_reason = Some(rejectionReason),
          is_local = None,
        )
      )
    }

    "handle PublicPackageUpload (two archives)" in {
      val sourceDescription = "Test source description"
      val update = state.Update.PublicPackageUpload(
        List(someArchive1, someArchive2),
        Some(sourceDescription),
        someRecordTime,
        Some(someSubmissionId),
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.Package(
          package_id = someArchive1.getHash,
          upload_id = someSubmissionId,
          source_description = Some(sourceDescription),
          package_size = someArchive1.getPayload.size.toLong,
          known_since = someRecordTime.toInstant,
          ledger_offset = someOffset.toHexString,
          _package = someArchive1.toByteArray,
        ),
        DbDto.Package(
          package_id = someArchive2.getHash,
          upload_id = someSubmissionId,
          source_description = Some(sourceDescription),
          package_size = someArchive2.getPayload.size.toLong,
          known_since = someRecordTime.toInstant,
          ledger_offset = someOffset.toHexString,
          _package = someArchive2.toByteArray,
        ),
        DbDto.PackageEntry(
          ledger_offset = someOffset.toHexString,
          recorded_at = someRecordTime.toInstant,
          submission_id = Some(someSubmissionId),
          typ = JdbcLedgerDao.acceptType,
          rejection_reason = None,
        ),
      )
    }

    "handle PublicPackageUploadRejected" in {
      val rejectionReason = "Test package rejection reason"
      val update = state.Update.PublicPackageUploadRejected(
        someSubmissionId,
        someRecordTime,
        rejectionReason,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.PackageEntry(
          ledger_offset = someOffset.toHexString,
          recorded_at = someRecordTime.toInstant,
          submission_id = Some(someSubmissionId),
          typ = JdbcLedgerDao.rejectType,
          rejection_reason = Some(rejectionReason),
        )
      )
    }

    "handle CommandRejected" in {
      val completionInfo = state.CompletionInfo(
        actAs = List(someParty),
        applicationId = someApplicationId,
        commandId = someCommandId,
        optDeduplicationPeriod = None,
        submissionId = someSubmissionId,
      )
      val status = StatusProto.of(Status.Code.ABORTED.value(), "test reason", Seq.empty)
      val update = state.Update.CommandRejected(
        someRecordTime,
        completionInfo,
        new state.Update.CommandRejected.FinalReason(status),
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.CommandCompletion(
          completion_offset = someOffset.toHexString,
          record_time = someRecordTime.toInstant,
          application_id = someApplicationId,
          submitters = Set(someParty),
          command_id = someCommandId,
          transaction_id = None,
          rejection_status_code = Some(status.code),
          rejection_status_message = Some(status.message),
          rejection_status_details = Some(StatusDetails.of(status.details).toByteArray),
        ),
        DbDto.CommandDeduplication(
          DeduplicationKeyMaker.make(
            domain.CommandId(completionInfo.commandId),
            completionInfo.actAs,
          )
        ),
      )
    }

    "handle TransactionAccepted (single create node)" in {
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val builder = new TransactionBuilder()
      val createNode = builder.create(
        id = builder.newCid,
        template = "pkgid:M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
        key = None,
      )
      val createNodeId = builder.add(createNode)
      val transaction = builder.buildCommitted()
      val update = state.Update.TransactionAccepted(
        optCompletionInfo = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        transactionId = Ref.TransactionId.assertFromString("TransactionId"),
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventCreate(
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.toInstant),
          command_id = Some(completionInfo.commandId),
          workflow_id = transactionMeta.workflowId,
          application_id = Some(completionInfo.applicationId),
          submitters = Some(completionInfo.actAs.toSet),
          node_index = Some(createNodeId.index),
          event_id = Some(EventId(update.transactionId, createNodeId).toLedgerString),
          contract_id = createNode.coid.coid,
          template_id = Some(createNode.coinst.template.toString),
          flat_event_witnesses = Set("signatory", "observer"), // stakeholders
          tree_event_witnesses = Set("signatory", "observer"), // informees
          create_argument = Some(emptyArray),
          create_signatories = Some(Set("signatory")),
          create_observers = Some(Set("observer")),
          create_agreement_text = None,
          create_key_value = None,
          create_key_hash = None,
          create_argument_compression = compressionAlgorithmId,
          create_key_value_compression = None,
          event_sequential_id = 0,
        ),
        DbDto.CommandCompletion(
          completion_offset = someOffset.toHexString,
          record_time = update.recordTime.toInstant,
          application_id = completionInfo.applicationId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          transaction_id = Some(update.transactionId),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
        ),
      )
    }

    "handle TransactionAccepted (single create node with agreement text)" in {
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val builder = new TransactionBuilder()
      val createNode = builder
        .create(
          id = builder.newCid,
          template = "pkgid:M:T",
          argument = Value.ValueUnit,
          signatories = List("signatory"),
          observers = List("observer"),
          key = None,
        )
        .copy(agreementText = "agreement text")
      val createNodeId = builder.add(createNode)
      val transaction = builder.buildCommitted()
      val update = state.Update.TransactionAccepted(
        optCompletionInfo = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        transactionId = Ref.TransactionId.assertFromString("TransactionId"),
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventCreate(
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.toInstant),
          command_id = Some(completionInfo.commandId),
          workflow_id = transactionMeta.workflowId,
          application_id = Some(completionInfo.applicationId),
          submitters = Some(completionInfo.actAs.toSet),
          node_index = Some(createNodeId.index),
          event_id = Some(EventId(update.transactionId, createNodeId).toLedgerString),
          contract_id = createNode.coid.coid,
          template_id = Some(createNode.coinst.template.toString),
          flat_event_witnesses = Set("signatory", "observer"), // stakeholders
          tree_event_witnesses = Set("signatory", "observer"), // informees
          create_argument = Some(emptyArray),
          create_signatories = Some(Set("signatory")),
          create_observers = Some(Set("observer")),
          create_agreement_text = Some(createNode.coinst.agreementText),
          create_key_value = None,
          create_key_hash = None,
          create_argument_compression = compressionAlgorithmId,
          create_key_value_compression = None,
          event_sequential_id = 0,
        ),
        DbDto.CommandCompletion(
          completion_offset = someOffset.toHexString,
          record_time = update.recordTime.toInstant,
          application_id = completionInfo.applicationId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          transaction_id = Some(update.transactionId),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
        ),
      )
    }

    "handle TransactionAccepted (single consuming exercise node)" in {
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val builder = new TransactionBuilder()
      val exerciseNode = {
        val createNode = builder.create(
          id = builder.newCid,
          template = "pkgid:M:T",
          argument = Value.ValueUnit,
          signatories = List("signatory"),
          observers = List("observer"),
          key = None,
        )
        builder.exercise(
          contract = createNode,
          choice = "someChoice",
          consuming = true,
          actingParties = Set("signatory"),
          argument = Value.ValueUnit,
          result = Some(Value.ValueUnit),
          choiceObservers = Set.empty,
          byKey = false,
        )
      }
      val exerciseNodeId = builder.add(exerciseNode)
      val transaction = builder.buildCommitted()
      val update = state.Update.TransactionAccepted(
        optCompletionInfo = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        transactionId = Ref.TransactionId.assertFromString("TransactionId"),
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventExercise(
          consuming = true,
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.toInstant),
          command_id = Some(completionInfo.commandId),
          workflow_id = transactionMeta.workflowId,
          application_id = Some(completionInfo.applicationId),
          submitters = Some(completionInfo.actAs.toSet),
          node_index = Some(exerciseNodeId.index),
          event_id = Some(EventId(update.transactionId, exerciseNodeId).toLedgerString),
          contract_id = exerciseNode.targetCoid.coid,
          template_id = Some(exerciseNode.templateId.toString),
          flat_event_witnesses = Set("signatory", "observer"), // stakeholders
          tree_event_witnesses = Set("signatory", "observer"), // informees
          create_key_value = None,
          exercise_choice = Some(exerciseNode.choiceId),
          exercise_argument = Some(emptyArray),
          exercise_result = Some(emptyArray),
          exercise_actors = Some(Set("signatory")),
          exercise_child_event_ids = Some(Set.empty),
          create_key_value_compression = compressionAlgorithmId,
          exercise_argument_compression = compressionAlgorithmId,
          exercise_result_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.CommandCompletion(
          completion_offset = someOffset.toHexString,
          record_time = update.recordTime.toInstant,
          application_id = completionInfo.applicationId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          transaction_id = Some(update.transactionId),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
        ),
      )
    }

    "handle TransactionAccepted (single non-consuming exercise node)" in {
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val builder = new TransactionBuilder()
      val exerciseNode = {
        val createNode = builder.create(
          id = builder.newCid,
          template = "pkgid:M:T",
          argument = Value.ValueUnit,
          signatories = List("signatory"),
          observers = List("observer"),
          key = None,
        )
        builder.exercise(
          contract = createNode,
          choice = "someChoice",
          consuming = false,
          actingParties = Set("signatory"),
          argument = Value.ValueUnit,
          result = Some(Value.ValueUnit),
          choiceObservers = Set.empty,
          byKey = false,
        )
      }
      val exerciseNodeId = builder.add(exerciseNode)
      val transaction = builder.buildCommitted()
      val update = state.Update.TransactionAccepted(
        optCompletionInfo = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        transactionId = Ref.TransactionId.assertFromString("TransactionId"),
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventExercise(
          consuming = false,
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.toInstant),
          command_id = Some(completionInfo.commandId),
          workflow_id = transactionMeta.workflowId,
          application_id = Some(completionInfo.applicationId),
          submitters = Some(completionInfo.actAs.toSet),
          node_index = Some(exerciseNodeId.index),
          event_id = Some(EventId(update.transactionId, exerciseNodeId).toLedgerString),
          contract_id = exerciseNode.targetCoid.coid,
          template_id = Some(exerciseNode.templateId.toString),
          flat_event_witnesses = Set.empty, // stakeholders
          tree_event_witnesses = Set("signatory"), // informees
          create_key_value = None,
          exercise_choice = Some(exerciseNode.choiceId),
          exercise_argument = Some(emptyArray),
          exercise_result = Some(emptyArray),
          exercise_actors = Some(Set("signatory")),
          exercise_child_event_ids = Some(Set.empty),
          create_key_value_compression = compressionAlgorithmId,
          exercise_argument_compression = compressionAlgorithmId,
          exercise_result_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.CommandCompletion(
          completion_offset = someOffset.toHexString,
          record_time = update.recordTime.toInstant,
          application_id = completionInfo.applicationId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          transaction_id = Some(update.transactionId),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
        ),
      )
    }

    "handle TransactionAccepted (nested exercise nodes)" in {
      // Previous transaction
      // └─ #1 Create
      // Transaction
      // └─ #2 Exercise (choice A)
      //    ├─ #3 Exercise (choice B)
      //    └─ #4 Exercise (choice C)
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val builder = new TransactionBuilder()
      val createNode = builder.create(
        id = builder.newCid,
        template = "pkgid:M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
        key = None,
      )
      val exerciseNodeA = builder.exercise(
        contract = createNode,
        choice = "A",
        consuming = false,
        actingParties = Set("signatory"),
        argument = Value.ValueUnit,
        result = Some(Value.ValueUnit),
        choiceObservers = Set.empty,
        byKey = false,
      )
      val exerciseNodeB = builder.exercise(
        contract = createNode,
        choice = "B",
        consuming = false,
        actingParties = Set("signatory"),
        argument = Value.ValueUnit,
        result = Some(Value.ValueUnit),
        choiceObservers = Set.empty,
        byKey = false,
      )
      val exerciseNodeC = builder.exercise(
        contract = createNode,
        choice = "C",
        consuming = false,
        actingParties = Set("signatory"),
        argument = Value.ValueUnit,
        result = Some(Value.ValueUnit),
        choiceObservers = Set.empty,
        byKey = false,
      )
      val exerciseNodeAId = builder.add(exerciseNodeA)
      val exerciseNodeBId = builder.add(exerciseNodeB, exerciseNodeAId)
      val exerciseNodeCId = builder.add(exerciseNodeC, exerciseNodeAId)
      val transaction = builder.buildCommitted()
      val update = state.Update.TransactionAccepted(
        optCompletionInfo = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        transactionId = Ref.TransactionId.assertFromString("TransactionId"),
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventExercise(
          consuming = false,
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.toInstant),
          command_id = Some(completionInfo.commandId),
          workflow_id = transactionMeta.workflowId,
          application_id = Some(completionInfo.applicationId),
          submitters = Some(completionInfo.actAs.toSet),
          node_index = Some(exerciseNodeAId.index),
          event_id = Some(EventId(update.transactionId, exerciseNodeAId).toLedgerString),
          contract_id = exerciseNodeA.targetCoid.coid,
          template_id = Some(exerciseNodeA.templateId.toString),
          flat_event_witnesses = Set.empty, // stakeholders
          tree_event_witnesses = Set("signatory"), // informees
          create_key_value = None,
          exercise_choice = Some(exerciseNodeA.choiceId),
          exercise_argument = Some(emptyArray),
          exercise_result = Some(emptyArray),
          exercise_actors = Some(Set("signatory")),
          exercise_child_event_ids = Some(
            Set(
              EventId(update.transactionId, exerciseNodeBId).toLedgerString,
              EventId(update.transactionId, exerciseNodeCId).toLedgerString,
            )
          ),
          create_key_value_compression = compressionAlgorithmId,
          exercise_argument_compression = compressionAlgorithmId,
          exercise_result_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.EventExercise(
          consuming = false,
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.toInstant),
          command_id = Some(completionInfo.commandId),
          workflow_id = transactionMeta.workflowId,
          application_id = Some(completionInfo.applicationId),
          submitters = Some(completionInfo.actAs.toSet),
          node_index = Some(exerciseNodeBId.index),
          event_id = Some(EventId(update.transactionId, exerciseNodeBId).toLedgerString),
          contract_id = exerciseNodeB.targetCoid.coid,
          template_id = Some(exerciseNodeB.templateId.toString),
          flat_event_witnesses = Set.empty, // stakeholders
          tree_event_witnesses = Set("signatory"), // informees
          create_key_value = None,
          exercise_choice = Some(exerciseNodeB.choiceId),
          exercise_argument = Some(emptyArray),
          exercise_result = Some(emptyArray),
          exercise_actors = Some(Set("signatory")),
          exercise_child_event_ids = Some(Set.empty),
          create_key_value_compression = compressionAlgorithmId,
          exercise_argument_compression = compressionAlgorithmId,
          exercise_result_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.EventExercise(
          consuming = false,
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.toInstant),
          command_id = Some(completionInfo.commandId),
          workflow_id = transactionMeta.workflowId,
          application_id = Some(completionInfo.applicationId),
          submitters = Some(completionInfo.actAs.toSet),
          node_index = Some(exerciseNodeCId.index),
          event_id = Some(EventId(update.transactionId, exerciseNodeCId).toLedgerString),
          contract_id = exerciseNodeC.targetCoid.coid,
          template_id = Some(exerciseNodeC.templateId.toString),
          flat_event_witnesses = Set.empty, // stakeholders
          tree_event_witnesses = Set("signatory"), // informees
          create_key_value = None,
          exercise_choice = Some(exerciseNodeC.choiceId),
          exercise_argument = Some(emptyArray),
          exercise_result = Some(emptyArray),
          exercise_actors = Some(Set("signatory")),
          exercise_child_event_ids = Some(Set.empty),
          create_key_value_compression = compressionAlgorithmId,
          exercise_argument_compression = compressionAlgorithmId,
          exercise_result_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.CommandCompletion(
          completion_offset = someOffset.toHexString,
          record_time = update.recordTime.toInstant,
          application_id = completionInfo.applicationId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          transaction_id = Some(update.transactionId),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
        ),
      )
    }

    "handle TransactionAccepted (single exercise node with divulgence)" in {
      // Previous transaction
      // └─ #1 Create
      // Transaction
      // └─ #2 Exercise (divulges #1 to 'divulgee')
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val builder = new TransactionBuilder()
      val createNode = builder.create(
        id = builder.newCid,
        template = "pkgid:M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
        key = None,
      )
      val exerciseNode = builder.exercise(
        contract = createNode,
        choice = "someChoice",
        consuming = true,
        actingParties = Set("signatory"),
        argument = Value.ValueUnit,
        result = Some(Value.ValueUnit),
        choiceObservers = Set("divulgee"),
        byKey = false,
      )
      val exerciseNodeId = builder.add(exerciseNode)
      val transaction = builder.buildCommitted()
      val update = state.Update.TransactionAccepted(
        optCompletionInfo = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        transactionId = Ref.TransactionId.assertFromString("TransactionId"),
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventExercise(
          consuming = true,
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.toInstant),
          command_id = Some(completionInfo.commandId),
          workflow_id = transactionMeta.workflowId,
          application_id = Some(completionInfo.applicationId),
          submitters = Some(completionInfo.actAs.toSet),
          node_index = Some(exerciseNodeId.index),
          event_id = Some(EventId(update.transactionId, exerciseNodeId).toLedgerString),
          contract_id = exerciseNode.targetCoid.coid,
          template_id = Some(exerciseNode.templateId.toString),
          flat_event_witnesses = Set("signatory", "observer"),
          tree_event_witnesses = Set("signatory", "observer", "divulgee"),
          create_key_value = None,
          exercise_choice = Some(exerciseNode.choiceId),
          exercise_argument = Some(emptyArray),
          exercise_result = Some(emptyArray),
          exercise_actors = Some(Set("signatory")),
          exercise_child_event_ids = Some(Set.empty),
          create_key_value_compression = compressionAlgorithmId,
          exercise_argument_compression = compressionAlgorithmId,
          exercise_result_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.EventDivulgence(
          event_offset = Some(someOffset.toHexString),
          command_id = Some(completionInfo.commandId),
          workflow_id = transactionMeta.workflowId,
          application_id = Some(completionInfo.applicationId),
          submitters = Some(completionInfo.actAs.toSet),
          contract_id = exerciseNode.targetCoid.coid,
          template_id =
            None, // No contract details stored. That's ok because the participant sees the create event.
          tree_event_witnesses = Set("divulgee"),
          create_argument =
            None, // No contract details stored. That's ok because the participant sees the create event.
          create_argument_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.CommandCompletion(
          completion_offset = someOffset.toHexString,
          record_time = update.recordTime.toInstant,
          application_id = completionInfo.applicationId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          transaction_id = Some(update.transactionId),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
        ),
      )
    }

    "handle TransactionAccepted (transaction with local divulgence)" in {
      // Transaction
      // ├─ #1 Create
      // └─ #2 Exercise (divulges #1 to 'divulgee')
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val builder = new TransactionBuilder()
      val createNode = builder.create(
        id = builder.newCid,
        template = "pkgid:M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
        key = None,
      )
      val exerciseNode = builder.exercise(
        contract = createNode,
        choice = "someChoice",
        consuming = true,
        actingParties = Set("signatory"),
        argument = Value.ValueUnit,
        result = Some(Value.ValueUnit),
        choiceObservers = Set("divulgee"),
        byKey = false,
      )
      val createNodeId = builder.add(createNode)
      val exerciseNodeId = builder.add(exerciseNode)
      val transaction = builder.buildCommitted()
      val update = state.Update.TransactionAccepted(
        optCompletionInfo = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        transactionId = Ref.TransactionId.assertFromString("TransactionId"),
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventCreate(
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.toInstant),
          command_id = Some(completionInfo.commandId),
          workflow_id = transactionMeta.workflowId,
          application_id = Some(completionInfo.applicationId),
          submitters = Some(completionInfo.actAs.toSet),
          node_index = Some(createNodeId.index),
          event_id = Some(EventId(update.transactionId, createNodeId).toLedgerString),
          contract_id = createNode.coid.coid,
          template_id = Some(createNode.coinst.template.toString),
          flat_event_witnesses = Set("signatory", "observer"),
          tree_event_witnesses = Set("signatory", "observer"),
          create_argument = Some(emptyArray),
          create_signatories = Some(Set("signatory")),
          create_observers = Some(Set("observer")),
          create_agreement_text = None,
          create_key_value = None,
          create_key_hash = None,
          create_argument_compression = compressionAlgorithmId,
          create_key_value_compression = None,
          event_sequential_id = 0,
        ),
        DbDto.EventExercise(
          consuming = true,
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.toInstant),
          command_id = Some(completionInfo.commandId),
          workflow_id = transactionMeta.workflowId,
          application_id = Some(completionInfo.applicationId),
          submitters = Some(completionInfo.actAs.toSet),
          node_index = Some(exerciseNodeId.index),
          event_id = Some(EventId(update.transactionId, exerciseNodeId).toLedgerString),
          contract_id = exerciseNode.targetCoid.coid,
          template_id = Some(exerciseNode.templateId.toString),
          flat_event_witnesses = Set("signatory", "observer"),
          tree_event_witnesses = Set("signatory", "observer", "divulgee"),
          create_key_value = None,
          exercise_choice = Some(exerciseNode.choiceId),
          exercise_argument = Some(emptyArray),
          exercise_result = Some(emptyArray),
          exercise_actors = Some(Set("signatory")),
          exercise_child_event_ids = Some(Set.empty),
          create_key_value_compression = compressionAlgorithmId,
          exercise_argument_compression = compressionAlgorithmId,
          exercise_result_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.EventDivulgence(
          event_offset = Some(someOffset.toHexString),
          command_id = Some(completionInfo.commandId),
          workflow_id = transactionMeta.workflowId,
          application_id = Some(completionInfo.applicationId),
          submitters = Some(completionInfo.actAs.toSet),
          contract_id = exerciseNode.targetCoid.coid,
          template_id =
            None, // No contract details stored. That's ok because the participant sees the create event.
          tree_event_witnesses = Set("divulgee"),
          create_argument =
            None, // No contract details stored.  That's ok because the participant sees the create event.
          create_argument_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.CommandCompletion(
          completion_offset = someOffset.toHexString,
          record_time = update.recordTime.toInstant,
          application_id = completionInfo.applicationId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          transaction_id = Some(update.transactionId),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
        ),
      )
    }

    "handle TransactionAccepted (explicit blinding info)" in {
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val builder = new TransactionBuilder()
      val createNode = builder.create(
        id = builder.newCid,
        template = "pkgid:M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
        key = None,
      )
      val exerciseNode = builder.exercise(
        contract = createNode,
        choice = "someChoice",
        consuming = true,
        actingParties = Set("signatory"),
        argument = Value.ValueUnit,
        result = Some(Value.ValueUnit),
        choiceObservers = Set.empty,
        byKey = false,
      )
      val exerciseNodeId = builder.add(exerciseNode)
      val transaction = builder.buildCommitted()
      val update = state.Update.TransactionAccepted(
        optCompletionInfo = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        transactionId = Ref.TransactionId.assertFromString("TransactionId"),
        recordTime = someRecordTime,
        divulgedContracts =
          List(state.DivulgedContract(createNode.coid, createNode.versionedCoinst)),
        blindingInfo = Some(
          BlindingInfo(
            disclosure = Map(exerciseNodeId -> Set(Ref.Party.assertFromString("disclosee"))),
            divulgence = Map(createNode.coid -> Set(Ref.Party.assertFromString("divulgee"))),
          )
        ),
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventExercise(
          consuming = true,
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.toInstant),
          command_id = Some(completionInfo.commandId),
          workflow_id = transactionMeta.workflowId,
          application_id = Some(completionInfo.applicationId),
          submitters = Some(completionInfo.actAs.toSet),
          node_index = Some(exerciseNodeId.index),
          event_id = Some(EventId(update.transactionId, exerciseNodeId).toLedgerString),
          contract_id = exerciseNode.targetCoid.coid,
          template_id = Some(exerciseNode.templateId.toString),
          flat_event_witnesses = Set("signatory", "observer"),
          tree_event_witnesses = Set("disclosee"), // taken from explicit blinding info
          create_key_value = None,
          exercise_choice = Some(exerciseNode.choiceId),
          exercise_argument = Some(emptyArray),
          exercise_result = Some(emptyArray),
          exercise_actors = Some(Set("signatory")),
          exercise_child_event_ids = Some(Set.empty),
          create_key_value_compression = compressionAlgorithmId,
          exercise_argument_compression = compressionAlgorithmId,
          exercise_result_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.EventDivulgence(
          event_offset = Some(someOffset.toHexString),
          command_id = Some(completionInfo.commandId),
          workflow_id = transactionMeta.workflowId,
          application_id = Some(completionInfo.applicationId),
          submitters = Some(completionInfo.actAs.toSet),
          contract_id = exerciseNode.targetCoid.coid,
          template_id =
            Some(createNode.templateId.toString), // taken from explicit divulgedContracts
          tree_event_witnesses = Set("divulgee"), // taken from explicit blinding info
          create_argument = Some(emptyArray), // taken from explicit divulgedContracts
          create_argument_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.CommandCompletion(
          completion_offset = someOffset.toHexString,
          record_time = update.recordTime.toInstant,
          application_id = completionInfo.applicationId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          transaction_id = Some(update.transactionId),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
        ),
      )
    }

    "handle TransactionAccepted (rollback node)" in {
      // Transaction
      // └─ #1 Rollback
      //    ├─ #2 Create
      //    └─ #3 Exercise (divulges #2 to divulgee)
      // - Create and Exercise events must not be visible
      // - Divulgence events from rolled back Exercise/Fetch nodes must be visible
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val builder = new TransactionBuilder()
      val rollbackNode = builder.rollback()
      val createNode = builder.create(
        id = builder.newCid,
        template = "pkgid:M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
        key = None,
      )
      val exerciseNode = builder.exercise(
        contract = createNode,
        choice = "someChoice",
        consuming = true,
        actingParties = Set("signatory"),
        argument = Value.ValueUnit,
        result = Some(Value.ValueUnit),
        choiceObservers = Set("divulgee"),
        byKey = false,
      )
      val rollbackNodeId = builder.add(rollbackNode)
      builder.add(createNode, rollbackNodeId)
      builder.add(exerciseNode, rollbackNodeId)
      val transaction = builder.buildCommitted()
      val update = state.Update.TransactionAccepted(
        optCompletionInfo = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        transactionId = Ref.TransactionId.assertFromString("TransactionId"),
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        // TODO append-only: Why is there a divulgence event? The divulged contract doesn't exist because it was rolled back.
        DbDto.EventDivulgence(
          event_offset = Some(someOffset.toHexString),
          command_id = Some(completionInfo.commandId),
          workflow_id = transactionMeta.workflowId,
          application_id = Some(completionInfo.applicationId),
          submitters = Some(completionInfo.actAs.toSet),
          contract_id = exerciseNode.targetCoid.coid,
          template_id = None,
          tree_event_witnesses = Set("divulgee"),
          create_argument = None,
          create_argument_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.CommandCompletion(
          completion_offset = someOffset.toHexString,
          record_time = update.recordTime.toInstant,
          application_id = completionInfo.applicationId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          transaction_id = Some(update.transactionId),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
        ),
      )
    }

    "handle TransactionAccepted (no submission info)" in {
      // Transaction that is missing a SubmitterInfo
      // This happens if a transaction was submitted through a different participant
      val transactionMeta = someTransactionMeta
      val builder = new TransactionBuilder()
      val createNode = builder.create(
        id = builder.newCid,
        template = "pkgid:M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
        key = None,
      )
      val createNodeId = builder.add(createNode)
      val transaction = builder.buildCommitted()
      val update = state.Update.TransactionAccepted(
        optCompletionInfo = None,
        transactionMeta = transactionMeta,
        transaction = transaction,
        transactionId = Ref.TransactionId.assertFromString("TransactionId"),
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventCreate(
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.toInstant),
          command_id = None,
          workflow_id = transactionMeta.workflowId,
          application_id = None,
          submitters = None,
          node_index = Some(createNodeId.index),
          event_id = Some(EventId(update.transactionId, createNodeId).toLedgerString),
          contract_id = createNode.coid.coid,
          template_id = Some(createNode.coinst.template.toString),
          flat_event_witnesses = Set("signatory", "observer"),
          tree_event_witnesses = Set("signatory", "observer"),
          create_argument = Some(emptyArray),
          create_signatories = Some(Set("signatory")),
          create_observers = Some(Set("observer")),
          create_agreement_text = None,
          create_key_value = None,
          create_key_hash = None,
          create_argument_compression = compressionAlgorithmId,
          create_key_value_compression = None,
          event_sequential_id = 0,
        )
      )
    }

  }
}

object UpdateToDbDtoSpec {
  private val emptyArray = Array.emptyByteArray

  // These tests do not check the correctness of the LF value serialization.
  // All LF values are serialized into empty arrays in this suite.
  private val valueSerialization = new LfValueSerialization {
    override def serialize(
        contractId: ContractId,
        contractArgument: Value.VersionedValue[ContractId],
    ): Array[Byte] = emptyArray

    /** Returns (contract argument, contract key) */
    override def serialize(eventId: EventId, create: Create): (Array[Byte], Option[Array[Byte]]) =
      (emptyArray, create.key.map(_ => emptyArray))

    /** Returns (choice argument, exercise result, contract key) */
    override def serialize(
        eventId: EventId,
        exercise: Exercise,
    ): (Array[Byte], Option[Array[Byte]], Option[Array[Byte]]) =
      (emptyArray, exercise.exerciseResult.map(_ => emptyArray), exercise.key.map(_ => emptyArray))
    override def deserialize[E](raw: Raw.Created[E], verbose: Boolean)(implicit
        ec: ExecutionContext,
        loggingContext: LoggingContext,
    ): Future[CreatedEvent] = Future.failed(new RuntimeException("Not implemented"))

    override def deserialize(raw: TreeEvent.Exercised, verbose: Boolean)(implicit
        ec: ExecutionContext,
        loggingContext: LoggingContext,
    ): Future[ExercisedEvent] = Future.failed(new RuntimeException("Not implemented"))
  }

  // These test do not check the correctness of compression.
  // All values are compressed using a dummy (identity) algorithm in this suite.
  private val compressionAlgorithmId = Some(123)
  private val compressionStrategy: CompressionStrategy = {
    val noCompression = new FieldCompressionStrategy(compressionAlgorithmId, x => x)
    CompressionStrategy(noCompression, noCompression, noCompression, noCompression)
  }

  private val someParticipantId =
    Ref.ParticipantId.assertFromString("UpdateToDbDtoSpecParticipant")
  private val otherParticipantId =
    Ref.ParticipantId.assertFromString("UpdateToDbDtoSpecRemoteParticipant")
  private val someOffset = Offset.fromHexString(Ref.HexString.assertFromString("abcdef"))
  private val someRecordTime = Time.Timestamp.assertFromString("2000-01-01T00:00:00.000000Z")
  private val someApplicationId =
    Ref.ApplicationId.assertFromString("UpdateToDbDtoSpecApplicationId")
  private val someCommandId = Ref.CommandId.assertFromString("UpdateToDbDtoSpecCommandId")
  private val someSubmissionId =
    Ref.SubmissionId.assertFromString("UpdateToDbDtoSpecSubmissionId")
  private val someWorkflowId = Ref.WorkflowId.assertFromString("UpdateToDbDtoSpecWorkflowId")
  private val someConfiguration =
    Configuration(1, LedgerTimeModel.reasonableDefault, Duration.ofHours(23))
  private val someParty = Ref.Party.assertFromString("UpdateToDbDtoSpecParty")
  private val someHash =
    crypto.Hash.assertFromString("01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086")
  private val someArchive1 = DamlLf.Archive.newBuilder
    .setHash("00001")
    .setHashFunction(DamlLf.HashFunction.SHA256)
    .setPayload(ByteString.copyFromUtf8("payload 1"))
    .build
  private val someArchive2 = DamlLf.Archive.newBuilder
    .setHash("00002")
    .setHashFunction(DamlLf.HashFunction.SHA256)
    .setPayload(ByteString.copyFromUtf8("payload 2 (longer than the other payload)"))
    .build
  private val someCompletionInfo = state.CompletionInfo(
    actAs = List(someParty),
    applicationId = someApplicationId,
    commandId = someCommandId,
    optDeduplicationPeriod = None,
    submissionId = someSubmissionId,
  )
  private val someTransactionMeta = state.TransactionMeta(
    ledgerEffectiveTime = Time.Timestamp.assertFromLong(2),
    workflowId = Some(someWorkflowId),
    submissionTime = Time.Timestamp.assertFromLong(3),
    submissionSeed = someHash,
    optUsedPackages = None,
    optNodeSeeds = None,
    optByKeyNodes = None,
  )

  // DbDto case classes contain serialized values in Arrays (sometimes wrapped in Options),
  // because this representation can efficiently be passed to Jdbc.
  // Using Arrays means DbDto instances are not comparable, so we have to define a custom equality operator.
  private implicit val DbDtoEq: org.scalactic.Equality[DbDto] = {
    case (a: DbDto, b: DbDto) =>
      (a.productPrefix === b.productPrefix) &&
        (a.productArity == b.productArity) &&
        (a.productIterator zip b.productIterator).forall {
          case (x: Array[_], y: Array[_]) => x sameElements y
          case (Some(x: Array[_]), Some(y: Array[_])) => x sameElements y
          case (x, y) => x === y
        }
    case (_, _) => false
  }
}
