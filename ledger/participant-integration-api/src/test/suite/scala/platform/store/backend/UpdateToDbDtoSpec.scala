// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.DeduplicationPeriod.{DeduplicationDuration, DeduplicationOffset}
import com.daml.ledger.api.v1.event.{CreatedEvent, ExercisedEvent}
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.crypto
import com.daml.lf.data.{Bytes, Ref, Time}
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.BlindingInfo
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.platform.{ContractId, Create, Exercise}
import com.daml.platform.index.index.StatusDetails
import com.daml.platform.store.dao.events.Raw.TreeEvent
import com.daml.platform.store.dao.{EventProjectionProperties, JdbcLedgerDao}
import com.daml.platform.store.dao.events.{
  CompressionStrategy,
  FieldCompressionStrategy,
  LfValueSerialization,
  Raw,
}
import com.google.protobuf.ByteString
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}

// Note: this suite contains hand-crafted updates that are impossible to produce on some ledgers
// (e.g., because the ledger removes rollback nodes before sending them to the index database).
// Should you ever consider replacing this suite by something else, make sure all functionality is still covered.
class UpdateToDbDtoSpec extends AnyWordSpec with Matchers {

  import TransactionBuilder.Implicits._
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
          recorded_at = update.recordTime.micros,
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
          recorded_at = someRecordTime.micros,
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
          recorded_at = someRecordTime.micros,
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
          recorded_at = someRecordTime.micros,
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
          recorded_at = someRecordTime.micros,
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
          known_since = someRecordTime.micros,
          ledger_offset = someOffset.toHexString,
          _package = someArchive1.toByteArray,
        ),
        DbDto.Package(
          package_id = someArchive2.getHash,
          upload_id = someSubmissionId,
          source_description = Some(sourceDescription),
          package_size = someArchive2.getPayload.size.toLong,
          known_since = someRecordTime.micros,
          ledger_offset = someOffset.toHexString,
          _package = someArchive2.toByteArray,
        ),
        DbDto.PackageEntry(
          ledger_offset = someOffset.toHexString,
          recorded_at = someRecordTime.micros,
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
          recorded_at = someRecordTime.micros,
          submission_id = Some(someSubmissionId),
          typ = JdbcLedgerDao.rejectType,
          rejection_reason = Some(rejectionReason),
        )
      )
    }

    "handle CommandRejected" in {
      val status = StatusProto.of(Status.Code.ABORTED.value(), "test reason", Seq.empty)
      val completionInfo = someCompletionInfo
      val update = state.Update.CommandRejected(
        someRecordTime,
        completionInfo,
        state.Update.CommandRejected.FinalReason(status),
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.CommandCompletion(
          completion_offset = someOffset.toHexString,
          record_time = someRecordTime.micros,
          application_id = someApplicationId,
          submitters = Set(someParty),
          command_id = someCommandId,
          transaction_id = None,
          rejection_status_code = Some(status.code),
          rejection_status_message = Some(status.message),
          rejection_status_details = Some(StatusDetails.of(status.details).toByteArray),
          submission_id = Some(someSubmissionId),
          deduplication_offset = None,
          deduplication_duration_seconds = None,
          deduplication_duration_nanos = None,
          deduplication_start = None,
        )
      )
    }

    val transactionId = Ref.TransactionId.assertFromString("TransactionId")

    "handle TransactionAccepted (single create node)" in {
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val builder = TransactionBuilder()
      val contractId = builder.newCid
      val createNode = builder
        .create(
          id = contractId,
          templateId = "M:T",
          argument = Value.ValueUnit,
          signatories = Set("signatory"),
          observers = Set("observer"),
          key = None,
        )
        .copy(agreementText = "agreement text")
      val createNodeId = builder.add(createNode)
      val transaction = builder.buildCommitted()
      val update = state.Update.TransactionAccepted(
        optCompletionInfo = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        transactionId = transactionId,
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
        contractMetadata = Map(contractId -> someContractDriverMetadata),
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos.head shouldEqual DbDto.EventCreate(
        event_offset = Some(someOffset.toHexString),
        transaction_id = Some(update.transactionId),
        ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
        command_id = Some(completionInfo.commandId),
        workflow_id = transactionMeta.workflowId,
        application_id = Some(completionInfo.applicationId),
        submitters = Some(completionInfo.actAs.toSet),
        node_index = Some(createNodeId.index),
        event_id = Some(EventId(update.transactionId, createNodeId).toLedgerString),
        contract_id = createNode.coid.coid,
        template_id = Some(createNode.templateId.toString),
        flat_event_witnesses = Set("signatory", "observer"), // stakeholders
        tree_event_witnesses = Set("signatory", "observer"), // informees
        create_argument = Some(emptyArray),
        create_signatories = Some(Set("signatory")),
        create_observers = Some(Set("observer")),
        create_agreement_text = Some(createNode.agreementText),
        create_key_value = None,
        create_key_hash = None,
        create_argument_compression = compressionAlgorithmId,
        create_key_value_compression = None,
        event_sequential_id = 0,
        driver_metadata = Some(someContractDriverMetadata.toByteArray),
      )
      dtos(3) shouldEqual DbDto.CommandCompletion(
        completion_offset = someOffset.toHexString,
        record_time = update.recordTime.micros,
        application_id = completionInfo.applicationId,
        submitters = completionInfo.actAs.toSet,
        command_id = completionInfo.commandId,
        transaction_id = Some(update.transactionId),
        rejection_status_code = None,
        rejection_status_message = None,
        rejection_status_details = None,
        submission_id = completionInfo.submissionId,
        deduplication_offset = None,
        deduplication_duration_nanos = None,
        deduplication_duration_seconds = None,
        deduplication_start = None,
      )
      dtos(4) shouldEqual DbDto.TransactionMeta(
        transaction_id = transactionId,
        event_offset = someOffset.toHexString,
        event_sequential_id_first = 0,
        event_sequential_id_last = 0,
      )
      Set(dtos(1), dtos(2)) should contain theSameElementsAs Set(
        DbDto.FilterCreateStakeholder(0L, createNode.templateId.toString, "signatory"),
        DbDto.FilterCreateStakeholder(0L, createNode.templateId.toString, "observer"),
      )
      dtos.size shouldEqual 5
    }

    "handle TransactionAccepted (single consuming exercise node)" in {
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val builder = TransactionBuilder()
      val exerciseNode = {
        val createNode = builder.create(
          id = builder.newCid,
          templateId = "M:T",
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
        transactionId = transactionId,
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
        contractMetadata = Map.empty,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventExercise(
          consuming = true,
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
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
          exercise_child_event_ids = Some(Vector.empty),
          create_key_value_compression = compressionAlgorithmId,
          exercise_argument_compression = compressionAlgorithmId,
          exercise_result_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.FilterConsumingStakeholder(
          event_sequential_id = 0,
          template_id = exerciseNode.templateId.toString,
          party_id = "signatory",
        ),
        DbDto.FilterConsumingStakeholder(
          event_sequential_id = 0,
          template_id = exerciseNode.templateId.toString,
          party_id = "observer",
        ),
        DbDto.CommandCompletion(
          completion_offset = someOffset.toHexString,
          record_time = update.recordTime.micros,
          application_id = completionInfo.applicationId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          transaction_id = Some(update.transactionId),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
          submission_id = completionInfo.submissionId,
          deduplication_offset = None,
          deduplication_duration_nanos = None,
          deduplication_duration_seconds = None,
          deduplication_start = None,
        ),
        DbDto.TransactionMeta(
          transaction_id = transactionId,
          event_offset = someOffset.toHexString,
          event_sequential_id_first = 0,
          event_sequential_id_last = 0,
        ),
      )
    }

    "handle TransactionAccepted (single non-consuming exercise node)" in {
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val builder = TransactionBuilder()
      val exerciseNode = {
        val createNode = builder.create(
          id = builder.newCid,
          templateId = "M:T",
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
        transactionId = transactionId,
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
        contractMetadata = Map.empty,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventExercise(
          consuming = false,
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
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
          exercise_child_event_ids = Some(Vector.empty),
          create_key_value_compression = compressionAlgorithmId,
          exercise_argument_compression = compressionAlgorithmId,
          exercise_result_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.FilterNonConsumingInformee(
          event_sequential_id = 0,
          party_id = "signatory",
        ),
        DbDto.CommandCompletion(
          completion_offset = someOffset.toHexString,
          record_time = update.recordTime.micros,
          application_id = completionInfo.applicationId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          transaction_id = Some(update.transactionId),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
          submission_id = completionInfo.submissionId,
          deduplication_offset = None,
          deduplication_duration_nanos = None,
          deduplication_duration_seconds = None,
          deduplication_start = None,
        ),
        DbDto.TransactionMeta(
          transaction_id = transactionId,
          event_offset = someOffset.toHexString,
          event_sequential_id_first = 0,
          event_sequential_id_last = 0,
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
      val builder = TransactionBuilder()
      val createNode = builder.create(
        id = builder.newCid,
        templateId = "M:T",
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
        transactionId = transactionId,
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
        contractMetadata = Map.empty,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventExercise(
          consuming = false,
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
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
            Vector(
              EventId(update.transactionId, exerciseNodeBId).toLedgerString,
              EventId(update.transactionId, exerciseNodeCId).toLedgerString,
            )
          ),
          create_key_value_compression = compressionAlgorithmId,
          exercise_argument_compression = compressionAlgorithmId,
          exercise_result_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.FilterNonConsumingInformee(
          event_sequential_id = 0,
          party_id = "signatory",
        ),
        DbDto.EventExercise(
          consuming = false,
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
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
          exercise_child_event_ids = Some(Vector.empty),
          create_key_value_compression = compressionAlgorithmId,
          exercise_argument_compression = compressionAlgorithmId,
          exercise_result_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.FilterNonConsumingInformee(
          event_sequential_id = 0,
          party_id = "signatory",
        ),
        DbDto.EventExercise(
          consuming = false,
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
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
          exercise_child_event_ids = Some(Vector.empty),
          create_key_value_compression = compressionAlgorithmId,
          exercise_argument_compression = compressionAlgorithmId,
          exercise_result_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.FilterNonConsumingInformee(
          event_sequential_id = 0,
          party_id = "signatory",
        ),
        DbDto.CommandCompletion(
          completion_offset = someOffset.toHexString,
          record_time = update.recordTime.micros,
          application_id = completionInfo.applicationId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          transaction_id = Some(update.transactionId),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
          submission_id = completionInfo.submissionId,
          deduplication_offset = None,
          deduplication_duration_nanos = None,
          deduplication_duration_seconds = None,
          deduplication_start = None,
        ),
        DbDto.TransactionMeta(
          transaction_id = transactionId,
          event_offset = someOffset.toHexString,
          event_sequential_id_first = 0,
          event_sequential_id_last = 0,
        ),
      )
    }

    "handle TransactionAccepted (fetch and lookup nodes)" in {
      // Previous transaction
      // └─ #1 Create
      // Transaction
      // ├─ #1 Fetch
      // ├─ #2 Fetch by key
      // └─ #3 Lookup by key
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val builder = TransactionBuilder()
      val createNode = builder.create(
        id = builder.newCid,
        templateId = "M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
        key = Some(Value.ValueUnit),
      )
      val fetchNode = builder.fetch(
        contract = createNode,
        byKey = false,
      )
      val fetchByKeyNode = builder.fetch(
        contract = createNode,
        byKey = true,
      )
      val lookupByKeyNode = builder.lookupByKey(
        contract = createNode,
        found = true,
      )
      builder.add(fetchNode)
      builder.add(fetchByKeyNode)
      builder.add(lookupByKeyNode)
      val transaction = builder.buildCommitted()
      val update = state.Update.TransactionAccepted(
        optCompletionInfo = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        transactionId = transactionId,
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
        contractMetadata = Map.empty,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      // Note: fetch and lookup nodes are not indexed
      dtos should contain theSameElementsInOrderAs List(
        DbDto.CommandCompletion(
          completion_offset = someOffset.toHexString,
          record_time = update.recordTime.micros,
          application_id = completionInfo.applicationId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          transaction_id = Some(update.transactionId),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
          submission_id = completionInfo.submissionId,
          deduplication_offset = None,
          deduplication_duration_nanos = None,
          deduplication_duration_seconds = None,
          deduplication_start = None,
        ),
        DbDto.TransactionMeta(
          transaction_id = transactionId,
          event_offset = someOffset.toHexString,
          event_sequential_id_first = 0,
          event_sequential_id_last = 0,
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
      val builder = TransactionBuilder()
      val createNode = builder.create(
        id = builder.newCid,
        templateId = "M:T",
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
        transactionId = transactionId,
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
        contractMetadata = Map.empty,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventExercise(
          consuming = true,
          event_offset = Some(someOffset.toHexString),
          transaction_id = Some(update.transactionId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
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
          exercise_child_event_ids = Some(Vector.empty),
          create_key_value_compression = compressionAlgorithmId,
          exercise_argument_compression = compressionAlgorithmId,
          exercise_result_compression = compressionAlgorithmId,
          event_sequential_id = 0,
        ),
        DbDto.FilterConsumingStakeholder(
          event_sequential_id = 0,
          template_id = exerciseNode.templateId.toString,
          party_id = "signatory",
        ),
        DbDto.FilterConsumingStakeholder(
          event_sequential_id = 0,
          template_id = exerciseNode.templateId.toString,
          party_id = "observer",
        ),
        DbDto.FilterConsumingNonStakeholderInformee(
          event_sequential_id = 0,
          party_id = "divulgee",
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
          record_time = update.recordTime.micros,
          application_id = completionInfo.applicationId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          transaction_id = Some(update.transactionId),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
          submission_id = completionInfo.submissionId,
          deduplication_offset = None,
          deduplication_duration_nanos = None,
          deduplication_duration_seconds = None,
          deduplication_start = None,
        ),
        DbDto.TransactionMeta(
          transaction_id = transactionId,
          event_offset = someOffset.toHexString,
          event_sequential_id_first = 0,
          event_sequential_id_last = 0,
        ),
      )
    }

    "handle TransactionAccepted (transaction with local divulgence)" in {
      // Transaction
      // ├─ #1 Create
      // └─ #2 Exercise (divulges #1 to 'divulgee')
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val builder = TransactionBuilder()
      val contractId = builder.newCid
      val createNode = builder.create(
        id = contractId,
        templateId = "M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
        key = None,
      )
      // TODO pbatko: reproduce in Daml Script
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
        transactionId = transactionId,
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
        contractMetadata = Map(contractId -> someContractDriverMetadata),
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos.head shouldEqual DbDto.EventCreate(
        event_offset = Some(someOffset.toHexString),
        transaction_id = Some(update.transactionId),
        ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
        command_id = Some(completionInfo.commandId),
        workflow_id = transactionMeta.workflowId,
        application_id = Some(completionInfo.applicationId),
        submitters = Some(completionInfo.actAs.toSet),
        node_index = Some(createNodeId.index),
        event_id = Some(EventId(update.transactionId, createNodeId).toLedgerString),
        contract_id = createNode.coid.coid,
        template_id = Some(createNode.templateId.toString),
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
        driver_metadata = Some(someContractDriverMetadata.toByteArray),
      )
      Set(dtos(1), dtos(2)) should contain theSameElementsAs Set(
        DbDto.FilterCreateStakeholder(0L, createNode.templateId.toString, "signatory"),
        DbDto.FilterCreateStakeholder(0L, createNode.templateId.toString, "observer"),
      )
      dtos(3) shouldEqual DbDto.EventExercise(
        consuming = true,
        event_offset = Some(someOffset.toHexString),
        transaction_id = Some(update.transactionId),
        ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
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
        exercise_child_event_ids = Some(Vector.empty),
        create_key_value_compression = compressionAlgorithmId,
        exercise_argument_compression = compressionAlgorithmId,
        exercise_result_compression = compressionAlgorithmId,
        event_sequential_id = 0,
      )
      dtos(4) shouldEqual DbDto.FilterConsumingStakeholder(
        event_sequential_id = 0,
        template_id = exerciseNode.templateId.toString,
        party_id = "signatory",
      )
      dtos(5) shouldEqual DbDto.FilterConsumingStakeholder(
        event_sequential_id = 0,
        template_id = exerciseNode.templateId.toString,
        party_id = "observer",
      )
      dtos(6) shouldEqual DbDto.FilterConsumingNonStakeholderInformee(
        event_sequential_id = 0,
        party_id = "divulgee",
      )
      dtos(7) shouldEqual DbDto.EventDivulgence(
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
      )
      dtos(8) shouldEqual DbDto.CommandCompletion(
        completion_offset = someOffset.toHexString,
        record_time = update.recordTime.micros,
        application_id = completionInfo.applicationId,
        submitters = completionInfo.actAs.toSet,
        command_id = completionInfo.commandId,
        transaction_id = Some(update.transactionId),
        rejection_status_code = None,
        rejection_status_message = None,
        rejection_status_details = None,
        submission_id = completionInfo.submissionId,
        deduplication_offset = None,
        deduplication_duration_nanos = None,
        deduplication_duration_seconds = None,
        deduplication_start = None,
      )
      dtos(9) shouldEqual DbDto.TransactionMeta(
        transaction_id = transactionId,
        event_offset = someOffset.toHexString,
        event_sequential_id_first = 0,
        event_sequential_id_last = 0,
      )
      dtos.size shouldEqual 10
    }

    "handle TransactionAccepted (explicit blinding info)" in {
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val builder = TransactionBuilder()
      val createNode = builder.create(
        id = builder.newCid,
        templateId = "M:T",
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
        transactionId = transactionId,
        recordTime = someRecordTime,
        divulgedContracts =
          List(state.DivulgedContract(createNode.coid, createNode.versionedCoinst)),
        blindingInfo = Some(
          BlindingInfo(
            disclosure = Map(exerciseNodeId -> Set(Ref.Party.assertFromString("disclosee"))),
            divulgence = Map(createNode.coid -> Set(Ref.Party.assertFromString("divulgee"))),
          )
        ),
        contractMetadata = Map.empty,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos(0) shouldEqual DbDto.EventExercise(
        consuming = true,
        event_offset = Some(someOffset.toHexString),
        transaction_id = Some(update.transactionId),
        ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
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
        exercise_child_event_ids = Some(Vector.empty),
        create_key_value_compression = compressionAlgorithmId,
        exercise_argument_compression = compressionAlgorithmId,
        exercise_result_compression = compressionAlgorithmId,
        event_sequential_id = 0,
      )
      dtos(1) shouldEqual DbDto.FilterConsumingStakeholder(
        event_sequential_id = 0,
        template_id = exerciseNode.templateId.toString,
        party_id = "signatory",
      )
      dtos(2) shouldEqual DbDto.FilterConsumingStakeholder(
        event_sequential_id = 0,
        template_id = exerciseNode.templateId.toString,
        party_id = "observer",
      )
      dtos(3) shouldEqual DbDto.FilterConsumingNonStakeholderInformee(
        event_sequential_id = 0,
        party_id = "disclosee",
      )
      dtos(4) shouldEqual DbDto.EventDivulgence(
        event_offset = Some(someOffset.toHexString),
        command_id = Some(completionInfo.commandId),
        workflow_id = transactionMeta.workflowId,
        application_id = Some(completionInfo.applicationId),
        submitters = Some(completionInfo.actAs.toSet),
        contract_id = exerciseNode.targetCoid.coid,
        template_id = Some(createNode.templateId.toString), // taken from explicit divulgedContracts
        tree_event_witnesses = Set("divulgee"), // taken from explicit blinding info
        create_argument = Some(emptyArray), // taken from explicit divulgedContracts
        create_argument_compression = compressionAlgorithmId,
        event_sequential_id = 0,
      )
      dtos(5) shouldEqual DbDto.CommandCompletion(
        completion_offset = someOffset.toHexString,
        record_time = update.recordTime.micros,
        application_id = completionInfo.applicationId,
        submitters = completionInfo.actAs.toSet,
        command_id = completionInfo.commandId,
        transaction_id = Some(update.transactionId),
        rejection_status_code = None,
        rejection_status_message = None,
        rejection_status_details = None,
        submission_id = completionInfo.submissionId,
        deduplication_offset = None,
        deduplication_duration_nanos = None,
        deduplication_duration_seconds = None,
        deduplication_start = None,
      )
      dtos(6) shouldEqual DbDto.TransactionMeta(
        transaction_id = transactionId,
        event_offset = someOffset.toHexString,
        event_sequential_id_first = 0,
        event_sequential_id_last = 0,
      )
      dtos should have length 7
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
      val builder = TransactionBuilder()
      val rollbackNode = builder.rollback()
      val createNode = builder.create(
        id = builder.newCid,
        templateId = "M:T",
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
        transactionId = transactionId,
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
        contractMetadata = Map.empty,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos should contain theSameElementsInOrderAs List(
        // Note: this divulgence event references a contract that was never created. This is correct:
        // the divulgee only sees the exercise node under the rollback node, it doesn't know that the contract creation
        // was rolled back (similar to how divulgees may not learn that a contract divulged to them was archived).
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
          record_time = update.recordTime.micros,
          application_id = completionInfo.applicationId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          transaction_id = Some(update.transactionId),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
          submission_id = completionInfo.submissionId,
          deduplication_offset = None,
          deduplication_duration_nanos = None,
          deduplication_duration_seconds = None,
          deduplication_start = None,
        ),
        DbDto.TransactionMeta(
          transaction_id = transactionId,
          event_offset = someOffset.toHexString,
          event_sequential_id_first = 0,
          event_sequential_id_last = 0,
        ),
      )
    }

    "handle TransactionAccepted (no submission info)" in {
      // Transaction that is missing a SubmitterInfo
      // This happens if a transaction was submitted through a different participant
      val transactionMeta = someTransactionMeta
      val builder = TransactionBuilder()
      val contractId = builder.newCid
      val createNode = builder.create(
        id = contractId,
        templateId = "M:T",
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
        contractMetadata = Map(contractId -> someContractDriverMetadata),
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos.head shouldEqual DbDto.EventCreate(
        event_offset = Some(someOffset.toHexString),
        transaction_id = Some(update.transactionId),
        ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
        command_id = None,
        workflow_id = transactionMeta.workflowId,
        application_id = None,
        submitters = None,
        node_index = Some(createNodeId.index),
        event_id = Some(EventId(update.transactionId, createNodeId).toLedgerString),
        contract_id = createNode.coid.coid,
        template_id = Some(createNode.templateId.toString),
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
        driver_metadata = Some(someContractDriverMetadata.toByteArray),
      )
      Set(dtos(1), dtos(2)) should contain theSameElementsAs Set(
        DbDto.CreateFilter(0L, createNode.templateId.toString, "signatory"),
        DbDto.CreateFilter(0L, createNode.templateId.toString, "observer"),
      )
      dtos.size shouldEqual 3
    }

    "handle TransactionAccepted (no contract metadata)" in {
      // Transaction that is missing the contract metadata
      // This can happen if the submitting participant is running an older version
      // predating the introduction of the contract driver metadata
      val transactionMeta = someTransactionMeta
      val builder = TransactionBuilder()
      val contractId = builder.newCid
      val createNode = builder.create(
        id = contractId,
        templateId = "M:T",
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
        transactionId = transactionId,
        recordTime = someRecordTime,
        divulgedContracts = List.empty,
        blindingInfo = None,
        contractMetadata = Map.empty,
      )
      val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
        someOffset
      )(update).toList

      dtos.head shouldEqual DbDto.EventCreate(
        event_offset = Some(someOffset.toHexString),
        transaction_id = Some(update.transactionId),
        ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
        command_id = None,
        workflow_id = transactionMeta.workflowId,
        application_id = None,
        submitters = None,
        node_index = Some(createNodeId.index),
        event_id = Some(EventId(update.transactionId, createNodeId).toLedgerString),
        contract_id = createNode.coid.coid,
        template_id = Some(createNode.templateId.toString),
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
        driver_metadata = None,
      )
      dtos(3) shouldEqual DbDto.TransactionMeta(
        transaction_id = transactionId,
        event_offset = someOffset.toHexString,
        event_sequential_id_first = 0,
        event_sequential_id_last = 0,
      )
      Set(dtos(1), dtos(2)) should contain theSameElementsAs Set(
        DbDto.FilterCreateStakeholder(0L, createNode.templateId.toString, "signatory"),
        DbDto.FilterCreateStakeholder(0L, createNode.templateId.toString, "observer"),
      )
      dtos.size shouldEqual 4
    }

    val deduplicationPeriods = Table(
      (
        "Deduplication period",
        "Expected deduplication offset",
        "Expected deduplication duration seconds",
        "Expected deduplication duration nanos",
      ),
      (None, None, None, None),
      (
        Some(DeduplicationOffset(Offset.beforeBegin)),
        Some(Offset.beforeBegin.toHexString),
        None,
        None,
      ),
      (
        Some(DeduplicationDuration(Duration.ofDays(1L).plusNanos(100 * 1000))),
        None,
        Some(Duration.ofDays(1L).toMinutes * 60L),
        Some(100 * 1000),
      ),
    )

    "handle CommandRejected (all deduplication data)" in {
      val status = StatusProto.of(Status.Code.ABORTED.value(), "test reason", Seq.empty)
      forAll(deduplicationPeriods) {
        case (
              deduplicationPeriod,
              expectedDeduplicationOffset,
              expectedDeduplicationDurationSeconds,
              expectedDeduplicationDurationNanos,
            ) =>
          val completionInfo = someCompletionInfo.copy(optDeduplicationPeriod = deduplicationPeriod)
          val update = state.Update.CommandRejected(
            someRecordTime,
            completionInfo,
            state.Update.CommandRejected.FinalReason(status),
          )
          val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
            someOffset
          )(update).toList

          dtos should contain theSameElementsInOrderAs List(
            DbDto.CommandCompletion(
              completion_offset = someOffset.toHexString,
              record_time = someRecordTime.micros,
              application_id = someApplicationId,
              submitters = Set(someParty),
              command_id = someCommandId,
              transaction_id = None,
              rejection_status_code = Some(status.code),
              rejection_status_message = Some(status.message),
              rejection_status_details = Some(StatusDetails.of(status.details).toByteArray),
              submission_id = Some(someSubmissionId),
              deduplication_offset = expectedDeduplicationOffset,
              deduplication_duration_seconds = expectedDeduplicationDurationSeconds,
              deduplication_duration_nanos = expectedDeduplicationDurationNanos,
              deduplication_start = None,
            )
          )
      }
    }

    "handle TransactionAccepted (all deduplication data)" in {
      val transactionMeta = someTransactionMeta
      val builder = TransactionBuilder()
      val contractId = builder.newCid
      val createNode = builder.create(
        id = contractId,
        templateId = "M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
        key = None,
      )
      val createNodeId = builder.add(createNode)
      val transaction = builder.buildCommitted()

      forAll(deduplicationPeriods) {
        case (
              deduplicationPeriod,
              expectedDeduplicationOffset,
              expectedDeduplicationDurationSeconds,
              expectedDeduplicationDurationNanos,
            ) =>
          val completionInfo = someCompletionInfo.copy(optDeduplicationPeriod = deduplicationPeriod)
          val update = state.Update.TransactionAccepted(
            optCompletionInfo = Some(completionInfo),
            transactionMeta = transactionMeta,
            transaction = transaction,
            transactionId = transactionId,
            recordTime = someRecordTime,
            divulgedContracts = List.empty,
            blindingInfo = None,
            contractMetadata = Map(contractId -> someContractDriverMetadata),
          )
          val dtos = UpdateToDbDto(someParticipantId, valueSerialization, compressionStrategy)(
            someOffset
          )(update).toList

          dtos.head shouldEqual DbDto.EventCreate(
            event_offset = Some(someOffset.toHexString),
            transaction_id = Some(update.transactionId),
            ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
            command_id = Some(completionInfo.commandId),
            workflow_id = transactionMeta.workflowId,
            application_id = Some(completionInfo.applicationId),
            submitters = Some(completionInfo.actAs.toSet),
            node_index = Some(createNodeId.index),
            event_id = Some(EventId(update.transactionId, createNodeId).toLedgerString),
            contract_id = createNode.coid.coid,
            template_id = Some(createNode.templateId.toString),
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
            driver_metadata = Some(someContractDriverMetadata.toByteArray),
          )
          Set(dtos(1), dtos(2)) should contain theSameElementsAs Set(
            DbDto.FilterCreateStakeholder(0L, createNode.templateId.toString, "signatory"),
            DbDto.FilterCreateStakeholder(0L, createNode.templateId.toString, "observer"),
          )
          dtos(3) shouldEqual DbDto.CommandCompletion(
            completion_offset = someOffset.toHexString,
            record_time = update.recordTime.micros,
            application_id = completionInfo.applicationId,
            submitters = completionInfo.actAs.toSet,
            command_id = completionInfo.commandId,
            transaction_id = Some(update.transactionId),
            rejection_status_code = None,
            rejection_status_message = None,
            rejection_status_details = None,
            submission_id = Some(someSubmissionId),
            deduplication_offset = expectedDeduplicationOffset,
            deduplication_duration_seconds = expectedDeduplicationDurationSeconds,
            deduplication_duration_nanos = expectedDeduplicationDurationNanos,
            deduplication_start = None,
          )
          dtos(4) shouldEqual DbDto.TransactionMeta(
            transaction_id = transactionId,
            event_offset = someOffset.toHexString,
            event_sequential_id_first = 0,
            event_sequential_id_last = 0,
          )
          dtos.size shouldEqual 5
      }
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
        contractArgument: Value.VersionedValue,
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
    override def deserialize[E](
        raw: Raw.Created[E],
        eventProjectionProperties: EventProjectionProperties,
    )(implicit
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
    submissionId = Some(someSubmissionId),
    statistics = None,
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
  private val someContractDriverMetadata = Bytes.assertFromString("00abcd")

  implicit private val DbDtoEqual: org.scalactic.Equality[DbDto] = DbDtoEq.DbDtoEq
}
