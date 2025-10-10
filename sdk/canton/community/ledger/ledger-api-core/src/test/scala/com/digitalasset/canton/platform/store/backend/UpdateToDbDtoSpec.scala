// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.metrics.api.MetricsContext
import com.daml.platform.v1.index.StatusDetails
import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.data.DeduplicationPeriod.{DeduplicationDuration, DeduplicationOffset}
import com.digitalasset.canton.data.{CantonTimestamp, LedgerTimeBoundaries, Offset}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  ChangedTo,
  Revoked,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.*
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationEvent,
  TopologyEvent,
}
import com.digitalasset.canton.ledger.participant.state.Update.TransactionAccepted.RepresentativePackageIds
import com.digitalasset.canton.ledger.participant.state.{
  Reassignment,
  ReassignmentInfo,
  TestAcsChangeFactory,
  Update,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.Conversions.{
  authorizationEventInt,
  participantPermissionInt,
}
import com.digitalasset.canton.platform.store.backend.DbDto.IdFilter
import com.digitalasset.canton.platform.store.backend.StorageBackendTestValues.someExternalTransactionHash
import com.digitalasset.canton.platform.store.backend.UpdateToDbDto.templateIdWithPackageName
import com.digitalasset.canton.platform.store.dao.JdbcLedgerDao
import com.digitalasset.canton.platform.store.dao.events.{
  CompressionStrategy,
  FieldCompressionStrategy,
  LfValueSerialization,
}
import com.digitalasset.canton.platform.{ContractId, Create, Exercise}
import com.digitalasset.canton.protocol.{ReassignmentId, TestUpdateId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.SerializableTraceContextExtension
import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.emptyTraceContext
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.transaction.GlobalKey
import com.digitalasset.daml.lf.transaction.test.TestNodeBuilder.CreateKey
import com.digitalasset.daml.lf.transaction.test.{
  NodeIdTransactionBuilder,
  TestNodeBuilder,
  TransactionBuilder,
}
import com.digitalasset.daml.lf.value.Value
import com.google.rpc.status.Status as StatusProto
import io.grpc.Status
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.*
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Duration, Instant}
import java.util.UUID

// Note: this suite contains hand-crafted updates that are impossible to produce on some ledgers
// (e.g., because the ledger removes rollback nodes before sending them to the index database).
// Should you ever consider replacing this suite by something else, make sure all functionality is still covered.
class UpdateToDbDtoSpec extends AnyWordSpec with Matchers {

  import TraceContext.Implicits.Empty.*
  import TransactionBuilder.Implicits.*
  import UpdateToDbDtoSpec.*

  object TxBuilder {
    def apply(): NodeIdTransactionBuilder & TestNodeBuilder = new NodeIdTransactionBuilder
      with TestNodeBuilder
  }

  "UpdateToDbDto" should {

    "handle PartyAddedToParticipant (local party)" in {
      val update = state.Update.PartyAddedToParticipant(
        someParty,
        someParticipantId,
        someRecordTime,
        Some(someSubmissionId),
      )
      val dtos = updateToDtos(update)

      dtos should contain theSameElementsInOrderAs List(
        DbDto.PartyEntry(
          ledger_offset = someOffset.unwrap,
          recorded_at = someRecordTime.toMicros,
          submission_id = Some(someSubmissionId),
          party = Some(someParty),
          typ = JdbcLedgerDao.acceptType,
          rejection_reason = None,
          is_local = Some(true),
        )
      )
    }

    "handle PartyAddedToParticipant (remote party)" in {
      val update = state.Update.PartyAddedToParticipant(
        someParty,
        otherParticipantId,
        someRecordTime,
        None,
      )
      val dtos = updateToDtos(update)

      dtos should contain theSameElementsInOrderAs List(
        DbDto.PartyEntry(
          ledger_offset = someOffset.unwrap,
          recorded_at = someRecordTime.toMicros,
          submission_id = None,
          party = Some(someParty),
          typ = JdbcLedgerDao.acceptType,
          rejection_reason = None,
          is_local = Some(false),
        )
      )
    }

    "handle CommandRejected (sequenced rejection)" in {
      val status = StatusProto.of(Status.Code.ABORTED.value(), "test reason", Seq.empty)
      val completionInfo = someCompletionInfo
      val update = state.Update.SequencedCommandRejected(
        completionInfo,
        state.Update.CommandRejected.FinalReason(status),
        someSynchronizerId1,
        CantonTimestamp.ofEpochMicro(1234567),
      )
      val dtos = updateToDtos(update)

      dtos should contain theSameElementsInOrderAs List(
        DbDto.CommandCompletion(
          completion_offset = someOffset.unwrap,
          record_time = 1234567L,
          publication_time = 0,
          user_id = someUserId,
          submitters = Set(someParty),
          command_id = someCommandId,
          update_id = None,
          rejection_status_code = Some(status.code),
          rejection_status_message = Some(status.message),
          rejection_status_details = Some(StatusDetails.of(status.details).toByteArray),
          submission_id = Some(someSubmissionId),
          deduplication_offset = None,
          deduplication_duration_seconds = None,
          deduplication_duration_nanos = None,
          synchronizer_id = someSynchronizerId1,
          message_uuid = None,
          is_transaction = true,
          trace_context = serializedEmptyTraceContext,
        )
      )
    }

    "handle CommandRejected (local rejection)" in {
      val status = StatusProto.of(Status.Code.ABORTED.value(), "test reason", Seq.empty)
      val messageUuid = UUID.randomUUID()
      val completionInfo = someCompletionInfo
      val update = state.Update.UnSequencedCommandRejected(
        completionInfo,
        state.Update.CommandRejected.FinalReason(status),
        someSynchronizerId1,
        someRecordTime,
        messageUuid,
      )
      val dtos = updateToDtos(update)

      dtos should contain theSameElementsInOrderAs List(
        DbDto.CommandCompletion(
          completion_offset = someOffset.unwrap,
          record_time = someRecordTime.toMicros,
          publication_time = 0,
          user_id = someUserId,
          submitters = Set(someParty),
          command_id = someCommandId,
          update_id = None,
          rejection_status_code = Some(status.code),
          rejection_status_message = Some(status.message),
          rejection_status_details = Some(StatusDetails.of(status.details).toByteArray),
          submission_id = Some(someSubmissionId),
          deduplication_offset = None,
          deduplication_duration_seconds = None,
          deduplication_duration_nanos = None,
          synchronizer_id = someSynchronizerId1,
          message_uuid = Some(messageUuid.toString),
          is_transaction = true,
          trace_context = serializedEmptyTraceContext,
        )
      )
    }

    val updateId = TestUpdateId("mock_hash")
    val updateIdByteArray = updateId.toProtoPrimitive.toByteArray

    // We only care about distinguishing between repair and sequencer transactions for create nodes
    // since for create nodes the representative package-id assignment policies are different between the two
    def handleAcsDeltaTransactionAcceptedWithSingleCreateNode(
        isAcsDelta: Boolean,
        isRepairTransaction: Boolean,
    ): Unit = {
      assert(
        isRepairTransaction && isAcsDelta || !isRepairTransaction,
        "Repair transaction is implicitly an ACS delta",
      )
      val updateName =
        if (isRepairTransaction) classOf[state.Update.RepairTransactionAccepted].getSimpleName
        else classOf[state.Update.SequencedTransactionAccepted].getSimpleName
      s"handle $updateName (single create node, isAcsDelta = $isAcsDelta)" in {
        val completionInfo = someCompletionInfo
        val transactionMeta = someTransactionMeta
        val externalTransactionHash = someExternalTransactionHash
        val builder = TxBuilder()
        val contractId = builder.newCid
        val internalContractIds = Map(contractId -> 42L)
        val contractTemplate = Ref.Identifier.assertFromString("P:M:T")
        val keyValue = Value.ValueUnit
        val createNode = builder
          .create(
            id = contractId,
            templateId = contractTemplate,
            argument = Value.ValueUnit,
            signatories = Set("signatory1", "signatory2", "signatory3"),
            observers = Set("observer"),
            key = CreateKey.KeyWithMaintainers(keyValue, Set("signatory2", "signatory3")),
          )
        val createNodeId = builder.add(createNode)
        val transaction = builder.buildCommitted()
        val update =
          if (isRepairTransaction)
            state.Update.RepairTransactionAccepted(
              transactionMeta = transactionMeta,
              transaction = transaction,
              updateId = updateId,
              contractAuthenticationData = Map(contractId -> someContractAuthenticationData),
              representativePackageIds = RepresentativePackageIds.DedicatedRepresentativePackageIds(
                Map(contractId -> someRepresentativePackageId)
              ),
              synchronizerId = someSynchronizerId1,
              recordTime = someRecordTime,
              repairCounter = RepairCounter(1337),
              internalContractIds = internalContractIds,
            )
          else
            state.Update.SequencedTransactionAccepted(
              completionInfoO = Some(completionInfo),
              transactionMeta = transactionMeta,
              transaction = transaction,
              updateId = updateId,
              contractAuthenticationData = Map(contractId -> someContractAuthenticationData),
              synchronizerId = someSynchronizerId1,
              recordTime = someRecordTime,
              externalTransactionHash = Some(externalTransactionHash),
              acsChangeFactory = TestAcsChangeFactory(contractActivenessChanged = isAcsDelta),
              internalContractIds = internalContractIds,
            )
        val dtos = updateToDtos(update)

        val dtoCreate = DbDto.EventActivate(
          event_offset = someOffset.unwrap,
          update_id = updateIdByteArray,
          workflow_id = transactionMeta.workflowId,
          command_id = Option.when(!isRepairTransaction)(completionInfo.commandId),
          submitters = Option.when(!isRepairTransaction)(completionInfo.actAs.toSet),
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
          trace_context = serializedEmptyTraceContext,
          external_transaction_hash =
            Option.when(!isRepairTransaction)(externalTransactionHash.unwrap.toByteArray),
          event_type = PersistentEventType.Create.asInt,
          event_sequential_id = 0,
          node_id = createNodeId.index,
          additional_witnesses = Some(
            if (isAcsDelta) Set.empty
            else Set("signatory1", "signatory2", "signatory3", "observer")
          ),
          source_synchronizer_id = None,
          reassignment_counter = None,
          reassignment_id = None,
          representative_package_id =
            if (isRepairTransaction) someRepresentativePackageId
            else createNode.templateId.packageId,
          notPersistedContractId = createNode.coid,
          internal_contract_id = 42L,
          create_key_hash = Some(
            GlobalKey
              .assertBuild(contractTemplate, keyValue, createNode.packageName)
              .hash
              .bytes
              .toHexString
          ),
        )
        val dtoCompletion = DbDto.CommandCompletion(
          completion_offset = someOffset.unwrap,
          record_time = someRecordTime.toMicros,
          publication_time = 0,
          user_id = completionInfo.userId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          update_id = Some(updateIdByteArray),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
          submission_id = completionInfo.submissionId,
          deduplication_offset = None,
          deduplication_duration_nanos = None,
          deduplication_duration_seconds = None,
          synchronizer_id = someSynchronizerId1,
          message_uuid = None,
          is_transaction = true,
          trace_context = serializedEmptyTraceContext,
        )
        val dtoTransactionMeta = DbDto.TransactionMeta(
          update_id = updateIdByteArray,
          event_offset = someOffset.unwrap,
          publication_time = 0,
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
          event_sequential_id_first = 0,
          event_sequential_id_last = 0,
        )

        dtos.head shouldEqual dtoCreate
        if (!isRepairTransaction) {
          dtos(5) shouldEqual dtoCompletion
          dtos(6) shouldEqual dtoTransactionMeta
        } else {
          dtos(5) shouldEqual dtoTransactionMeta
        }
        Set(dtos(1), dtos(2), dtos(3), dtos(4)) should contain theSameElementsAs
          (if (isAcsDelta)
             Set(
               DbDto.IdFilterActivateStakeholder(
                 IdFilter(
                   0L,
                   templateIdWithPackageName(createNode),
                   "signatory1",
                   first_per_sequential_id = true,
                 )
               ),
               DbDto.IdFilterActivateStakeholder(
                 IdFilter(
                   0L,
                   templateIdWithPackageName(createNode),
                   "signatory2",
                   first_per_sequential_id = false,
                 )
               ),
               DbDto.IdFilterActivateStakeholder(
                 IdFilter(
                   0L,
                   templateIdWithPackageName(createNode),
                   "signatory3",
                   first_per_sequential_id = false,
                 )
               ),
               DbDto
                 .IdFilterActivateStakeholder(
                   IdFilter(
                     0L,
                     templateIdWithPackageName(createNode),
                     "observer",
                     first_per_sequential_id = false,
                   )
                 ),
             )
           else
             Set(
               DbDto.IdFilterActivateWitness(
                 IdFilter(
                   0L,
                   templateIdWithPackageName(createNode),
                   "signatory1",
                   first_per_sequential_id = true,
                 )
               ),
               DbDto.IdFilterActivateWitness(
                 IdFilter(
                   0L,
                   templateIdWithPackageName(createNode),
                   "signatory2",
                   first_per_sequential_id = false,
                 )
               ),
               DbDto.IdFilterActivateWitness(
                 IdFilter(
                   0L,
                   templateIdWithPackageName(createNode),
                   "signatory3",
                   first_per_sequential_id = false,
                 )
               ),
               DbDto.IdFilterActivateWitness(
                 IdFilter(
                   0L,
                   templateIdWithPackageName(createNode),
                   "observer",
                   first_per_sequential_id = false,
                 )
               ),
             ))

        if (isRepairTransaction)
          dtos.size shouldEqual 6
        else
          dtos.size shouldEqual 7
      }
    }

    handleAcsDeltaTransactionAcceptedWithSingleCreateNode(
      isAcsDelta = true,
      isRepairTransaction = false,
    )
    handleAcsDeltaTransactionAcceptedWithSingleCreateNode(
      isAcsDelta = true,
      isRepairTransaction = true,
    )

    "handle SequencedTransactionAccepted (single create node, isAcsDelta = false)" in {
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val externalTransactionHash = someExternalTransactionHash
      val builder = TxBuilder()
      val contractId = builder.newCid
      val contractTemplate = Ref.Identifier.assertFromString("P:M:T")
      val keyValue = Value.ValueUnit
      val createNode = builder
        .create(
          id = contractId,
          templateId = contractTemplate,
          argument = Value.ValueUnit,
          signatories = Set("signatory1", "signatory2", "signatory3"),
          observers = Set("observer"),
          key = CreateKey.KeyWithMaintainers(keyValue, Set("signatory2", "signatory3")),
        )
      val createNodeId = builder.add(createNode)
      val transaction = builder.buildCommitted()
      val update =
        state.Update.SequencedTransactionAccepted(
          completionInfoO = Some(completionInfo),
          transactionMeta = transactionMeta,
          transaction = transaction,
          updateId = updateId,
          contractAuthenticationData = Map(contractId -> someContractAuthenticationData),
          synchronizerId = someSynchronizerId1,
          recordTime = someRecordTime,
          externalTransactionHash = Some(externalTransactionHash),
          acsChangeFactory = TestAcsChangeFactory(false),
          internalContractIds = Map(contractId -> 42L),
        )
      val dtos = updateToDtos(update)

      val dtoCreate = DbDto.EventVariousWitnessed(
        event_offset = someOffset.unwrap,
        update_id = updateIdByteArray,
        workflow_id = transactionMeta.workflowId,
        command_id = Some(completionInfo.commandId),
        submitters = Some(completionInfo.actAs.toSet),
        record_time = someRecordTime.toMicros,
        synchronizer_id = someSynchronizerId1,
        trace_context = serializedEmptyTraceContext,
        external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
        event_type = PersistentEventType.WitnessedCreate.asInt,
        event_sequential_id = 0,
        node_id = createNodeId.index,
        additional_witnesses = Set("signatory1", "signatory2", "signatory3", "observer"),
        consuming = None,
        exercise_choice = None,
        exercise_choice_interface_id = None,
        exercise_argument = None,
        exercise_result = None,
        exercise_actors = None,
        exercise_last_descendant_node_id = None,
        exercise_argument_compression = None,
        exercise_result_compression = None,
        representative_package_id = Some(createNode.templateId.packageId),
        contract_id = None,
        internal_contract_id = Some(42L),
        template_id = None,
        package_id = None,
        ledger_effective_time = None,
      )
      val dtoCompletion = DbDto.CommandCompletion(
        completion_offset = someOffset.unwrap,
        record_time = someRecordTime.toMicros,
        publication_time = 0,
        user_id = completionInfo.userId,
        submitters = completionInfo.actAs.toSet,
        command_id = completionInfo.commandId,
        update_id = Some(updateIdByteArray),
        rejection_status_code = None,
        rejection_status_message = None,
        rejection_status_details = None,
        submission_id = completionInfo.submissionId,
        deduplication_offset = None,
        deduplication_duration_nanos = None,
        deduplication_duration_seconds = None,
        synchronizer_id = someSynchronizerId1,
        message_uuid = None,
        is_transaction = true,
        trace_context = serializedEmptyTraceContext,
      )
      val dtoTransactionMeta = DbDto.TransactionMeta(
        update_id = updateIdByteArray,
        event_offset = someOffset.unwrap,
        publication_time = 0,
        record_time = someRecordTime.toMicros,
        synchronizer_id = someSynchronizerId1,
        event_sequential_id_first = 0,
        event_sequential_id_last = 0,
      )

      dtos.head shouldEqual dtoCreate
      dtos(5) shouldEqual dtoCompletion
      dtos(6) shouldEqual dtoTransactionMeta
      Set(dtos(1), dtos(2), dtos(3), dtos(4)) should contain theSameElementsAs
        Set(
          DbDto.IdFilterVariousWitness(
            IdFilter(
              0L,
              templateIdWithPackageName(createNode),
              "signatory1",
              first_per_sequential_id = true,
            )
          ),
          DbDto.IdFilterVariousWitness(
            IdFilter(
              0L,
              templateIdWithPackageName(createNode),
              "signatory2",
              first_per_sequential_id = false,
            )
          ),
          DbDto.IdFilterVariousWitness(
            IdFilter(
              0L,
              templateIdWithPackageName(createNode),
              "signatory3",
              first_per_sequential_id = false,
            )
          ),
          DbDto.IdFilterVariousWitness(
            IdFilter(
              0L,
              templateIdWithPackageName(createNode),
              "observer",
              first_per_sequential_id = false,
            )
          ),
        )

      dtos.size shouldEqual 7
    }

    s"handle TransactionAccepted (single consuming exercise node, isAcsDelta = true)" in {
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val externalTransactionHash = someExternalTransactionHash
      val builder = TxBuilder()
      val exerciseNode = {
        val createNode = builder.create(
          id = builder.newCid,
          templateId = "M:T",
          argument = Value.ValueUnit,
          signatories = List("signatory"),
          observers = List("observer"),
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
      val update = state.Update.SequencedTransactionAccepted(
        completionInfoO = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        updateId = updateId,
        contractAuthenticationData = Map.empty,
        synchronizerId = someSynchronizerId1,
        recordTime = CantonTimestamp.ofEpochMicro(120),
        externalTransactionHash = Some(externalTransactionHash),
        acsChangeFactory = TestAcsChangeFactory(true),
        internalContractIds = Map.empty,
      )
      val dtos = updateToDtos(update)

      dtos.head shouldEqual
        DbDto.EventDeactivate(
          event_offset = someOffset.unwrap,
          update_id = updateIdByteArray,
          workflow_id = transactionMeta.workflowId,
          command_id = Some(completionInfo.commandId),
          submitters = Some(completionInfo.actAs.toSet),
          record_time = 120,
          synchronizer_id = someSynchronizerId1,
          trace_context = serializedEmptyTraceContext,
          external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
          event_type = PersistentEventType.ConsumingExercise.asInt,
          event_sequential_id = 0,
          node_id = exerciseNodeId.index,
          deactivated_event_sequential_id = None,
          additional_witnesses = Some(Set.empty),
          exercise_choice = exerciseNode.choiceId,
          exercise_choice_interface_id = None,
          exercise_argument =
            Some(compressArrayWith(compressionAlgorithmIdConsumingArg, emptyArray)),
          exercise_result = Some(compressArrayWith(compressionAlgorithmIdConsumingRes, emptyArray)),
          exercise_actors = Some(Set("signatory")),
          exercise_last_descendant_node_id = Some(exerciseNodeId.index),
          exercise_argument_compression = compressionAlgorithmIdConsumingArg,
          exercise_result_compression = compressionAlgorithmIdConsumingRes,
          reassignment_id = None,
          assignment_exclusivity = None,
          target_synchronizer_id = None,
          reassignment_counter = None,
          contract_id = exerciseNode.targetCoid,
          internal_contract_id = None,
          template_id = templateIdWithPackageName(exerciseNode),
          package_id = exerciseNode.templateId.packageId,
          stakeholders = Set("signatory", "observer"),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
        )
      dtos(3) shouldEqual
        DbDto.CommandCompletion(
          completion_offset = someOffset.unwrap,
          record_time = 120,
          publication_time = 0,
          user_id = completionInfo.userId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          update_id = Some(updateIdByteArray),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
          submission_id = completionInfo.submissionId,
          deduplication_offset = None,
          deduplication_duration_nanos = None,
          deduplication_duration_seconds = None,
          synchronizer_id = someSynchronizerId1,
          message_uuid = None,
          is_transaction = true,
          trace_context = serializedEmptyTraceContext,
        )
      dtos(4) shouldEqual
        DbDto.TransactionMeta(
          update_id = updateIdByteArray,
          event_offset = someOffset.unwrap,
          publication_time = 0,
          record_time = 120,
          synchronizer_id = someSynchronizerId1,
          event_sequential_id_first = 0,
          event_sequential_id_last = 0,
        )

      Set(dtos(1), dtos(2)) should contain theSameElementsAs
        Set(
          DbDto.IdFilterDeactivateStakeholder(
            IdFilter(
              event_sequential_id = 0,
              template_id = templateIdWithPackageName(exerciseNode),
              party_id = "signatory",
              first_per_sequential_id = true,
            )
          ),
          DbDto.IdFilterDeactivateStakeholder(
            IdFilter(
              event_sequential_id = 0,
              template_id = templateIdWithPackageName(exerciseNode),
              party_id = "observer",
              first_per_sequential_id = false,
            )
          ),
        )

      dtos.size shouldEqual 5
    }

    s"handle TransactionAccepted (single consuming exercise node, isAcsDelta = false)" in {
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val externalTransactionHash = someExternalTransactionHash
      val builder = TxBuilder()
      val exerciseNode = {
        val createNode = builder.create(
          id = builder.newCid,
          templateId = "M:T",
          argument = Value.ValueUnit,
          signatories = List("signatory"),
          observers = List("observer"),
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
      val update = state.Update.SequencedTransactionAccepted(
        completionInfoO = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        updateId = updateId,
        contractAuthenticationData = Map.empty,
        synchronizerId = someSynchronizerId1,
        recordTime = CantonTimestamp.ofEpochMicro(120),
        externalTransactionHash = Some(externalTransactionHash),
        acsChangeFactory = TestAcsChangeFactory(false),
        internalContractIds = Map.empty,
      )
      val dtos = updateToDtos(update)

      dtos.head shouldEqual
        DbDto.EventVariousWitnessed(
          event_offset = someOffset.unwrap,
          update_id = updateIdByteArray,
          workflow_id = transactionMeta.workflowId,
          command_id = Some(completionInfo.commandId),
          submitters = Some(completionInfo.actAs.toSet),
          record_time = 120,
          synchronizer_id = someSynchronizerId1,
          trace_context = serializedEmptyTraceContext,
          external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
          event_type = PersistentEventType.WitnessedConsumingExercise.asInt,
          event_sequential_id = 0,
          node_id = exerciseNodeId.index,
          additional_witnesses = Set("signatory", "observer"),
          consuming = Some(true),
          exercise_choice = exerciseNode.choiceId,
          exercise_choice_interface_id = None,
          exercise_argument =
            Some(compressArrayWith(compressionAlgorithmIdConsumingArg, emptyArray)),
          exercise_result = Some(compressArrayWith(compressionAlgorithmIdConsumingRes, emptyArray)),
          exercise_actors = Some(Set("signatory")),
          exercise_last_descendant_node_id = Some(exerciseNodeId.index),
          exercise_argument_compression = compressionAlgorithmIdConsumingArg,
          exercise_result_compression = compressionAlgorithmIdConsumingRes,
          representative_package_id = None,
          contract_id = Some(exerciseNode.targetCoid),
          internal_contract_id = None,
          template_id = Some(templateIdWithPackageName(exerciseNode)),
          package_id = Some(exerciseNode.templateId.packageId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
        )
      dtos(3) shouldEqual
        DbDto.CommandCompletion(
          completion_offset = someOffset.unwrap,
          record_time = 120,
          publication_time = 0,
          user_id = completionInfo.userId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          update_id = Some(updateIdByteArray),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
          submission_id = completionInfo.submissionId,
          deduplication_offset = None,
          deduplication_duration_nanos = None,
          deduplication_duration_seconds = None,
          synchronizer_id = someSynchronizerId1,
          message_uuid = None,
          is_transaction = true,
          trace_context = serializedEmptyTraceContext,
        )
      dtos(4) shouldEqual
        DbDto.TransactionMeta(
          update_id = updateIdByteArray,
          event_offset = someOffset.unwrap,
          publication_time = 0,
          record_time = 120,
          synchronizer_id = someSynchronizerId1,
          event_sequential_id_first = 0,
          event_sequential_id_last = 0,
        )

      Set(dtos(1), dtos(2)) should contain theSameElementsAs
        Set(
          DbDto.IdFilterVariousWitness(
            IdFilter(
              event_sequential_id = 0,
              template_id = templateIdWithPackageName(exerciseNode),
              party_id = "signatory",
              first_per_sequential_id = true,
            )
          ),
          DbDto.IdFilterVariousWitness(
            IdFilter(
              event_sequential_id = 0,
              template_id = templateIdWithPackageName(exerciseNode),
              party_id = "observer",
              first_per_sequential_id = false,
            )
          ),
        )

      dtos.size shouldEqual 5
    }

    "handle TransactionAccepted (single non-consuming exercise node)" in {
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val externalTransactionHash = someExternalTransactionHash
      val builder = TxBuilder()
      val exerciseNode = {
        val createNode = builder.create(
          id = builder.newCid,
          templateId = "M:T",
          argument = Value.ValueUnit,
          signatories = List("signatory"),
          observers = List("observer"),
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
      val update = state.Update.SequencedTransactionAccepted(
        completionInfoO = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        updateId = updateId,
        contractAuthenticationData = Map.empty,
        synchronizerId = someSynchronizerId1,
        recordTime = someRecordTime,
        externalTransactionHash = Some(externalTransactionHash),
        acsChangeFactory = TestAcsChangeFactory(),
        internalContractIds = Map.empty,
      )
      val dtos = updateToDtos(update)

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventVariousWitnessed(
          event_offset = someOffset.unwrap,
          update_id = updateIdByteArray,
          workflow_id = transactionMeta.workflowId,
          command_id = Some(completionInfo.commandId),
          submitters = Some(completionInfo.actAs.toSet),
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
          trace_context = serializedEmptyTraceContext,
          external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
          event_type = PersistentEventType.NonConsumingExercise.asInt,
          event_sequential_id = 0,
          node_id = exerciseNodeId.index,
          additional_witnesses = Set("signatory"),
          consuming = Some(false),
          exercise_choice = exerciseNode.choiceId,
          exercise_choice_interface_id = None,
          exercise_argument =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingArg, emptyArray)),
          exercise_result =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingRes, emptyArray)),
          exercise_actors = Some(Set("signatory")),
          exercise_last_descendant_node_id = Some(exerciseNodeId.index),
          exercise_argument_compression = compressionAlgorithmIdNonConsumingArg,
          exercise_result_compression = compressionAlgorithmIdNonConsumingRes,
          representative_package_id = None,
          contract_id = Some(exerciseNode.targetCoid),
          internal_contract_id = None,
          template_id = Some(templateIdWithPackageName(exerciseNode)),
          package_id = Some(exerciseNode.templateId.packageId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
        ),
        DbDto.IdFilterVariousWitness(
          IdFilter(
            event_sequential_id = 0,
            template_id = templateIdWithPackageName(exerciseNode),
            party_id = "signatory",
            first_per_sequential_id = true,
          )
        ),
        DbDto.CommandCompletion(
          completion_offset = someOffset.unwrap,
          record_time = someRecordTime.toMicros,
          publication_time = 0,
          user_id = completionInfo.userId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          update_id = Some(updateIdByteArray),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
          submission_id = completionInfo.submissionId,
          deduplication_offset = None,
          deduplication_duration_nanos = None,
          deduplication_duration_seconds = None,
          synchronizer_id = someSynchronizerId1,
          message_uuid = None,
          is_transaction = true,
          trace_context = serializedEmptyTraceContext,
        ),
        DbDto.TransactionMeta(
          update_id = updateIdByteArray,
          event_offset = someOffset.unwrap,
          publication_time = 0,
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
          event_sequential_id_first = 0,
          event_sequential_id_last = 0,
        ),
      )
    }

    "handle TransactionAccepted (create node divulged)" in {
      // Previous transaction
      // └─ #1 Create
      // Transaction
      // └─ #2 Exercise (choice A)
      //    ├─ #3 Exercise (choice B)
      //    └─ #4 Create (C)
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val externalTransactionHash = someExternalTransactionHash
      val builder = TxBuilder()
      val createNode = builder.create(
        id = builder.newCid,
        templateId = "M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
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
      val createNodeC = builder.create(
        id = builder.newCid,
        templateId = "M:T2",
        argument = Value.ValueUnit,
        signatories = List("signatory2"),
        observers = Set.empty,
      )
      val exerciseNodeAId = builder.add(exerciseNodeA)
      val exerciseNodeBId = builder.add(exerciseNodeB, exerciseNodeAId)
      val createNodeCId = builder.add(createNodeC, exerciseNodeAId)
      val transaction = builder.buildCommitted()
      val update = state.Update.SequencedTransactionAccepted(
        completionInfoO = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        updateId = updateId,
        contractAuthenticationData = Map.empty,
        synchronizerId = someSynchronizerId1,
        recordTime = someRecordTime,
        externalTransactionHash = Some(externalTransactionHash),
        acsChangeFactory = TestAcsChangeFactory(false),
        internalContractIds = Map(createNodeC.coid -> 42L),
      )
      val dtos = updateToDtos(update)

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventVariousWitnessed(
          event_offset = someOffset.unwrap,
          update_id = updateIdByteArray,
          workflow_id = transactionMeta.workflowId,
          command_id = Some(completionInfo.commandId),
          submitters = Some(completionInfo.actAs.toSet),
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
          trace_context = serializedEmptyTraceContext,
          external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
          event_type = PersistentEventType.NonConsumingExercise.asInt,
          event_sequential_id = 0,
          node_id = exerciseNodeAId.index,
          additional_witnesses = Set("signatory"),
          consuming = Some(false),
          exercise_choice = exerciseNodeA.choiceId,
          exercise_choice_interface_id = None,
          exercise_argument =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingArg, emptyArray)),
          exercise_result =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingRes, emptyArray)),
          exercise_actors = Some(Set("signatory")),
          exercise_last_descendant_node_id = Some(createNodeCId.index),
          exercise_argument_compression = compressionAlgorithmIdNonConsumingArg,
          exercise_result_compression = compressionAlgorithmIdNonConsumingRes,
          representative_package_id = None,
          contract_id = Some(exerciseNodeA.targetCoid),
          internal_contract_id = None,
          template_id = Some(templateIdWithPackageName(exerciseNodeA)),
          package_id = Some(exerciseNodeA.templateId.packageId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
        ),
        DbDto.IdFilterVariousWitness(
          IdFilter(
            event_sequential_id = 0,
            template_id = templateIdWithPackageName(exerciseNodeA),
            party_id = "signatory",
            first_per_sequential_id = true,
          )
        ),
        DbDto.EventVariousWitnessed(
          event_offset = someOffset.unwrap,
          update_id = updateIdByteArray,
          workflow_id = transactionMeta.workflowId,
          command_id = Some(completionInfo.commandId),
          submitters = Some(completionInfo.actAs.toSet),
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
          trace_context = serializedEmptyTraceContext,
          external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
          event_type = PersistentEventType.NonConsumingExercise.asInt,
          event_sequential_id = 0,
          node_id = exerciseNodeBId.index,
          additional_witnesses = Set("signatory"),
          consuming = Some(false),
          exercise_choice = exerciseNodeB.choiceId,
          exercise_choice_interface_id = None,
          exercise_argument =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingArg, emptyArray)),
          exercise_result =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingRes, emptyArray)),
          exercise_actors = Some(Set("signatory")),
          exercise_last_descendant_node_id = Some(exerciseNodeBId.index),
          exercise_argument_compression = compressionAlgorithmIdNonConsumingArg,
          exercise_result_compression = compressionAlgorithmIdNonConsumingRes,
          representative_package_id = None,
          contract_id = Some(exerciseNodeB.targetCoid),
          internal_contract_id = None,
          template_id = Some(templateIdWithPackageName(exerciseNodeB)),
          package_id = Some(exerciseNodeB.templateId.packageId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
        ),
        DbDto.IdFilterVariousWitness(
          IdFilter(
            event_sequential_id = 0,
            template_id = templateIdWithPackageName(exerciseNodeB),
            party_id = "signatory",
            first_per_sequential_id = true,
          )
        ),
        DbDto.EventVariousWitnessed(
          event_offset = someOffset.unwrap,
          update_id = updateIdByteArray,
          workflow_id = transactionMeta.workflowId,
          command_id = Some(completionInfo.commandId),
          submitters = Some(completionInfo.actAs.toSet),
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
          trace_context = serializedEmptyTraceContext,
          external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
          event_type = PersistentEventType.WitnessedCreate.asInt,
          event_sequential_id = 0,
          node_id = createNodeCId.index,
          additional_witnesses = Set("signatory", "signatory2"),
          consuming = None,
          exercise_choice = None,
          exercise_choice_interface_id = None,
          exercise_argument = None,
          exercise_result = None,
          exercise_actors = None,
          exercise_last_descendant_node_id = None,
          exercise_argument_compression = None,
          exercise_result_compression = None,
          representative_package_id = Some(createNode.templateId.packageId),
          contract_id = None,
          internal_contract_id = Some(42L),
          template_id = None,
          package_id = None,
          ledger_effective_time = None,
        ),
        DbDto.IdFilterVariousWitness(
          IdFilter(
            event_sequential_id = 0,
            template_id = templateIdWithPackageName(createNodeC),
            party_id = "signatory",
            first_per_sequential_id = true,
          )
        ),
        DbDto.IdFilterVariousWitness(
          IdFilter(
            event_sequential_id = 0,
            template_id = templateIdWithPackageName(createNodeC),
            party_id = "signatory2",
            first_per_sequential_id = false,
          )
        ),
        DbDto.CommandCompletion(
          completion_offset = someOffset.unwrap,
          record_time = someRecordTime.toMicros,
          publication_time = 0,
          user_id = completionInfo.userId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          update_id = Some(updateIdByteArray),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
          submission_id = completionInfo.submissionId,
          deduplication_offset = None,
          deduplication_duration_nanos = None,
          deduplication_duration_seconds = None,
          synchronizer_id = someSynchronizerId1,
          message_uuid = None,
          is_transaction = true,
          trace_context = serializedEmptyTraceContext,
        ),
        DbDto.TransactionMeta(
          update_id = updateIdByteArray,
          event_offset = someOffset.unwrap,
          publication_time = 0,
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
          event_sequential_id_first = 0,
          event_sequential_id_last = 0,
        ),
      )
    }

    "handle TransactionAccepted (nested create node, ACSDelta = true)" in {
      // Previous transaction
      // └─ #1 Create
      // Transaction
      // └─ #2 Exercise (choice A)
      //    ├─ #3 Exercise (choice B)
      //    └─ #4 Create (C)
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val externalTransactionHash = someExternalTransactionHash
      val builder = TxBuilder()
      val createNode = builder.create(
        id = builder.newCid,
        templateId = "M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
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
      val createNodeC = builder.create(
        id = builder.newCid,
        templateId = "M:T2",
        argument = Value.ValueUnit,
        signatories = List("signatory2"),
        observers = Set.empty,
        key = CreateKey.KeyWithMaintainers(Value.ValueUnit, Set("signatory2")),
      )
      val exerciseNodeAId = builder.add(exerciseNodeA)
      val exerciseNodeBId = builder.add(exerciseNodeB, exerciseNodeAId)
      val createNodeCId = builder.add(createNodeC, exerciseNodeAId)
      val transaction = builder.buildCommitted()
      val update = state.Update.SequencedTransactionAccepted(
        completionInfoO = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        updateId = updateId,
        contractAuthenticationData = Map.empty,
        synchronizerId = someSynchronizerId1,
        recordTime = someRecordTime,
        externalTransactionHash = Some(externalTransactionHash),
        acsChangeFactory = TestAcsChangeFactory(true),
        internalContractIds = Map(createNodeC.coid -> 42L),
      )
      val dtos = updateToDtos(update)

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventVariousWitnessed(
          event_offset = someOffset.unwrap,
          update_id = updateIdByteArray,
          workflow_id = transactionMeta.workflowId,
          command_id = Some(completionInfo.commandId),
          submitters = Some(completionInfo.actAs.toSet),
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
          trace_context = serializedEmptyTraceContext,
          external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
          event_type = PersistentEventType.NonConsumingExercise.asInt,
          event_sequential_id = 0,
          node_id = exerciseNodeAId.index,
          additional_witnesses = Set("signatory"),
          consuming = Some(false),
          exercise_choice = exerciseNodeA.choiceId,
          exercise_choice_interface_id = None,
          exercise_argument =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingArg, emptyArray)),
          exercise_result =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingRes, emptyArray)),
          exercise_actors = Some(Set("signatory")),
          exercise_last_descendant_node_id = Some(createNodeCId.index),
          exercise_argument_compression = compressionAlgorithmIdNonConsumingArg,
          exercise_result_compression = compressionAlgorithmIdNonConsumingRes,
          representative_package_id = None,
          contract_id = Some(exerciseNodeA.targetCoid),
          internal_contract_id = None,
          template_id = Some(templateIdWithPackageName(exerciseNodeA)),
          package_id = Some(exerciseNodeA.templateId.packageId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
        ),
        DbDto.IdFilterVariousWitness(
          IdFilter(
            event_sequential_id = 0,
            template_id = templateIdWithPackageName(exerciseNodeA),
            party_id = "signatory",
            first_per_sequential_id = true,
          )
        ),
        DbDto.EventVariousWitnessed(
          event_offset = someOffset.unwrap,
          update_id = updateIdByteArray,
          workflow_id = transactionMeta.workflowId,
          command_id = Some(completionInfo.commandId),
          submitters = Some(completionInfo.actAs.toSet),
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
          trace_context = serializedEmptyTraceContext,
          external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
          event_type = PersistentEventType.NonConsumingExercise.asInt,
          event_sequential_id = 0,
          node_id = exerciseNodeBId.index,
          additional_witnesses = Set("signatory"),
          consuming = Some(false),
          exercise_choice = exerciseNodeB.choiceId,
          exercise_choice_interface_id = None,
          exercise_argument =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingArg, emptyArray)),
          exercise_result =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingRes, emptyArray)),
          exercise_actors = Some(Set("signatory")),
          exercise_last_descendant_node_id = Some(exerciseNodeBId.index),
          exercise_argument_compression = compressionAlgorithmIdNonConsumingArg,
          exercise_result_compression = compressionAlgorithmIdNonConsumingRes,
          representative_package_id = None,
          contract_id = Some(exerciseNodeB.targetCoid),
          internal_contract_id = None,
          template_id = Some(templateIdWithPackageName(exerciseNodeB)),
          package_id = Some(exerciseNodeB.templateId.packageId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
        ),
        DbDto.IdFilterVariousWitness(
          IdFilter(
            event_sequential_id = 0,
            template_id = templateIdWithPackageName(exerciseNodeB),
            party_id = "signatory",
            first_per_sequential_id = true,
          )
        ),
        DbDto.EventActivate(
          event_offset = someOffset.unwrap,
          update_id = updateIdByteArray,
          workflow_id = transactionMeta.workflowId,
          command_id = Some(completionInfo.commandId),
          submitters = Some(completionInfo.actAs.toSet),
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
          trace_context = serializedEmptyTraceContext,
          external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
          event_type = PersistentEventType.Create.asInt,
          event_sequential_id = 0,
          node_id = createNodeCId.index,
          additional_witnesses = Some(Set("signatory")),
          source_synchronizer_id = None,
          reassignment_counter = None,
          reassignment_id = None,
          representative_package_id = createNodeC.templateId.packageId,
          notPersistedContractId = createNodeC.coid,
          internal_contract_id = 42L,
          create_key_hash = Some(
            GlobalKey
              .assertBuild(
                Ref.Identifier.assertFromString("P:M:T2"),
                Value.ValueUnit,
                createNodeC.packageName,
              )
              .hash
              .bytes
              .toHexString
          ),
        ),
        DbDto.IdFilterActivateStakeholder(
          IdFilter(
            event_sequential_id = 0,
            template_id = templateIdWithPackageName(createNodeC),
            party_id = "signatory2",
            first_per_sequential_id = true,
          )
        ),
        DbDto.IdFilterActivateWitness(
          IdFilter(
            event_sequential_id = 0,
            template_id = templateIdWithPackageName(createNodeC),
            party_id = "signatory",
            first_per_sequential_id = true,
          )
        ),
        DbDto.CommandCompletion(
          completion_offset = someOffset.unwrap,
          record_time = someRecordTime.toMicros,
          publication_time = 0,
          user_id = completionInfo.userId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          update_id = Some(updateIdByteArray),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
          submission_id = completionInfo.submissionId,
          deduplication_offset = None,
          deduplication_duration_nanos = None,
          deduplication_duration_seconds = None,
          synchronizer_id = someSynchronizerId1,
          message_uuid = None,
          is_transaction = true,
          trace_context = serializedEmptyTraceContext,
        ),
        DbDto.TransactionMeta(
          update_id = updateIdByteArray,
          event_offset = someOffset.unwrap,
          publication_time = 0,
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
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
      //       └─ #5 Exercise (choice D)
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val externalTransactionHash = someExternalTransactionHash
      val builder = TxBuilder()
      val createNode = builder.create(
        id = builder.newCid,
        templateId = "M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
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
      val exerciseNodeD = builder.exercise(
        contract = createNode,
        choice = "D",
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
      val exerciseNodeDId = builder.add(exerciseNodeD, exerciseNodeCId)
      val transaction = builder.buildCommitted()
      val update = state.Update.SequencedTransactionAccepted(
        completionInfoO = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        updateId = updateId,
        contractAuthenticationData = Map.empty,
        synchronizerId = someSynchronizerId1,
        recordTime = someRecordTime,
        externalTransactionHash = Some(externalTransactionHash),
        acsChangeFactory = TestAcsChangeFactory(),
        internalContractIds = Map.empty,
      )
      val dtos = updateToDtos(update)

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventVariousWitnessed(
          event_offset = someOffset.unwrap,
          update_id = updateIdByteArray,
          workflow_id = transactionMeta.workflowId,
          command_id = Some(completionInfo.commandId),
          submitters = Some(completionInfo.actAs.toSet),
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
          trace_context = serializedEmptyTraceContext,
          external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
          event_type = PersistentEventType.NonConsumingExercise.asInt,
          event_sequential_id = 0,
          node_id = exerciseNodeAId.index,
          additional_witnesses = Set("signatory"),
          consuming = Some(false),
          exercise_choice = exerciseNodeA.choiceId,
          exercise_choice_interface_id = None,
          exercise_argument =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingArg, emptyArray)),
          exercise_result =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingRes, emptyArray)),
          exercise_actors = Some(Set("signatory")),
          exercise_last_descendant_node_id = Some(exerciseNodeDId.index),
          exercise_argument_compression = compressionAlgorithmIdNonConsumingArg,
          exercise_result_compression = compressionAlgorithmIdNonConsumingRes,
          representative_package_id = None,
          contract_id = Some(exerciseNodeA.targetCoid),
          internal_contract_id = None,
          template_id = Some(templateIdWithPackageName(exerciseNodeA)),
          package_id = Some(exerciseNodeA.templateId.packageId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
        ),
        DbDto.IdFilterVariousWitness(
          IdFilter(
            event_sequential_id = 0,
            template_id = templateIdWithPackageName(exerciseNodeA),
            party_id = "signatory",
            first_per_sequential_id = true,
          )
        ),
        DbDto.EventVariousWitnessed(
          event_offset = someOffset.unwrap,
          update_id = updateIdByteArray,
          workflow_id = transactionMeta.workflowId,
          command_id = Some(completionInfo.commandId),
          submitters = Some(completionInfo.actAs.toSet),
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
          trace_context = serializedEmptyTraceContext,
          external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
          event_type = PersistentEventType.NonConsumingExercise.asInt,
          event_sequential_id = 0,
          node_id = exerciseNodeBId.index,
          additional_witnesses = Set("signatory"),
          consuming = Some(false),
          exercise_choice = exerciseNodeB.choiceId,
          exercise_choice_interface_id = None,
          exercise_argument =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingArg, emptyArray)),
          exercise_result =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingRes, emptyArray)),
          exercise_actors = Some(Set("signatory")),
          exercise_last_descendant_node_id = Some(exerciseNodeBId.index),
          exercise_argument_compression = compressionAlgorithmIdNonConsumingArg,
          exercise_result_compression = compressionAlgorithmIdNonConsumingRes,
          representative_package_id = None,
          contract_id = Some(exerciseNodeB.targetCoid),
          internal_contract_id = None,
          template_id = Some(templateIdWithPackageName(exerciseNodeB)),
          package_id = Some(exerciseNodeB.templateId.packageId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
        ),
        DbDto.IdFilterVariousWitness(
          IdFilter(
            event_sequential_id = 0,
            template_id = templateIdWithPackageName(exerciseNodeB),
            party_id = "signatory",
            first_per_sequential_id = true,
          )
        ),
        DbDto.EventVariousWitnessed(
          event_offset = someOffset.unwrap,
          update_id = updateIdByteArray,
          workflow_id = transactionMeta.workflowId,
          command_id = Some(completionInfo.commandId),
          submitters = Some(completionInfo.actAs.toSet),
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
          trace_context = serializedEmptyTraceContext,
          external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
          event_type = PersistentEventType.NonConsumingExercise.asInt,
          event_sequential_id = 0,
          node_id = exerciseNodeCId.index,
          additional_witnesses = Set("signatory"),
          consuming = Some(false),
          exercise_choice = exerciseNodeC.choiceId,
          exercise_choice_interface_id = None,
          exercise_argument =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingArg, emptyArray)),
          exercise_result =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingRes, emptyArray)),
          exercise_actors = Some(Set("signatory")),
          exercise_last_descendant_node_id = Some(exerciseNodeDId.index),
          exercise_argument_compression = compressionAlgorithmIdNonConsumingArg,
          exercise_result_compression = compressionAlgorithmIdNonConsumingRes,
          representative_package_id = None,
          contract_id = Some(exerciseNodeC.targetCoid),
          internal_contract_id = None,
          template_id = Some(templateIdWithPackageName(exerciseNodeC)),
          package_id = Some(exerciseNodeC.templateId.packageId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
        ),
        DbDto.IdFilterVariousWitness(
          IdFilter(
            event_sequential_id = 0,
            template_id = templateIdWithPackageName(exerciseNodeC),
            party_id = "signatory",
            first_per_sequential_id = true,
          )
        ),
        DbDto.EventVariousWitnessed(
          event_offset = someOffset.unwrap,
          update_id = updateIdByteArray,
          workflow_id = transactionMeta.workflowId,
          command_id = Some(completionInfo.commandId),
          submitters = Some(completionInfo.actAs.toSet),
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
          trace_context = serializedEmptyTraceContext,
          external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
          event_type = PersistentEventType.NonConsumingExercise.asInt,
          event_sequential_id = 0,
          node_id = exerciseNodeDId.index,
          additional_witnesses = Set("signatory"),
          consuming = Some(false),
          exercise_choice = exerciseNodeD.choiceId,
          exercise_choice_interface_id = None,
          exercise_argument =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingArg, emptyArray)),
          exercise_result =
            Some(compressArrayWith(compressionAlgorithmIdNonConsumingRes, emptyArray)),
          exercise_actors = Some(Set("signatory")),
          exercise_last_descendant_node_id = Some(exerciseNodeDId.index),
          exercise_argument_compression = compressionAlgorithmIdNonConsumingArg,
          exercise_result_compression = compressionAlgorithmIdNonConsumingRes,
          representative_package_id = None,
          contract_id = Some(exerciseNodeD.targetCoid),
          internal_contract_id = None,
          template_id = Some(templateIdWithPackageName(exerciseNodeD)),
          package_id = Some(exerciseNodeD.templateId.packageId),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
        ),
        DbDto.IdFilterVariousWitness(
          IdFilter(
            event_sequential_id = 0,
            template_id = templateIdWithPackageName(exerciseNodeD),
            party_id = "signatory",
            first_per_sequential_id = true,
          )
        ),
        DbDto.CommandCompletion(
          completion_offset = someOffset.unwrap,
          record_time = someRecordTime.toMicros,
          publication_time = 0,
          user_id = completionInfo.userId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          update_id = Some(updateIdByteArray),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
          submission_id = completionInfo.submissionId,
          deduplication_offset = None,
          deduplication_duration_nanos = None,
          deduplication_duration_seconds = None,
          synchronizer_id = someSynchronizerId1,
          message_uuid = None,
          is_transaction = true,
          trace_context = serializedEmptyTraceContext,
        ),
        DbDto.TransactionMeta(
          update_id = updateIdByteArray,
          event_offset = someOffset.unwrap,
          publication_time = 0,
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
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
      val builder = TxBuilder()
      val createNode = builder.create(
        id = builder.newCid,
        templateId = "M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
        key = CreateKey.SignatoryMaintainerKey(Value.ValueUnit),
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
        contract = createNode
      )
      builder.add(fetchNode)
      builder.add(fetchByKeyNode)
      builder.add(lookupByKeyNode)
      val transaction = builder.buildCommitted()
      val update = state.Update.SequencedTransactionAccepted(
        completionInfoO = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        updateId = updateId,
        contractAuthenticationData = Map.empty,
        synchronizerId = someSynchronizerId1,
        recordTime = someRecordTime,
        acsChangeFactory = TestAcsChangeFactory(),
        internalContractIds = Map.empty,
      )
      val dtos = updateToDtos(update)

      // Note: fetch and lookup nodes are not indexed
      dtos should contain theSameElementsInOrderAs List(
        DbDto.CommandCompletion(
          completion_offset = someOffset.unwrap,
          record_time = someRecordTime.toMicros,
          publication_time = 0,
          user_id = completionInfo.userId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          update_id = Some(updateIdByteArray),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
          submission_id = completionInfo.submissionId,
          deduplication_offset = None,
          deduplication_duration_nanos = None,
          deduplication_duration_seconds = None,
          synchronizer_id = someSynchronizerId1,
          message_uuid = None,
          is_transaction = true,
          trace_context = serializedEmptyTraceContext,
        ),
        DbDto.TransactionMeta(
          update_id = updateIdByteArray,
          event_offset = someOffset.unwrap,
          publication_time = 0,
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
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
      val externalTransactionHash = someExternalTransactionHash

      val builder = TxBuilder()
      val createNode = builder.create(
        id = builder.newCid,
        templateId = "M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
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
      val update = state.Update.SequencedTransactionAccepted(
        completionInfoO = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        updateId = updateId,
        contractAuthenticationData = Map.empty,
        synchronizerId = someSynchronizerId1,
        recordTime = someRecordTime,
        externalTransactionHash = Some(externalTransactionHash),
        acsChangeFactory = TestAcsChangeFactory(),
        internalContractIds = Map.empty,
      )
      val dtos = updateToDtos(update)

      dtos should contain theSameElementsInOrderAs List(
        DbDto.EventDeactivate(
          event_offset = someOffset.unwrap,
          update_id = updateIdByteArray,
          workflow_id = transactionMeta.workflowId,
          command_id = Some(completionInfo.commandId),
          submitters = Some(completionInfo.actAs.toSet),
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
          trace_context = serializedEmptyTraceContext,
          external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
          event_type = PersistentEventType.ConsumingExercise.asInt,
          event_sequential_id = 0,
          node_id = exerciseNodeId.index,
          deactivated_event_sequential_id = None,
          additional_witnesses = Some(Set("divulgee")),
          exercise_choice = exerciseNode.choiceId,
          exercise_choice_interface_id = None,
          exercise_argument =
            Some(compressArrayWith(compressionAlgorithmIdConsumingArg, emptyArray)),
          exercise_result = Some(compressArrayWith(compressionAlgorithmIdConsumingRes, emptyArray)),
          exercise_actors = Some(Set("signatory")),
          exercise_last_descendant_node_id = Some(exerciseNodeId.index),
          exercise_argument_compression = compressionAlgorithmIdConsumingArg,
          exercise_result_compression = compressionAlgorithmIdConsumingRes,
          reassignment_id = None,
          assignment_exclusivity = None,
          target_synchronizer_id = None,
          reassignment_counter = None,
          contract_id = exerciseNode.targetCoid,
          internal_contract_id = None,
          template_id = templateIdWithPackageName(exerciseNode),
          package_id = exerciseNode.templateId.packageId,
          stakeholders = Set("signatory", "observer"),
          ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
        ),
        DbDto.IdFilterDeactivateStakeholder(
          IdFilter(
            event_sequential_id = 0,
            template_id = templateIdWithPackageName(exerciseNode),
            party_id = "signatory",
            first_per_sequential_id = true,
          )
        ),
        DbDto.IdFilterDeactivateStakeholder(
          IdFilter(
            event_sequential_id = 0,
            template_id = templateIdWithPackageName(exerciseNode),
            party_id = "observer",
            first_per_sequential_id = false,
          )
        ),
        DbDto.IdFilterDeactivateWitness(
          IdFilter(
            event_sequential_id = 0,
            template_id = templateIdWithPackageName(exerciseNode),
            party_id = "divulgee",
            first_per_sequential_id = true,
          )
        ),
        DbDto.CommandCompletion(
          completion_offset = someOffset.unwrap,
          record_time = someRecordTime.toMicros,
          publication_time = 0,
          user_id = completionInfo.userId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          update_id = Some(updateIdByteArray),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
          submission_id = completionInfo.submissionId,
          deduplication_offset = None,
          deduplication_duration_nanos = None,
          deduplication_duration_seconds = None,
          synchronizer_id = someSynchronizerId1,
          message_uuid = None,
          is_transaction = true,
          trace_context = serializedEmptyTraceContext,
        ),
        DbDto.TransactionMeta(
          update_id = updateIdByteArray,
          event_offset = someOffset.unwrap,
          publication_time = 0,
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
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
      val externalTransactionHash = someExternalTransactionHash
      val builder = TxBuilder()
      val contractId = builder.newCid
      val interfaceId = toIdentifier("M:I")
      val createNode = builder.create(
        id = contractId,
        templateId = "M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
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
        interfaceId = Some(interfaceId),
      )
      val createNodeId = builder.add(createNode)
      val exerciseNodeId = builder.add(exerciseNode)
      val transaction = builder.buildCommitted()
      val update = state.Update.SequencedTransactionAccepted(
        completionInfoO = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        updateId = updateId,
        contractAuthenticationData = Map(contractId -> someContractAuthenticationData),
        synchronizerId = someSynchronizerId1,
        recordTime = someRecordTime,
        externalTransactionHash = Some(externalTransactionHash),
        acsChangeFactory = TestAcsChangeFactory(),
        internalContractIds = Map(contractId -> 42L),
      )
      val dtos = updateToDtos(update)

      dtos.head shouldEqual DbDto.EventActivate(
        event_offset = someOffset.unwrap,
        update_id = updateIdByteArray,
        workflow_id = transactionMeta.workflowId,
        command_id = Some(completionInfo.commandId),
        submitters = Some(completionInfo.actAs.toSet),
        record_time = someRecordTime.toMicros,
        synchronizer_id = someSynchronizerId1,
        trace_context = serializedEmptyTraceContext,
        external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
        event_type = PersistentEventType.Create.asInt,
        event_sequential_id = 0,
        node_id = createNodeId.index,
        additional_witnesses = Some(Set.empty),
        source_synchronizer_id = None,
        reassignment_counter = None,
        reassignment_id = None,
        representative_package_id = createNode.templateId.packageId,
        notPersistedContractId = createNode.coid,
        internal_contract_id = 42L,
        create_key_hash = None,
      )
      Set(dtos(1), dtos(2)) should contain theSameElementsAs Set(
        DbDto.IdFilterActivateStakeholder(
          IdFilter(
            0L,
            templateIdWithPackageName(createNode),
            "signatory",
            first_per_sequential_id = true,
          )
        ),
        DbDto.IdFilterActivateStakeholder(
          IdFilter(
            0L,
            templateIdWithPackageName(createNode),
            "observer",
            first_per_sequential_id = false,
          )
        ),
      )
      dtos(3) shouldEqual DbDto.EventDeactivate(
        event_offset = someOffset.unwrap,
        update_id = updateIdByteArray,
        workflow_id = transactionMeta.workflowId,
        command_id = Some(completionInfo.commandId),
        submitters = Some(completionInfo.actAs.toSet),
        record_time = someRecordTime.toMicros,
        synchronizer_id = someSynchronizerId1,
        trace_context = serializedEmptyTraceContext,
        external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
        event_type = PersistentEventType.ConsumingExercise.asInt,
        event_sequential_id = 0,
        node_id = exerciseNodeId.index,
        deactivated_event_sequential_id = None,
        additional_witnesses = Some(Set("divulgee")),
        exercise_choice = Some(exerciseNode.choiceId),
        exercise_choice_interface_id = Some(interfaceId.toString),
        exercise_argument = Some(compressArrayWith(compressionAlgorithmIdConsumingArg, emptyArray)),
        exercise_result = Some(compressArrayWith(compressionAlgorithmIdConsumingRes, emptyArray)),
        exercise_actors = Some(Set("signatory")),
        exercise_last_descendant_node_id = Some(exerciseNodeId.index),
        exercise_argument_compression = compressionAlgorithmIdConsumingArg,
        exercise_result_compression = compressionAlgorithmIdConsumingRes,
        reassignment_id = None,
        assignment_exclusivity = None,
        target_synchronizer_id = None,
        reassignment_counter = None,
        contract_id = exerciseNode.targetCoid,
        internal_contract_id = None,
        template_id = templateIdWithPackageName(exerciseNode),
        package_id = exerciseNode.templateId.packageId,
        stakeholders = Set("signatory", "observer"),
        ledger_effective_time = Some(transactionMeta.ledgerEffectiveTime.micros),
      )
      dtos(4) shouldEqual DbDto.IdFilterDeactivateStakeholder(
        IdFilter(
          event_sequential_id = 0,
          template_id = templateIdWithPackageName(exerciseNode),
          party_id = "signatory",
          first_per_sequential_id = true,
        )
      )
      dtos(5) shouldEqual DbDto.IdFilterDeactivateStakeholder(
        IdFilter(
          event_sequential_id = 0,
          template_id = templateIdWithPackageName(exerciseNode),
          party_id = "observer",
          first_per_sequential_id = false,
        )
      )
      dtos(6) shouldEqual DbDto.IdFilterDeactivateWitness(
        IdFilter(
          event_sequential_id = 0,
          template_id = templateIdWithPackageName(exerciseNode),
          party_id = "divulgee",
          first_per_sequential_id = true,
        )
      )
      dtos(7) shouldEqual DbDto.CommandCompletion(
        completion_offset = someOffset.unwrap,
        record_time = someRecordTime.toMicros,
        publication_time = 0,
        user_id = completionInfo.userId,
        submitters = completionInfo.actAs.toSet,
        command_id = completionInfo.commandId,
        update_id = Some(updateIdByteArray),
        rejection_status_code = None,
        rejection_status_message = None,
        rejection_status_details = None,
        submission_id = completionInfo.submissionId,
        deduplication_offset = None,
        deduplication_duration_nanos = None,
        deduplication_duration_seconds = None,
        synchronizer_id = someSynchronizerId1,
        message_uuid = None,
        is_transaction = true,
        trace_context = serializedEmptyTraceContext,
      )
      dtos(8) shouldEqual DbDto.TransactionMeta(
        update_id = updateIdByteArray,
        event_offset = someOffset.unwrap,
        publication_time = 0,
        record_time = someRecordTime.toMicros,
        synchronizer_id = someSynchronizerId1,
        event_sequential_id_first = 0,
        event_sequential_id_last = 0,
      )
      dtos.size shouldEqual 9
    }

    "handle TransactionAccepted (rollback node)" in {
      // Transaction
      // └─ #1 Rollback
      //    ├─ #2 Create
      //    └─ #3 Exercise (divulges #2 to divulgee)
      // - Create and Exercise events must not be visible
      val completionInfo = someCompletionInfo
      val transactionMeta = someTransactionMeta
      val builder = TxBuilder()
      val rollbackNode = builder.rollback()
      val createNode = builder.create(
        id = builder.newCid,
        templateId = "M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
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
      val update = state.Update.SequencedTransactionAccepted(
        completionInfoO = Some(completionInfo),
        transactionMeta = transactionMeta,
        transaction = transaction,
        updateId = updateId,
        contractAuthenticationData = Map.empty,
        synchronizerId = someSynchronizerId1,
        recordTime = someRecordTime,
        acsChangeFactory = TestAcsChangeFactory(),
        internalContractIds = Map.empty,
      )
      val dtos = updateToDtos(update)

      dtos should contain theSameElementsInOrderAs List(
        DbDto.CommandCompletion(
          completion_offset = someOffset.unwrap,
          record_time = someRecordTime.toMicros,
          publication_time = 0,
          user_id = completionInfo.userId,
          submitters = completionInfo.actAs.toSet,
          command_id = completionInfo.commandId,
          update_id = Some(updateIdByteArray),
          rejection_status_code = None,
          rejection_status_message = None,
          rejection_status_details = None,
          submission_id = completionInfo.submissionId,
          deduplication_offset = None,
          deduplication_duration_nanos = None,
          deduplication_duration_seconds = None,
          synchronizer_id = someSynchronizerId1,
          message_uuid = None,
          is_transaction = true,
          trace_context = serializedEmptyTraceContext,
        ),
        DbDto.TransactionMeta(
          update_id = updateIdByteArray,
          event_offset = someOffset.unwrap,
          publication_time = 0,
          record_time = someRecordTime.toMicros,
          synchronizer_id = someSynchronizerId1,
          event_sequential_id_first = 0,
          event_sequential_id_last = 0,
        ),
      )
    }

    "handle TransactionAccepted (no submission info)" in {
      // Transaction that is missing a SubmitterInfo
      // This happens if a transaction was submitted through a different participant
      val transactionMeta = someTransactionMeta
      val externalTransactionHash = someExternalTransactionHash
      val builder = TxBuilder()
      val contractId = builder.newCid
      val createNode = builder.create(
        id = contractId,
        templateId = "M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
      )
      val createNodeId = builder.add(createNode)
      val transaction = builder.buildCommitted()
      val update = state.Update.SequencedTransactionAccepted(
        completionInfoO = None,
        transactionMeta = transactionMeta,
        transaction = transaction,
        updateId = updateId,
        contractAuthenticationData = Map(contractId -> someContractAuthenticationData),
        synchronizerId = someSynchronizerId1,
        recordTime = someRecordTime,
        externalTransactionHash = Some(externalTransactionHash),
        acsChangeFactory = TestAcsChangeFactory(),
        internalContractIds = Map(contractId -> 42L),
      )
      val dtos = updateToDtos(update)

      dtos.head shouldEqual DbDto.EventActivate(
        event_offset = someOffset.unwrap,
        update_id = updateIdByteArray,
        workflow_id = transactionMeta.workflowId,
        command_id = None,
        submitters = None,
        record_time = someRecordTime.toMicros,
        synchronizer_id = someSynchronizerId1,
        trace_context = serializedEmptyTraceContext,
        external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
        event_type = PersistentEventType.Create.asInt,
        event_sequential_id = 0,
        node_id = createNodeId.index,
        additional_witnesses = Some(Set.empty),
        source_synchronizer_id = None,
        reassignment_counter = None,
        reassignment_id = None,
        representative_package_id = createNode.templateId.packageId,
        notPersistedContractId = createNode.coid,
        internal_contract_id = 42L,
        create_key_hash = None,
      )
      Set(dtos(1), dtos(2)) should contain theSameElementsAs Set(
        DbDto.IdFilterActivateStakeholder(
          IdFilter(
            0L,
            templateIdWithPackageName(createNode),
            "signatory",
            first_per_sequential_id = true,
          )
        ),
        DbDto.IdFilterActivateStakeholder(
          IdFilter(
            0L,
            templateIdWithPackageName(createNode),
            "observer",
            first_per_sequential_id = false,
          )
        ),
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
        Some(DeduplicationOffset(None)),
        Some(0L),
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
          val update = state.Update.SequencedCommandRejected(
            completionInfo,
            state.Update.CommandRejected.FinalReason(status),
            someSynchronizerId1,
            someRecordTime,
          )
          val dtos = updateToDtos(update)

          dtos should contain theSameElementsInOrderAs List(
            DbDto.CommandCompletion(
              completion_offset = someOffset.unwrap,
              record_time = someRecordTime.toMicros,
              publication_time = 0,
              user_id = someUserId,
              submitters = Set(someParty),
              command_id = someCommandId,
              update_id = None,
              rejection_status_code = Some(status.code),
              rejection_status_message = Some(status.message),
              rejection_status_details = Some(StatusDetails.of(status.details).toByteArray),
              submission_id = Some(someSubmissionId),
              deduplication_offset = expectedDeduplicationOffset,
              deduplication_duration_seconds = expectedDeduplicationDurationSeconds,
              deduplication_duration_nanos = expectedDeduplicationDurationNanos,
              synchronizer_id = someSynchronizerId1,
              message_uuid = None,
              is_transaction = true,
              trace_context = serializedEmptyTraceContext,
            )
          )
      }
    }

    "handle TransactionAccepted (all deduplication data)" in {
      val transactionMeta = someTransactionMeta
      val externalTransactionHash = someExternalTransactionHash
      val builder = TxBuilder()
      val contractId = builder.newCid
      val createNode = builder.create(
        id = contractId,
        templateId = "M:T",
        argument = Value.ValueUnit,
        signatories = List("signatory"),
        observers = List("observer"),
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
          val update = state.Update.SequencedTransactionAccepted(
            completionInfoO = Some(completionInfo),
            transactionMeta = transactionMeta,
            transaction = transaction,
            updateId = updateId,
            contractAuthenticationData = Map(contractId -> someContractAuthenticationData),
            synchronizerId = someSynchronizerId1,
            recordTime = someRecordTime,
            externalTransactionHash = Some(externalTransactionHash),
            acsChangeFactory = TestAcsChangeFactory(),
            internalContractIds = Map(contractId -> 42L),
          )
          val dtos = updateToDtos(update)

          dtos.head shouldEqual DbDto.EventActivate(
            event_offset = someOffset.unwrap,
            update_id = updateIdByteArray,
            workflow_id = transactionMeta.workflowId,
            command_id = Some(completionInfo.commandId),
            submitters = Some(completionInfo.actAs.toSet),
            record_time = someRecordTime.toMicros,
            synchronizer_id = someSynchronizerId1,
            trace_context = serializedEmptyTraceContext,
            external_transaction_hash = Some(externalTransactionHash.unwrap.toByteArray),
            event_type = PersistentEventType.Create.asInt,
            event_sequential_id = 0,
            node_id = createNodeId.index,
            additional_witnesses = Some(Set.empty),
            source_synchronizer_id = None,
            reassignment_counter = None,
            reassignment_id = None,
            representative_package_id = createNode.templateId.packageId,
            notPersistedContractId = createNode.coid,
            internal_contract_id = 42L,
            create_key_hash = None,
          )
          Set(dtos(1), dtos(2)) should contain theSameElementsAs Set(
            DbDto.IdFilterActivateStakeholder(
              IdFilter(
                0L,
                templateIdWithPackageName(createNode),
                "signatory",
                first_per_sequential_id = true,
              )
            ),
            DbDto.IdFilterActivateStakeholder(
              IdFilter(
                0L,
                templateIdWithPackageName(createNode),
                "observer",
                first_per_sequential_id = false,
              )
            ),
          )
          dtos(3) shouldEqual DbDto.CommandCompletion(
            completion_offset = someOffset.unwrap,
            record_time = someRecordTime.toMicros,
            publication_time = 0,
            user_id = completionInfo.userId,
            submitters = completionInfo.actAs.toSet,
            command_id = completionInfo.commandId,
            update_id = Some(updateIdByteArray),
            rejection_status_code = None,
            rejection_status_message = None,
            rejection_status_details = None,
            submission_id = Some(someSubmissionId),
            deduplication_offset = expectedDeduplicationOffset,
            deduplication_duration_seconds = expectedDeduplicationDurationSeconds,
            deduplication_duration_nanos = expectedDeduplicationDurationNanos,
            synchronizer_id = someSynchronizerId1,
            message_uuid = None,
            is_transaction = true,
            trace_context = serializedEmptyTraceContext,
          )
          dtos(4) shouldEqual DbDto.TransactionMeta(
            update_id = updateIdByteArray,
            event_offset = someOffset.unwrap,
            publication_time = 0,
            record_time = someRecordTime.toMicros,
            synchronizer_id = someSynchronizerId1,
            event_sequential_id_first = 0,
            event_sequential_id_last = 0,
          )
          dtos.size shouldEqual 5
      }
    }

    "handle ReassignmentAccepted - Assign" in {
      val completionInfo = someCompletionInfo
      val builder = TxBuilder()
      val contractId = builder.newCid
      val createNode = builder
        .create(
          id = contractId,
          templateId = "M:T",
          argument = Value.ValueUnit,
          signatories = Set("signatory"),
          observers = Set("observer", "observer2"),
        )

      val targetSynchronizerId = Target(SynchronizerId.tryFromString("x::synchronizer2"))
      val update = state.Update.SequencedReassignmentAccepted(
        optCompletionInfo = Some(completionInfo),
        workflowId = Some(someWorkflowId),
        updateId = updateId,
        reassignmentInfo = ReassignmentInfo(
          sourceSynchronizer = Source(SynchronizerId.tryFromString("x::synchronizer1")),
          targetSynchronizer = targetSynchronizerId,
          submitter = Option(someParty),
          reassignmentId = ReassignmentId.tryCreate("001000000000"),
          isReassigningParticipant = true,
        ),
        reassignment = Reassignment.Batch(
          Reassignment.Assign(
            ledgerEffectiveTime = Time.Timestamp.assertFromLong(17000000),
            createNode = createNode,
            contractAuthenticationData = someContractAuthenticationData,
            reassignmentCounter = 1500L,
            nodeId = 0,
          )
        ),
        recordTime = someRecordTime,
        synchronizerId = targetSynchronizerId.unwrap,
        acsChangeFactory = TestAcsChangeFactory(),
        internalContractIds = Map(contractId -> 42L),
      )

      val dtos = updateToDtos(update)

      dtos.head shouldEqual DbDto.EventActivate(
        event_offset = someOffset.unwrap,
        update_id = update.updateId.toProtoPrimitive.toByteArray,
        workflow_id = Some(someWorkflowId),
        command_id = Some(completionInfo.commandId),
        submitters = Option(Set(someParty)),
        record_time = someRecordTime.toMicros,
        synchronizer_id = SynchronizerId.tryFromString("x::synchronizer2"),
        trace_context = serializedEmptyTraceContext,
        external_transaction_hash = None,
        event_type = PersistentEventType.Assign.asInt,
        event_sequential_id = 0,
        node_id = 0,
        additional_witnesses = None,
        source_synchronizer_id = Some(SynchronizerId.tryFromString("x::synchronizer1")),
        reassignment_counter = Some(1500L),
        reassignment_id = Some(ReassignmentId.tryCreate("001000000000").toBytes.toByteArray),
        representative_package_id = createNode.templateId.packageId,
        notPersistedContractId = createNode.coid,
        internal_contract_id = 42L,
        create_key_hash = None,
      )
      dtos(4) shouldEqual DbDto.CommandCompletion(
        completion_offset = someOffset.unwrap,
        record_time = someRecordTime.toMicros,
        publication_time = 0,
        user_id = completionInfo.userId,
        submitters = completionInfo.actAs.toSet,
        command_id = completionInfo.commandId,
        update_id = Some(updateIdByteArray),
        rejection_status_code = None,
        rejection_status_message = None,
        rejection_status_details = None,
        submission_id = completionInfo.submissionId,
        deduplication_offset = None,
        deduplication_duration_nanos = None,
        deduplication_duration_seconds = None,
        synchronizer_id = SynchronizerId.tryFromString("x::synchronizer2"),
        message_uuid = None,
        is_transaction = false,
        trace_context = serializedEmptyTraceContext,
      )
      dtos(5) shouldEqual DbDto.TransactionMeta(
        update_id = updateIdByteArray,
        event_offset = someOffset.unwrap,
        publication_time = 0,
        record_time = someRecordTime.toMicros,
        synchronizer_id = SynchronizerId.tryFromString("x::synchronizer2"),
        event_sequential_id_first = 0,
        event_sequential_id_last = 0,
      )
      Set(dtos(1), dtos(2), dtos(3)) should contain theSameElementsAs Set(
        DbDto.IdFilterActivateStakeholder(
          IdFilter(
            0L,
            templateIdWithPackageName(createNode),
            "signatory",
            first_per_sequential_id = true,
          )
        ),
        DbDto.IdFilterActivateStakeholder(
          IdFilter(
            0L,
            templateIdWithPackageName(createNode),
            "observer",
            first_per_sequential_id = false,
          )
        ),
        DbDto.IdFilterActivateStakeholder(
          IdFilter(
            0L,
            templateIdWithPackageName(createNode),
            "observer2",
            first_per_sequential_id = false,
          )
        ),
      )
      dtos.size shouldEqual 6
    }

    "handle ReassignmentAccepted - Unassign" in {
      val completionInfo = someCompletionInfo
      val builder = TxBuilder()
      val contractId = builder.newCid
      val createNode = builder
        .create(
          id = contractId,
          templateId = "M:T",
          argument = Value.ValueUnit,
          signatories = Set("signatory"),
          observers = Set("observer"),
        )

      val sourceSynchronizerId = Source(SynchronizerId.tryFromString("x::synchronizer1"))
      val update = state.Update.SequencedReassignmentAccepted(
        optCompletionInfo = Some(completionInfo),
        workflowId = Some(someWorkflowId),
        updateId = updateId,
        reassignmentInfo = ReassignmentInfo(
          sourceSynchronizer = sourceSynchronizerId,
          targetSynchronizer = Target(SynchronizerId.tryFromString("x::synchronizer2")),
          submitter = Option(someParty),
          reassignmentId = ReassignmentId.tryCreate("001000000000"),
          isReassigningParticipant = true,
        ),
        reassignment = Reassignment.Batch(
          Reassignment.Unassign(
            contractId = contractId,
            templateId = createNode.templateId,
            packageName = createNode.packageName,
            stakeholders =
              List("signatory12", "observer23", "asdasdasd").map(Ref.Party.assertFromString),
            assignmentExclusivity = Some(Time.Timestamp.assertFromLong(123456)),
            reassignmentCounter = 1500L,
            nodeId = 0,
          )
        ),
        recordTime = CantonTimestamp.ofEpochMicro(120),
        synchronizerId = sourceSynchronizerId.unwrap,
        acsChangeFactory = TestAcsChangeFactory(),
        internalContractIds = Map.empty,
      )

      val dtos = updateToDtos(update)

      dtos.head shouldEqual DbDto.EventDeactivate(
        event_offset = someOffset.unwrap,
        update_id = update.updateId.toProtoPrimitive.toByteArray,
        workflow_id = Some(someWorkflowId),
        command_id = Some(completionInfo.commandId),
        submitters = Some(Set(someParty)),
        record_time = 120L,
        synchronizer_id = SynchronizerId.tryFromString("x::synchronizer1"),
        trace_context = serializedEmptyTraceContext,
        external_transaction_hash = None,
        event_type = PersistentEventType.Unassign.asInt,
        event_sequential_id = 0,
        node_id = 0,
        deactivated_event_sequential_id = None,
        additional_witnesses = None,
        exercise_choice = None,
        exercise_choice_interface_id = None,
        exercise_argument = None,
        exercise_result = None,
        exercise_actors = None,
        exercise_last_descendant_node_id = None,
        exercise_argument_compression = None,
        exercise_result_compression = None,
        reassignment_id = Some(ReassignmentId.tryCreate("001000000000").toBytes.toByteArray),
        assignment_exclusivity = Some(123456L),
        target_synchronizer_id = Some(SynchronizerId.tryFromString("x::synchronizer2")),
        reassignment_counter = Some(1500L),
        contract_id = createNode.coid,
        internal_contract_id = None,
        template_id = templateIdWithPackageName(createNode),
        package_id = createNode.templateId.packageId,
        stakeholders = Set("signatory12", "observer23", "asdasdasd"),
        ledger_effective_time = None,
      )
      dtos(4) shouldEqual DbDto.CommandCompletion(
        completion_offset = someOffset.unwrap,
        record_time = 120L,
        publication_time = 0,
        user_id = completionInfo.userId,
        submitters = completionInfo.actAs.toSet,
        command_id = completionInfo.commandId,
        update_id = Some(updateIdByteArray),
        rejection_status_code = None,
        rejection_status_message = None,
        rejection_status_details = None,
        submission_id = completionInfo.submissionId,
        deduplication_offset = None,
        deduplication_duration_nanos = None,
        deduplication_duration_seconds = None,
        synchronizer_id = SynchronizerId.tryFromString("x::synchronizer1"),
        message_uuid = None,
        is_transaction = false,
        trace_context = serializedEmptyTraceContext,
      )
      dtos(5) shouldEqual DbDto.TransactionMeta(
        update_id = updateIdByteArray,
        event_offset = someOffset.unwrap,
        publication_time = 0,
        record_time = 120L,
        synchronizer_id = SynchronizerId.tryFromString("x::synchronizer1"),
        event_sequential_id_first = 0,
        event_sequential_id_last = 0,
      )
      Set(dtos(1), dtos(2), dtos(3)) should contain theSameElementsAs Set(
        DbDto.IdFilterDeactivateStakeholder(
          IdFilter(
            0L,
            templateIdWithPackageName(createNode),
            "signatory12",
            first_per_sequential_id = true,
          )
        ),
        DbDto.IdFilterDeactivateStakeholder(
          IdFilter(
            0L,
            templateIdWithPackageName(createNode),
            "observer23",
            first_per_sequential_id = false,
          )
        ),
        DbDto.IdFilterDeactivateStakeholder(
          IdFilter(
            0L,
            templateIdWithPackageName(createNode),
            "asdasdasd",
            first_per_sequential_id = false,
          )
        ),
      )
      dtos.size shouldEqual 6
    }

    "handle TopologyTransactionEffective - PartyToParticipantAuthorization" in {
      val submissionParty = Ref.Party.assertFromString("SubmissionParty")
      val confirmationParty = Ref.Party.assertFromString("ConfirmationParty")
      val observationParty = Ref.Party.assertFromString("ObservationParty")

      val events = Set[TopologyEvent](
        PartyToParticipantAuthorization(
          party = submissionParty,
          participant = someParticipantId,
          authorizationEvent = Added(Submission),
        ),
        PartyToParticipantAuthorization(
          party = confirmationParty,
          participant = someParticipantId,
          authorizationEvent = Added(Confirmation),
        ),
        PartyToParticipantAuthorization(
          party = observationParty,
          participant = someParticipantId,
          authorizationEvent = Added(Observation),
        ),
        PartyToParticipantAuthorization(
          party = submissionParty,
          participant = otherParticipantId,
          authorizationEvent = ChangedTo(Submission),
        ),
        PartyToParticipantAuthorization(
          party = confirmationParty,
          participant = otherParticipantId,
          authorizationEvent = ChangedTo(Confirmation),
        ),
        PartyToParticipantAuthorization(
          party = observationParty,
          participant = otherParticipantId,
          authorizationEvent = ChangedTo(Observation),
        ),
        PartyToParticipantAuthorization(
          party = someParty,
          participant = someParticipantId,
          authorizationEvent = Revoked,
        ),
      )

      val update = state.Update.TopologyTransactionEffective(
        updateId = updateId,
        events = events,
        synchronizerId = someSynchronizerId1,
        effectiveTime = someRecordTime,
      )

      def eventPartyToParticipant(
          partyId: String,
          participantId: String,
          authorizationEvent: AuthorizationEvent,
      ) =
        DbDto.EventPartyToParticipant(
          event_sequential_id = 0,
          event_offset = someOffset.unwrap,
          update_id = update.updateId.toProtoPrimitive.toByteArray,
          party_id = partyId,
          participant_id = participantId,
          participant_permission = participantPermissionInt(authorizationEvent),
          participant_authorization_event = authorizationEventInt(authorizationEvent),
          synchronizer_id = someSynchronizerId1,
          record_time = someRecordTime.toMicros,
          trace_context = serializedEmptyTraceContext,
        )

      val dtos = updateToDtos(update)

      dtos should contain(
        eventPartyToParticipant(
          partyId = submissionParty,
          participantId = someParticipantId,
          authorizationEvent = Added(Submission),
        )
      )
      dtos should contain(
        eventPartyToParticipant(
          partyId = confirmationParty,
          participantId = someParticipantId,
          authorizationEvent = Added(Confirmation),
        )
      )
      dtos should contain(
        eventPartyToParticipant(
          partyId = observationParty,
          participantId = someParticipantId,
          authorizationEvent = Added(Observation),
        )
      )
      dtos should contain(
        eventPartyToParticipant(
          partyId = submissionParty,
          participantId = otherParticipantId,
          authorizationEvent = ChangedTo(Submission),
        )
      )
      dtos should contain(
        eventPartyToParticipant(
          partyId = confirmationParty,
          participantId = otherParticipantId,
          authorizationEvent = ChangedTo(Confirmation),
        )
      )
      dtos should contain(
        eventPartyToParticipant(
          partyId = observationParty,
          participantId = otherParticipantId,
          authorizationEvent = ChangedTo(Observation),
        )
      )
      dtos should contain(
        eventPartyToParticipant(
          partyId = someParty,
          participantId = someParticipantId,
          authorizationEvent = Revoked,
        )
      )
      dtos should contain(
        DbDto.TransactionMeta(
          update_id = updateIdByteArray,
          event_offset = someOffset.unwrap,
          publication_time = 0,
          record_time = someRecordTime.toMicros,
          synchronizer_id = SynchronizerId.tryFromString("x::synchronizer1"),
          event_sequential_id_first = 0,
          event_sequential_id_last = 0,
        )
      )
    }

    "handle SequencerIndexMoved" in {
      val update = state.Update.SequencerIndexMoved(
        synchronizerId = someSynchronizerId1,
        recordTime = CantonTimestamp.ofEpochMicro(2000),
      )
      val dtos = updateToDtos(update)

      dtos.head shouldEqual DbDto.SequencerIndexMoved(
        synchronizerId = someSynchronizerId1
      )
      dtos.size shouldEqual 1
    }

  }

  private def updateToDtos(update: Update) =
    UpdateToDbDto(
      someParticipantId,
      valueSerialization,
      compressionStrategy,
      LedgerApiServerMetrics.ForTesting,
    )(
      MetricsContext.Empty
    )(
      someOffset
    )(update).toList
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
    override def serialize(create: Create): (Array[Byte], Option[Array[Byte]]) =
      (emptyArray, create.keyOpt.map(_ => emptyArray))

    /** Returns (choice argument, exercise result, contract key) */
    override def serialize(
        exercise: Exercise
    ): (Array[Byte], Option[Array[Byte]], Option[Array[Byte]]) =
      (
        emptyArray,
        exercise.exerciseResult.map(_ => emptyArray),
        exercise.keyOpt.map(_ => emptyArray),
      )
  }

  // These test do not check the correctness of compression.
  // All values are compressed using a dummy (identity) algorithm in this suite.
  private val compressionAlgorithmIdInvalid = Some(12)
  private val compressionAlgorithmIdConsumingArg = Some(13)
  private val compressionAlgorithmIdConsumingRes = Some(14)
  private val compressionAlgorithmIdNonConsumingArg = Some(15)
  private val compressionAlgorithmIdNonConsumingRes = Some(16)
  private val compressionStrategy: CompressionStrategy = CompressionStrategy(
    new FieldCompressionStrategy(compressionAlgorithmIdInvalid, x => x),
    new FieldCompressionStrategy(compressionAlgorithmIdInvalid, x => x),
    new FieldCompressionStrategy(
      compressionAlgorithmIdConsumingArg,
      compressArrayWith(compressionAlgorithmIdConsumingArg, _),
    ),
    new FieldCompressionStrategy(
      compressionAlgorithmIdConsumingRes,
      compressArrayWith(compressionAlgorithmIdConsumingRes, _),
    ),
    new FieldCompressionStrategy(
      compressionAlgorithmIdNonConsumingArg,
      compressArrayWith(compressionAlgorithmIdNonConsumingArg, _),
    ),
    new FieldCompressionStrategy(
      compressionAlgorithmIdNonConsumingRes,
      compressArrayWith(compressionAlgorithmIdNonConsumingRes, _),
    ),
  )

  private def compressArrayWith(id: Option[Int], x: Array[Byte]) =
    x ++ Array(id.getOrElse(-1).toByte)

  private val someParticipantId =
    Ref.ParticipantId.assertFromString("UpdateToDbDtoSpecParticipant")
  private val otherParticipantId =
    Ref.ParticipantId.assertFromString("UpdateToDbDtoSpecRemoteParticipant")
  private val someOffset = Offset.tryFromLong(12345678L)
  private val someRecordTime =
    CantonTimestamp(
      Time.Timestamp.assertFromInstant(Instant.parse(("2000-01-01T00:00:00.000000Z")))
    )
  private val someUserId =
    Ref.UserId.assertFromString("UpdateToDbDtoSpecUserId")
  private val someCommandId = Ref.CommandId.assertFromString("UpdateToDbDtoSpecCommandId")
  private val someSubmissionId =
    Ref.SubmissionId.assertFromString("UpdateToDbDtoSpecSubmissionId")
  private val someWorkflowId = Ref.WorkflowId.assertFromString("UpdateToDbDtoSpecWorkflowId")
  private val someParty = Ref.Party.assertFromString("UpdateToDbDtoSpecParty")
  private val someHash =
    crypto.Hash.assertFromString("01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086")
  private val someCompletionInfo = state.CompletionInfo(
    actAs = List(someParty),
    userId = someUserId,
    commandId = someCommandId,
    optDeduplicationPeriod = None,
    submissionId = Some(someSubmissionId),
  )
  private val someSynchronizerId1 = SynchronizerId.tryFromString("x::synchronizer1")
  private val someTransactionMeta = state.TransactionMeta(
    ledgerEffectiveTime = Time.Timestamp.assertFromLong(2),
    workflowId = Some(someWorkflowId),
    preparationTime = Time.Timestamp.assertFromLong(3),
    submissionSeed = someHash,
    timeBoundaries = LedgerTimeBoundaries.unconstrained,
    optUsedPackages = None,
    optNodeSeeds = None,
    optByKeyNodes = None,
  )
  private val someContractAuthenticationData = Bytes.assertFromString("00abcd")
  private val someRepresentativePackageId = Ref.PackageId.assertFromString("rp-id")

  implicit private val DbDtoEqual: org.scalactic.Equality[DbDto] = ScalatestEqualityHelpers.DbDtoEq

  private val serializedEmptyTraceContext =
    SerializableTraceContext(emptyTraceContext).toDamlProto.toByteArray
}
