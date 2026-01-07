// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.platform.store.backend.DbDto.IdFilter
import com.digitalasset.canton.protocol.TestUpdateId
import com.digitalasset.daml.lf.data.Ref
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DbDtoSpec extends AnyWordSpec with Matchers {
  import StorageBackendTestValues.*
  implicit private val DbDtoEqual: org.scalactic.Equality[DbDto] = ScalatestEqualityHelpers.DbDtoEq

  val updateId = TestUpdateId("mock_hash")
  val updateIdByteArray = updateId.toProtoPrimitive.toByteArray

  "DbDto.createDbDtos" should {
    "populate correct DbDtos" in {
      DbDto
        .createDbDtos(
          event_offset = 1,
          update_id = updateIdByteArray,
          workflow_id = Some("w"),
          command_id = Some("c"),
          submitters = Some(Set(someParty)),
          record_time = 2,
          synchronizer_id = someSynchronizerId,
          trace_context = serializableTraceContext,
          external_transaction_hash = Some(someExternalTransactionHashBinary),
          event_sequential_id = 3,
          node_id = 4,
          additional_witnesses = Set(someParty2),
          representative_package_id = someRepresentativePackageId,
          notPersistedContractId = hashCid("1"),
          internal_contract_id = 3,
          create_key_hash = Some("hash"),
        )(
          stakeholders = Set(someParty3, someParty4),
          template_id = someTemplateId,
        )
        .toList should contain theSameElementsInOrderAs List(
        DbDto.EventActivate(
          event_offset = 1,
          update_id = updateIdByteArray,
          workflow_id = Some("w"),
          command_id = Some("c"),
          submitters = Some(Set(someParty)),
          record_time = 2,
          synchronizer_id = someSynchronizerId,
          trace_context = serializableTraceContext,
          external_transaction_hash = Some(someExternalTransactionHashBinary),
          event_type = PersistentEventType.Create.asInt,
          event_sequential_id = 3,
          node_id = 4,
          additional_witnesses = Some(Set(someParty2)),
          source_synchronizer_id = None,
          reassignment_counter = None,
          reassignment_id = None,
          representative_package_id = someRepresentativePackageId,
          notPersistedContractId = hashCid("1"),
          internal_contract_id = 3,
          create_key_hash = Some("hash"),
        ),
        DbDto.IdFilterActivateStakeholder(
          IdFilter(
            event_sequential_id = 3,
            template_id = someTemplateId,
            party_id = someParty3,
            first_per_sequential_id = true,
          )
        ),
        DbDto.IdFilterActivateStakeholder(
          IdFilter(
            event_sequential_id = 3,
            template_id = someTemplateId,
            party_id = someParty4,
            first_per_sequential_id = false,
          )
        ),
        DbDto.IdFilterActivateWitness(
          IdFilter(
            event_sequential_id = 3,
            template_id = someTemplateId,
            party_id = someParty2,
            first_per_sequential_id = true,
          )
        ),
      )
    }
  }

  "DbDto.assignDbDtos" should {
    "populate correct DbDtos" in {
      DbDto
        .assignDbDtos(
          event_offset = 1,
          update_id = updateIdByteArray,
          workflow_id = Some("w"),
          command_id = Some("c"),
          submitter = Some(someParty),
          record_time = 2,
          synchronizer_id = someSynchronizerId,
          trace_context = serializableTraceContext,
          event_sequential_id = 3,
          node_id = 4,
          source_synchronizer_id = someSynchronizerId2,
          reassignment_counter = 19,
          reassignment_id = Array(1, 2),
          representative_package_id = someRepresentativePackageId,
          notPersistedContractId = hashCid("1"),
          internal_contract_id = 3,
        )(
          stakeholders = someParties("party3", "party4"),
          template_id = someTemplateId,
        )
        .toList should contain theSameElementsInOrderAs List(
        DbDto.EventActivate(
          event_offset = 1,
          update_id = updateIdByteArray,
          workflow_id = Some("w"),
          command_id = Some("c"),
          submitters = Some(Set(someParty)),
          record_time = 2,
          synchronizer_id = someSynchronizerId,
          trace_context = serializableTraceContext,
          external_transaction_hash = None,
          event_type = PersistentEventType.Assign.asInt,
          event_sequential_id = 3,
          node_id = 4,
          additional_witnesses = None,
          source_synchronizer_id = Some(someSynchronizerId2),
          reassignment_counter = Some(19),
          reassignment_id = Some(Array(1, 2)),
          representative_package_id = someRepresentativePackageId,
          notPersistedContractId = hashCid("1"),
          internal_contract_id = 3,
          create_key_hash = None,
        ),
        DbDto.IdFilterActivateStakeholder(
          IdFilter(
            event_sequential_id = 3,
            template_id = someTemplateId,
            party_id = someParty3,
            first_per_sequential_id = true,
          )
        ),
        DbDto.IdFilterActivateStakeholder(
          IdFilter(
            event_sequential_id = 3,
            template_id = someTemplateId,
            party_id = someParty4,
            first_per_sequential_id = false,
          )
        ),
      )
    }
  }

  "DbDto.consumingExerciseDbDtos" should {
    "populate correct DbDtos" in {
      DbDto
        .consumingExerciseDbDtos(
          event_offset = 1,
          update_id = updateIdByteArray,
          workflow_id = Some("w"),
          command_id = Some("c"),
          submitters = Some(Set(someParty)),
          record_time = 2,
          synchronizer_id = someSynchronizerId,
          trace_context = serializableTraceContext,
          external_transaction_hash = Some(someExternalTransactionHashBinary),
          event_sequential_id = 3,
          node_id = 4,
          deactivated_event_sequential_id = Some(10),
          additional_witnesses = Set(someParty2),
          exercise_choice = someChoice,
          exercise_choice_interface_id = Some(someInterfaceId),
          exercise_argument = Array(1, 2, 3),
          exercise_result = Some(Array(1, 2, 3, 4)),
          exercise_actors = Set(someParty5),
          exercise_last_descendant_node_id = 10,
          exercise_argument_compression = Some(1),
          exercise_result_compression = Some(2),
          contract_id = hashCid("23"),
          internal_contract_id = Some(3),
          template_id = someTemplateId,
          package_id = somePackageId,
          stakeholders = someParties("1", "2", "3"),
          ledger_effective_time = 13,
        )
        .toList should contain theSameElementsInOrderAs List(
        DbDto.EventDeactivate(
          event_offset = 1,
          update_id = updateIdByteArray,
          workflow_id = Some("w"),
          command_id = Some("c"),
          submitters = Some(Set(someParty)),
          record_time = 2,
          synchronizer_id = someSynchronizerId,
          trace_context = serializableTraceContext,
          external_transaction_hash = Some(someExternalTransactionHashBinary),
          event_type = PersistentEventType.ConsumingExercise.asInt,
          event_sequential_id = 3,
          node_id = 4,
          deactivated_event_sequential_id = Some(10),
          additional_witnesses = Some(Set(someParty2)),
          exercise_choice = Some(someChoice),
          exercise_choice_interface_id = Some(someInterfaceId),
          exercise_argument = Some(Array(1, 2, 3)),
          exercise_result = Some(Array(1, 2, 3, 4)),
          exercise_actors = Some(Set(someParty5)),
          exercise_last_descendant_node_id = Some(10),
          exercise_argument_compression = Some(1),
          exercise_result_compression = Some(2),
          reassignment_id = None,
          assignment_exclusivity = None,
          target_synchronizer_id = None,
          reassignment_counter = None,
          contract_id = hashCid("23"),
          internal_contract_id = Some(3),
          template_id = someTemplateId,
          package_id = somePackageId,
          stakeholders = someParties("1", "2", "3"),
          ledger_effective_time = Some(13),
        ),
        DbDto.IdFilterDeactivateStakeholder(
          IdFilter(
            event_sequential_id = 3,
            template_id = someTemplateId,
            party_id = Ref.Party.assertFromString("1"),
            first_per_sequential_id = true,
          )
        ),
        DbDto.IdFilterDeactivateStakeholder(
          IdFilter(
            event_sequential_id = 3,
            template_id = someTemplateId,
            party_id = Ref.Party.assertFromString("2"),
            first_per_sequential_id = false,
          )
        ),
        DbDto.IdFilterDeactivateStakeholder(
          IdFilter(
            event_sequential_id = 3,
            template_id = someTemplateId,
            party_id = Ref.Party.assertFromString("3"),
            first_per_sequential_id = false,
          )
        ),
        DbDto.IdFilterDeactivateWitness(
          IdFilter(
            event_sequential_id = 3,
            template_id = someTemplateId,
            party_id = someParty2,
            first_per_sequential_id = true,
          )
        ),
      )
    }
  }

  "DbDto.unassignDbDtos" should {
    "populate correct DbDtos" in {
      DbDto
        .unassignDbDtos(
          event_offset = 1,
          update_id = updateIdByteArray,
          workflow_id = Some("w"),
          command_id = Some("c"),
          submitter = Some(someParty),
          record_time = 2,
          synchronizer_id = someSynchronizerId,
          trace_context = serializableTraceContext,
          event_sequential_id = 3,
          node_id = 4,
          deactivated_event_sequential_id = Some(10),
          reassignment_id = Array(2, 3, 4),
          assignment_exclusivity = Some(10),
          target_synchronizer_id = someSynchronizerId2,
          reassignment_counter = 234,
          contract_id = hashCid("23"),
          internal_contract_id = Some(3),
          template_id = someTemplateId,
          package_id = somePackageId,
          stakeholders = someParties("1", "2", "3"),
        )
        .toList should contain theSameElementsInOrderAs List(
        DbDto.EventDeactivate(
          event_offset = 1,
          update_id = updateIdByteArray,
          workflow_id = Some("w"),
          command_id = Some("c"),
          submitters = Some(Set(someParty)),
          record_time = 2,
          synchronizer_id = someSynchronizerId,
          trace_context = serializableTraceContext,
          external_transaction_hash = None,
          event_type = PersistentEventType.Unassign.asInt,
          event_sequential_id = 3,
          node_id = 4,
          deactivated_event_sequential_id = Some(10),
          additional_witnesses = None,
          exercise_choice = None,
          exercise_choice_interface_id = None,
          exercise_argument = None,
          exercise_result = None,
          exercise_actors = None,
          exercise_last_descendant_node_id = None,
          exercise_argument_compression = None,
          exercise_result_compression = None,
          reassignment_id = Some(Array(2, 3, 4)),
          assignment_exclusivity = Some(10),
          target_synchronizer_id = Some(someSynchronizerId2),
          reassignment_counter = Some(234),
          contract_id = hashCid("23"),
          internal_contract_id = Some(3),
          template_id = someTemplateId,
          package_id = somePackageId,
          stakeholders = someParties("1", "2", "3"),
          ledger_effective_time = None,
        ),
        DbDto.IdFilterDeactivateStakeholder(
          IdFilter(
            event_sequential_id = 3,
            template_id = someTemplateId,
            party_id = Ref.Party.assertFromString("1"),
            first_per_sequential_id = true,
          )
        ),
        DbDto.IdFilterDeactivateStakeholder(
          IdFilter(
            event_sequential_id = 3,
            template_id = someTemplateId,
            party_id = Ref.Party.assertFromString("2"),
            first_per_sequential_id = false,
          )
        ),
        DbDto.IdFilterDeactivateStakeholder(
          IdFilter(
            event_sequential_id = 3,
            template_id = someTemplateId,
            party_id = Ref.Party.assertFromString("3"),
            first_per_sequential_id = false,
          )
        ),
      )
    }
  }

  "DbDto.witnessedExercisedDbDtos" should {
    "populate correct DbDtos for witnessed consuming exercise" in {
      DbDto
        .witnessedExercisedDbDtos(
          event_offset = 1,
          update_id = updateIdByteArray,
          workflow_id = Some("w"),
          command_id = Some("c"),
          submitters = Some(Set(someParty)),
          record_time = 2,
          synchronizer_id = someSynchronizerId,
          trace_context = serializableTraceContext,
          external_transaction_hash = Some(someExternalTransactionHashBinary),
          event_sequential_id = 3,
          node_id = 4,
          additional_witnesses = Set(someParty2),
          consuming = true,
          exercise_choice = someChoice,
          exercise_choice_interface_id = Some(someInterfaceId),
          exercise_argument = Array(1, 2, 3),
          exercise_result = Some(Array(1, 2, 3, 4)),
          exercise_actors = Set(someParty5),
          exercise_last_descendant_node_id = 10,
          exercise_argument_compression = Some(1),
          exercise_result_compression = Some(2),
          contract_id = hashCid("23"),
          internal_contract_id = Some(3),
          template_id = someTemplateId,
          package_id = somePackageId,
          ledger_effective_time = 13,
        )
        .toList should contain theSameElementsInOrderAs List(
        DbDto.EventVariousWitnessed(
          event_offset = 1,
          update_id = updateIdByteArray,
          workflow_id = Some("w"),
          command_id = Some("c"),
          submitters = Some(Set(someParty)),
          record_time = 2,
          synchronizer_id = someSynchronizerId,
          trace_context = serializableTraceContext,
          external_transaction_hash = Some(someExternalTransactionHashBinary),
          event_type = PersistentEventType.WitnessedConsumingExercise.asInt,
          event_sequential_id = 3,
          node_id = 4,
          additional_witnesses = Set(someParty2),
          consuming = Some(true),
          exercise_choice = Some(someChoice),
          exercise_choice_interface_id = Some(someInterfaceId),
          exercise_argument = Some(Array(1, 2, 3)),
          exercise_result = Some(Array(1, 2, 3, 4)),
          exercise_actors = Some(Set(someParty5)),
          exercise_last_descendant_node_id = Some(10),
          exercise_argument_compression = Some(1),
          exercise_result_compression = Some(2),
          representative_package_id = None,
          contract_id = Some(hashCid("23")),
          internal_contract_id = Some(3),
          template_id = Some(someTemplateId),
          package_id = Some(somePackageId),
          ledger_effective_time = Some(13),
        ),
        DbDto.IdFilterVariousWitness(
          IdFilter(
            event_sequential_id = 3,
            template_id = someTemplateId,
            party_id = someParty2,
            first_per_sequential_id = true,
          )
        ),
      )
    }
  }

  "DbDto.witnessedExercisedDbDtos" should {
    "populate correct DbDtos for witnessed non consuming exercise" in {
      DbDto
        .witnessedExercisedDbDtos(
          event_offset = 1,
          update_id = updateIdByteArray,
          workflow_id = Some("w"),
          command_id = Some("c"),
          submitters = Some(Set(someParty)),
          record_time = 2,
          synchronizer_id = someSynchronizerId,
          trace_context = serializableTraceContext,
          external_transaction_hash = Some(someExternalTransactionHashBinary),
          event_sequential_id = 3,
          node_id = 4,
          additional_witnesses = Set(someParty2),
          consuming = false,
          exercise_choice = someChoice,
          exercise_choice_interface_id = Some(someInterfaceId),
          exercise_argument = Array(1, 2, 3),
          exercise_result = Some(Array(1, 2, 3, 4)),
          exercise_actors = Set(someParty5),
          exercise_last_descendant_node_id = 10,
          exercise_argument_compression = Some(1),
          exercise_result_compression = Some(2),
          contract_id = hashCid("23"),
          internal_contract_id = Some(3),
          template_id = someTemplateId,
          package_id = somePackageId,
          ledger_effective_time = 13,
        )
        .toList should contain theSameElementsInOrderAs List(
        DbDto.EventVariousWitnessed(
          event_offset = 1,
          update_id = updateIdByteArray,
          workflow_id = Some("w"),
          command_id = Some("c"),
          submitters = Some(Set(someParty)),
          record_time = 2,
          synchronizer_id = someSynchronizerId,
          trace_context = serializableTraceContext,
          external_transaction_hash = Some(someExternalTransactionHashBinary),
          event_type = PersistentEventType.NonConsumingExercise.asInt,
          event_sequential_id = 3,
          node_id = 4,
          additional_witnesses = Set(someParty2),
          consuming = Some(false),
          exercise_choice = Some(someChoice),
          exercise_choice_interface_id = Some(someInterfaceId),
          exercise_argument = Some(Array(1, 2, 3)),
          exercise_result = Some(Array(1, 2, 3, 4)),
          exercise_actors = Some(Set(someParty5)),
          exercise_last_descendant_node_id = Some(10),
          exercise_argument_compression = Some(1),
          exercise_result_compression = Some(2),
          representative_package_id = None,
          contract_id = Some(hashCid("23")),
          internal_contract_id = Some(3),
          template_id = Some(someTemplateId),
          package_id = Some(somePackageId),
          ledger_effective_time = Some(13),
        ),
        DbDto.IdFilterVariousWitness(
          IdFilter(
            event_sequential_id = 3,
            template_id = someTemplateId,
            party_id = someParty2,
            first_per_sequential_id = true,
          )
        ),
      )
    }
  }

  "DbDto.witnessedCreateDbDtos" should {
    "populate correct DbDtos" in {
      DbDto
        .witnessedCreateDbDtos(
          event_offset = 1,
          update_id = updateIdByteArray,
          workflow_id = Some("w"),
          command_id = Some("c"),
          submitters = Some(Set(someParty)),
          record_time = 2,
          synchronizer_id = someSynchronizerId,
          trace_context = serializableTraceContext,
          external_transaction_hash = Some(someExternalTransactionHashBinary),
          event_sequential_id = 3,
          node_id = 4,
          additional_witnesses = Set(someParty2),
          representative_package_id = someRepresentativePackageId,
          internal_contract_id = 3,
        )(template_id = someTemplateId)
        .toList should contain theSameElementsInOrderAs List(
        DbDto.EventVariousWitnessed(
          event_offset = 1,
          update_id = updateIdByteArray,
          workflow_id = Some("w"),
          command_id = Some("c"),
          submitters = Some(Set(someParty)),
          record_time = 2,
          synchronizer_id = someSynchronizerId,
          trace_context = serializableTraceContext,
          external_transaction_hash = Some(someExternalTransactionHashBinary),
          event_type = PersistentEventType.WitnessedCreate.asInt,
          event_sequential_id = 3,
          node_id = 4,
          additional_witnesses = Set(someParty2),
          consuming = None,
          exercise_choice = None,
          exercise_choice_interface_id = None,
          exercise_argument = None,
          exercise_result = None,
          exercise_actors = None,
          exercise_last_descendant_node_id = None,
          exercise_argument_compression = None,
          exercise_result_compression = None,
          representative_package_id = Some(someRepresentativePackageId),
          contract_id = None,
          internal_contract_id = Some(3),
          template_id = None,
          package_id = None,
          ledger_effective_time = None,
        ),
        DbDto.IdFilterVariousWitness(
          IdFilter(
            event_sequential_id = 3,
            template_id = someTemplateId,
            party_id = someParty2,
            first_per_sequential_id = true,
          )
        ),
      )
    }
  }
}
