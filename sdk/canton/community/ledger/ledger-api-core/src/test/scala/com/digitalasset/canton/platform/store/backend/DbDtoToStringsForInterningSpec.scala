// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.SerializableTraceContextExtension
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DbDtoToStringsForInterningSpec extends AnyFlatSpec with Matchers {

  it should "select all relevant strings for interning" in {
    val iterators = DbDtoToStringsForInterning(fixture)
    iterators.templateIds.toList.sorted shouldBe List(
      "25",
      "50",
      "87",
      "94",
    ).sorted
    iterators.parties.toList.sorted shouldBe List(
      "2",
      "20",
      "21",
      "22",
      "26",
      "27",
      "28",
      "29",
      "30",
      "31",
      "32",
      "33",
      "34",
      "35",
      "36",
      "37",
      "45",
      "46",
      "47",
      "51",
      "52",
      "53",
      "54",
      "55",
      "56",
      "57",
      "58",
      "59",
      "66",
      "67",
      "68",
      "s1",
      "88",
      "89",
      "90",
      "91",
      "92",
      "93",
      "s2",
      "95",
      "96",
      "97",
    ).sorted
    iterators.synchronizerIds.toList.map(_.toProtoPrimitive).sorted shouldBe List(
      "x::synchronizer2",
      "x::synchronizer3",
      "x::synchronizer4",
      "x::synchronizer5",
      "x::synchronizer6",
      "x::synchronizer7",
      "x::synchronizer8",
      "x::synchronizer9",
      "x::synchronizer10",
    ).sorted
    iterators.packageIds.toList.sorted shouldBe List(
      "25.1",
      "25.2",
      "50.1",
      "87.1",
      "94.1",
    ).sorted
    iterators.userIds.toList.sorted shouldBe List(
      "65"
    ).sorted
    iterators.participantIds.toList.sorted shouldBe List(
      "participant1"
    ).sorted
    iterators.choiceNames.toList.sorted shouldBe List(
      "60"
    ).sorted
    iterators.interfaceIds.toList.sorted shouldBe List(
      "61"
    ).sorted
  }

  private val serializableTraceContext =
    SerializableTraceContext(TraceContext.empty).toDamlProto.toByteArray

  private val externalTransactionHash =
    Hash
      .digest(HashPurpose.PreparedSubmission, ByteString.copyFromUtf8("mock_hash"), Sha256)
      .unwrap
      .toByteArray

  private def fixture: List[DbDto] = List(
    DbDto.PartyEntry(
      ledger_offset = 1,
      recorded_at = 0,
      submission_id = Some("1"),
      party = Some("2"),
      typ = "3",
      rejection_reason = Some("4"),
      is_local = None,
    ),
    DbDto.StringInterningDto(
      internalId = 1,
      externalString = "5",
    ),
    DbDto.EventCreate(
      event_offset = 15,
      update_id = updateId("16"),
      ledger_effective_time = 1,
      command_id = Some("17"),
      workflow_id = Some("18"),
      user_id = Some("19"),
      submitters = Some(Set("20", "21", "22")),
      node_id = 1,
      contract_id = hashCid("24"),
      template_id = "25",
      package_id = "25.1",
      representative_package_id = "25.2",
      flat_event_witnesses = Set("26", "27", "28"),
      tree_event_witnesses = Set("29", "30", "31"),
      create_argument = Array.empty,
      create_signatories = Set("32", "33", "34"),
      create_observers = Set("35", "36", "37"),
      create_key_value = None,
      create_key_maintainers = Some(Set("32", "33")),
      create_key_hash = Some("39"),
      create_argument_compression = Some(1),
      create_key_value_compression = Some(1),
      event_sequential_id = 1,
      authentication_data = Array.empty,
      synchronizer_id = SynchronizerId.tryFromString("x::synchronizer2"),
      trace_context = serializableTraceContext,
      record_time = 1,
      external_transaction_hash = Some(externalTransactionHash),
      internal_contract_id = 101,
    ),
    DbDto.EventExercise(
      consuming = true,
      event_offset = 40,
      update_id = updateId("41"),
      ledger_effective_time = 1,
      command_id = Some("42"),
      workflow_id = Some("43"),
      user_id = Some("44"),
      submitters = Some(Set("45", "46", "47")),
      node_id = 1,
      contract_id = hashCid("49"),
      template_id = "50",
      package_id = "50.1",
      flat_event_witnesses = Set("51", "52", "53"),
      tree_event_witnesses = Set("54", "55", "56"),
      exercise_argument = Array.empty,
      exercise_actors = Set("57", "58", "59"),
      exercise_argument_compression = Some(1),
      event_sequential_id = 1,
      exercise_choice = "60",
      exercise_choice_interface_id = Some("61"),
      exercise_result = None,
      exercise_last_descendant_node_id = 63,
      exercise_result_compression = Some(1),
      synchronizer_id = SynchronizerId.tryFromString("x::synchronizer3"),
      trace_context = serializableTraceContext,
      record_time = 1,
      external_transaction_hash = Some(externalTransactionHash),
      deactivated_event_sequential_id = None,
    ),
    DbDto.CommandCompletion(
      completion_offset = 64,
      record_time = 2,
      publication_time = 0,
      user_id = "65",
      submitters = Set("66", "67", "68"),
      command_id = "69",
      update_id = Some(updateId("70")),
      rejection_status_code = Some(1),
      rejection_status_message = Some("71"),
      rejection_status_details = None,
      submission_id = Some("72"),
      deduplication_offset = Some(73),
      deduplication_duration_seconds = Some(1),
      deduplication_duration_nanos = Some(1),
      synchronizer_id = SynchronizerId.tryFromString("x::synchronizer4"),
      message_uuid = None,
      is_transaction = true,
      trace_context = serializableTraceContext,
    ),
    DbDto.EventAssign(
      event_offset = 1,
      update_id = updateId(""),
      command_id = None,
      workflow_id = None,
      submitter = Option("s1"),
      node_id = 0,
      contract_id = hashCid("114"),
      template_id = "87",
      package_id = "87.1",
      flat_event_witnesses = Set("88", "89"),
      create_argument = Array.empty,
      create_signatories = Set("90", "91"),
      create_observers = Set("92", "93"),
      create_key_value = None,
      create_key_maintainers = Some(Set("91")),
      create_key_hash = None,
      create_argument_compression = None,
      create_key_value_compression = None,
      event_sequential_id = 0,
      ledger_effective_time = 0,
      authentication_data = Array.empty,
      source_synchronizer_id = SynchronizerId.tryFromString("x::synchronizer5"),
      target_synchronizer_id = SynchronizerId.tryFromString("x::synchronizer6"),
      reassignment_id = "",
      reassignment_counter = 0,
      trace_context = serializableTraceContext,
      record_time = 0,
      internal_contract_id = 102,
    ),
    DbDto.EventUnassign(
      event_offset = 1,
      update_id = updateId(""),
      command_id = None,
      workflow_id = None,
      submitter = Option("s2"),
      node_id = 0,
      contract_id = hashCid("115"),
      template_id = "94",
      package_id = "94.1",
      flat_event_witnesses = Set("95", "96"),
      event_sequential_id = 0,
      source_synchronizer_id = SynchronizerId.tryFromString("x::synchronizer7"),
      target_synchronizer_id = SynchronizerId.tryFromString("x::synchronizer8"),
      reassignment_id = "",
      reassignment_counter = 0,
      assignment_exclusivity = None,
      trace_context = serializableTraceContext,
      record_time = 0,
      deactivated_event_sequential_id = None,
    ),
    DbDto.SequencerIndexMoved(SynchronizerId.tryFromString("x::synchronizer9")),
    DbDto.EventPartyToParticipant(
      event_sequential_id = 0,
      event_offset = 1,
      update_id = updateId(""),
      party_id = "97",
      participant_id = "participant1",
      participant_permission = 1,
      participant_authorization_event = 2,
      synchronizer_id = SynchronizerId.tryFromString("x::synchronizer10"),
      record_time = 0,
      trace_context = Array.empty,
    ),
  )

  private def hashCid(key: String): ContractId =
    ContractId.V1(com.digitalasset.daml.lf.crypto.Hash.hashPrivateKey(key))

  private def updateId(key: String): Array[Byte] = key.getBytes
}
