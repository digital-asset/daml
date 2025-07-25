// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.SerializableTraceContextExtension
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
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
    ).sorted
    iterators.synchronizerIds.toList.sorted shouldBe List(
      "synchronizer2",
      "synchronizer3",
      "synchronizer4",
      "synchronizer5",
      "synchronizer6",
      "synchronizer7",
      "synchronizer8",
      "synchronizer9",
    ).sorted
    iterators.packageNames.toList.sorted shouldBe List(
      "25.1",
      "50.1",
      "87.1",
      "94.1",
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
      update_id = "16",
      ledger_effective_time = 1,
      command_id = Some("17"),
      workflow_id = Some("18"),
      user_id = Some("19"),
      submitters = Some(Set("20", "21", "22")),
      node_id = 1,
      contract_id = Array(24),
      template_id = "25",
      package_name = "25.1",
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
      driver_metadata = Array.empty,
      synchronizer_id = "synchronizer2",
      trace_context = serializableTraceContext,
      record_time = 1,
      external_transaction_hash = Some(externalTransactionHash),
    ),
    DbDto.EventExercise(
      consuming = true,
      event_offset = 40,
      update_id = "41",
      ledger_effective_time = 1,
      command_id = Some("42"),
      workflow_id = Some("43"),
      user_id = Some("44"),
      submitters = Some(Set("45", "46", "47")),
      node_id = 1,
      contract_id = Array(49),
      template_id = "50",
      package_name = "50.1",
      flat_event_witnesses = Set("51", "52", "53"),
      tree_event_witnesses = Set("54", "55", "56"),
      exercise_argument = Array.empty,
      exercise_actors = Set("57", "58", "59"),
      create_key_value = None,
      exercise_argument_compression = Some(1),
      create_key_value_compression = Some(1),
      event_sequential_id = 1,
      exercise_choice = "60",
      exercise_result = None,
      exercise_last_descendant_node_id = 63,
      exercise_result_compression = Some(1),
      synchronizer_id = "synchronizer3",
      trace_context = serializableTraceContext,
      record_time = 1,
      external_transaction_hash = Some(externalTransactionHash),
    ),
    DbDto.CommandCompletion(
      completion_offset = 64,
      record_time = 2,
      publication_time = 0,
      user_id = "65",
      submitters = Set("66", "67", "68"),
      command_id = "69",
      update_id = Some("70"),
      rejection_status_code = Some(1),
      rejection_status_message = Some("71"),
      rejection_status_details = None,
      submission_id = Some("72"),
      deduplication_offset = Some(73),
      deduplication_duration_seconds = Some(1),
      deduplication_duration_nanos = Some(1),
      synchronizer_id = "synchronizer4",
      message_uuid = None,
      is_transaction = true,
      trace_context = serializableTraceContext,
    ),
    DbDto.EventAssign(
      event_offset = 1,
      update_id = "",
      command_id = None,
      workflow_id = None,
      submitter = Option("s1"),
      node_id = 0,
      contract_id = Array(114),
      template_id = "87",
      package_name = "87.1",
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
      driver_metadata = Array.empty,
      source_synchronizer_id = "synchronizer5",
      target_synchronizer_id = "synchronizer6",
      reassignment_id = "",
      reassignment_counter = 0,
      trace_context = serializableTraceContext,
      record_time = 0,
    ),
    DbDto.EventUnassign(
      event_offset = 1,
      update_id = "",
      command_id = None,
      workflow_id = None,
      submitter = Option("s2"),
      node_id = 0,
      contract_id = Array(115),
      template_id = "94",
      package_name = "94.1",
      flat_event_witnesses = Set("95", "96"),
      event_sequential_id = 0,
      source_synchronizer_id = "synchronizer7",
      target_synchronizer_id = "synchronizer8",
      reassignment_id = "",
      reassignment_counter = 0,
      assignment_exclusivity = None,
      trace_context = serializableTraceContext,
      record_time = 0,
    ),
    DbDto.SequencerIndexMoved("synchronizer9"),
  )

}
