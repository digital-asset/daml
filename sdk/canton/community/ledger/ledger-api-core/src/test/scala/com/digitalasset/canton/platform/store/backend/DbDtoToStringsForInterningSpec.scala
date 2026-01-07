// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.platform.store.backend.StorageBackendTestValues.someParticipant
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.SerializableTraceContextExtension
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{
  ChoiceName,
  Identifier,
  NameTypeConRef,
  PackageId,
  Party,
  UserId,
}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DbDtoToStringsForInterningSpec extends AnyFlatSpec with Matchers {

  it should "select all relevant strings for interning" in {
    val iterators = DbDtoToStringsForInterning(fixture)
    iterators.templateIds.map(_.toString).toList.sorted shouldBe List(
      "#p1:m1:i08",
      "#p2:m1:i09",
      "#p3:m:idt1",
      "#p3:m:idt2",
      "#p3:m:idt3",
      "#p3:m:idt4",
      "#p3:m:idt5",
    ).sorted
    iterators.parties.toList.sorted shouldBe List(
      "2",
      "3",
      "4",
      "5",
      "6",
      "7",
      "8",
      "9",
      "10",
      "11",
      "12",
      "13",
      "14",
      "15",
      "16",
      "17",
      "18",
      "19",
      "66",
      "67",
      "68",
      "97",
      "idp1",
      "idp2",
      "idp3",
      "idp4",
      "idp5",
    ).sorted
    iterators.synchronizerIds.toList.map(_.toProtoPrimitive).sorted shouldBe List(
      "x::synchronizer1",
      "x::synchronizer1b",
      "x::synchronizer1c",
      "x::synchronizer1d",
      "x::synchronizer1e",
      "x::synchronizer4",
      "x::synchronizer9",
      "x::synchronizer10",
    ).sorted
    iterators.packageIds.toList.sorted shouldBe List(
      "11_1",
      "11_2",
      "11_3",
      "11_4",
    ).sorted
    iterators.userIds.toList.sorted shouldBe List(
      "65"
    ).sorted
    iterators.participantIds.toList.sorted shouldBe List(
      "participant1"
    ).sorted
    iterators.choiceNames.toList.sorted shouldBe List(
      "c_42",
      "c_44",
    ).sorted
    iterators.interfaceIds.map(_.toString).toList.sorted shouldBe List(
      "11_2:m1:n43",
      "11_4:m1:n45",
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
      party = Some(party("2")),
      typ = "3",
      rejection_reason = Some("4"),
      is_local = None,
    ),
    DbDto.StringInterningDto(
      internalId = 1,
      externalString = "5",
    ),
    DbDto.EventActivate(
      event_offset = 10,
      update_id = updateId("10"),
      workflow_id = Some("10"),
      command_id = Some("11"),
      submitters = Some(Set("3", "4", "5").map(party)),
      record_time = 1,
      synchronizer_id = SynchronizerId.tryFromString("x::synchronizer1"),
      trace_context = serializableTraceContext,
      external_transaction_hash = Some(externalTransactionHash),
      event_type = 5,
      event_sequential_id = 1,
      node_id = 1,
      additional_witnesses = Some(Set("6", "7").map(party)),
      source_synchronizer_id = Some(SynchronizerId.tryFromString("x::synchronizer1b")),
      reassignment_counter = None,
      reassignment_id = None,
      representative_package_id = packageId("11_1"),
      notPersistedContractId = hashCid("24"),
      internal_contract_id = 55,
      create_key_hash = None,
    ),
    DbDto.EventDeactivate(
      event_offset = 11,
      update_id = updateId("11"),
      workflow_id = Some("11"),
      command_id = Some("12"),
      submitters = Some(Set("8", "9", "10").map(party)),
      record_time = 1,
      synchronizer_id = SynchronizerId.tryFromString("x::synchronizer1c"),
      trace_context = serializableTraceContext,
      external_transaction_hash = Some(externalTransactionHash),
      event_type = 5,
      event_sequential_id = 1,
      node_id = 1,
      deactivated_event_sequential_id = None,
      additional_witnesses = Some(Set("11", "12").map(party)),
      exercise_choice = Some(choice("c_42")),
      exercise_choice_interface_id = Some(interfaceId("11_2:m1:n43")),
      exercise_argument = None,
      exercise_result = None,
      exercise_actors = Some(Set(party("13"))),
      exercise_last_descendant_node_id = None,
      exercise_argument_compression = None,
      exercise_result_compression = None,
      reassignment_id = None,
      assignment_exclusivity = None,
      target_synchronizer_id = Some(SynchronizerId.tryFromString("x::synchronizer1d")),
      reassignment_counter = None,
      contract_id = hashCid("56"),
      internal_contract_id = Some(57),
      template_id = templateId("#p1:m1:i08"),
      package_id = packageId("11_2"),
      stakeholders = Set(party("14")),
      ledger_effective_time = None,
    ),
    DbDto.EventVariousWitnessed(
      event_offset = 12,
      update_id = updateId("12"),
      workflow_id = Some("12"),
      command_id = Some("13"),
      submitters = Some(Set(party("15"), party("16"))),
      record_time = 1,
      synchronizer_id = SynchronizerId.tryFromString("x::synchronizer1e"),
      trace_context = serializableTraceContext,
      external_transaction_hash = Some(externalTransactionHash),
      event_type = 5,
      event_sequential_id = 1,
      node_id = 1,
      additional_witnesses = Set(party("17"), party("18")),
      consuming = Some(false),
      exercise_choice = Some(choice("c_44")),
      exercise_choice_interface_id = Some(interfaceId("11_4:m1:n45")),
      exercise_argument = None,
      exercise_result = None,
      exercise_actors = Some(Set(party("19"))),
      exercise_last_descendant_node_id = None,
      exercise_argument_compression = None,
      exercise_result_compression = None,
      representative_package_id = Some(packageId("11_3")),
      contract_id = Some(hashCid("57")),
      internal_contract_id = Some(58),
      template_id = Some(templateId("#p2:m1:i09")),
      package_id = Some(packageId("11_4")),
      ledger_effective_time = None,
    ),
    DbDto.CommandCompletion(
      completion_offset = 64,
      record_time = 2,
      publication_time = 0,
      user_id = userId("65"),
      submitters = Set("66", "67", "68").map(Ref.Party.assertFromString),
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
    DbDto.SequencerIndexMoved(SynchronizerId.tryFromString("x::synchronizer9")),
    DbDto.EventPartyToParticipant(
      event_sequential_id = 0,
      event_offset = 1,
      update_id = updateId(""),
      party_id = Ref.Party.fromInt(97),
      participant_id = someParticipant,
      participant_permission = 1,
      participant_authorization_event = 2,
      synchronizer_id = SynchronizerId.tryFromString("x::synchronizer10"),
      record_time = 0,
      trace_context = Array.empty,
    ),
    DbDto
      .IdFilter(0, templateId("#p3:m:idt1"), party("idp1"), first_per_sequential_id = false)
      .activateStakeholder,
    DbDto
      .IdFilter(0, templateId("#p3:m:idt2"), party("idp2"), first_per_sequential_id = false)
      .activateWitness,
    DbDto
      .IdFilter(0, templateId("#p3:m:idt3"), party("idp3"), first_per_sequential_id = false)
      .deactivateStakeholder,
    DbDto
      .IdFilter(0, templateId("#p3:m:idt4"), party("idp4"), first_per_sequential_id = false)
      .deactivateWitness,
    DbDto
      .IdFilter(0, templateId("#p3:m:idt5"), party("idp5"), first_per_sequential_id = false)
      .variousWitness,
  )

  private def templateId(id: String): NameTypeConRef = NameTypeConRef.assertFromString(id)

  private def interfaceId(id: String): Identifier = Identifier.assertFromString(id)

  private def party(id: String): Party = Party.assertFromString(id)

  private def userId(id: String): UserId = UserId.assertFromString(id)

  private def packageId(id: String): PackageId = PackageId.assertFromString(id)

  private def choice(name: String): ChoiceName = ChoiceName.assertFromString(name)

  private def hashCid(key: String): ContractId =
    ContractId.V1(com.digitalasset.daml.lf.crypto.Hash.hashPrivateKey(key))

  private def updateId(key: String): Array[Byte] = key.getBytes
}
