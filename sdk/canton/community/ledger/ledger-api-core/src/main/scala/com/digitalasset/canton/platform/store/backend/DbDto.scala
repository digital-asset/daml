// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.scalautil.NeverEqualsOverride

sealed trait DbDto
    extends NeverEqualsOverride
    with Product
    with Serializable // to aid type inference for case class implementors

object DbDto {

  final case class EventCreate(
      event_offset: Long,
      update_id: String,
      ledger_effective_time: Long,
      command_id: Option[String],
      workflow_id: Option[String],
      user_id: Option[String],
      submitters: Option[Set[String]],
      node_id: Int,
      contract_id: Array[Byte],
      template_id: String,
      package_name: String,
      package_version: Option[String],
      flat_event_witnesses: Set[String],
      tree_event_witnesses: Set[String],
      create_argument: Array[Byte],
      create_signatories: Set[String],
      create_observers: Set[String],
      create_key_value: Option[Array[Byte]],
      create_key_maintainers: Option[Set[String]],
      create_key_hash: Option[String],
      create_argument_compression: Option[Int],
      create_key_value_compression: Option[Int],
      event_sequential_id: Long,
      driver_metadata: Array[Byte],
      synchronizer_id: String,
      trace_context: Array[Byte],
      record_time: Long,
  ) extends DbDto

  final case class EventExercise(
      consuming: Boolean,
      event_offset: Long,
      update_id: String,
      ledger_effective_time: Long,
      command_id: Option[String],
      workflow_id: Option[String],
      user_id: Option[String],
      submitters: Option[Set[String]],
      node_id: Int,
      contract_id: Array[Byte],
      template_id: String,
      package_name: String,
      flat_event_witnesses: Set[
        String
      ], // only for consuming, for non-consuming exercise this field is omitted
      tree_event_witnesses: Set[String],
      create_key_value: Option[Array[Byte]],
      exercise_choice: String,
      exercise_argument: Array[Byte],
      exercise_result: Option[Array[Byte]],
      exercise_actors: Set[String],
      exercise_last_descendant_node_id: Int,
      create_key_value_compression: Option[Int],
      exercise_argument_compression: Option[Int],
      exercise_result_compression: Option[Int],
      event_sequential_id: Long,
      synchronizer_id: String,
      trace_context: Array[Byte],
      record_time: Long,
  ) extends DbDto

  final case class EventAssign(
      event_offset: Long,
      update_id: String,
      command_id: Option[String],
      workflow_id: Option[String],
      submitter: Option[String],
      node_id: Int,
      contract_id: Array[Byte],
      template_id: String,
      package_name: String,
      package_version: Option[String],
      flat_event_witnesses: Set[String],
      create_argument: Array[Byte],
      create_signatories: Set[String],
      create_observers: Set[String],
      create_key_value: Option[Array[Byte]],
      create_key_maintainers: Option[Set[String]],
      create_key_hash: Option[String],
      create_argument_compression: Option[Int],
      create_key_value_compression: Option[Int],
      event_sequential_id: Long,
      ledger_effective_time: Long,
      driver_metadata: Array[Byte],
      source_synchronizer_id: String,
      target_synchronizer_id: String,
      unassign_id: String,
      reassignment_counter: Long,
      trace_context: Array[Byte],
      record_time: Long,
  ) extends DbDto

  final case class EventUnassign(
      event_offset: Long,
      update_id: String,
      command_id: Option[String],
      workflow_id: Option[String],
      submitter: Option[String],
      node_id: Int,
      contract_id: Array[Byte],
      template_id: String,
      package_name: String,
      flat_event_witnesses: Set[String],
      event_sequential_id: Long,
      source_synchronizer_id: String,
      target_synchronizer_id: String,
      unassign_id: String,
      reassignment_counter: Long,
      assignment_exclusivity: Option[Long],
      trace_context: Array[Byte],
      record_time: Long,
  ) extends DbDto

  final case class EventPartyToParticipant(
      event_sequential_id: Long,
      event_offset: Long,
      update_id: String,
      party_id: String,
      participant_id: String,
      participant_permission: Int,
      synchronizer_id: String,
      record_time: Long,
      trace_context: Array[Byte],
  ) extends DbDto

  final case class PartyEntry(
      ledger_offset: Long,
      recorded_at: Long,
      submission_id: Option[String],
      party: Option[String],
      typ: String,
      rejection_reason: Option[String],
      is_local: Option[Boolean],
  ) extends DbDto

  final case class CommandCompletion(
      completion_offset: Long,
      record_time: Long,
      publication_time: Long,
      user_id: String,
      submitters: Set[String],
      command_id: String,
      update_id: Option[String],
      rejection_status_code: Option[Int],
      rejection_status_message: Option[String],
      rejection_status_details: Option[Array[Byte]],
      submission_id: Option[String],
      deduplication_offset: Option[Long],
      deduplication_duration_seconds: Option[Long],
      deduplication_duration_nanos: Option[Int],
      synchronizer_id: String,
      message_uuid: Option[String],
      is_transaction: Boolean,
      trace_context: Array[Byte],
  ) extends DbDto

  final case class StringInterningDto(
      internalId: Int,
      externalString: String,
  ) extends DbDto

  object StringInterningDto {
    def from(entry: (Int, String)): StringInterningDto =
      StringInterningDto(entry._1, entry._2)
  }

  final case class IdFilterCreateStakeholder(
      event_sequential_id: Long,
      template_id: String,
      party_id: String,
  ) extends DbDto

  final case class IdFilterCreateNonStakeholderInformee(
      event_sequential_id: Long,
      template_id: String,
      party_id: String,
  ) extends DbDto

  final case class IdFilterConsumingStakeholder(
      event_sequential_id: Long,
      template_id: String,
      party_id: String,
  ) extends DbDto

  final case class IdFilterConsumingNonStakeholderInformee(
      event_sequential_id: Long,
      template_id: String,
      party_id: String,
  ) extends DbDto

  final case class IdFilterNonConsumingInformee(
      event_sequential_id: Long,
      template_id: String,
      party_id: String,
  ) extends DbDto

  final case class IdFilterUnassignStakeholder(
      event_sequential_id: Long,
      template_id: String,
      party_id: String,
  ) extends DbDto

  final case class IdFilterAssignStakeholder(
      event_sequential_id: Long,
      template_id: String,
      party_id: String,
  ) extends DbDto

  final case class TransactionMeta(
      update_id: String,
      event_offset: Long,
      publication_time: Long,
      record_time: Long,
      synchronizer_id: String,
      event_sequential_id_first: Long,
      event_sequential_id_last: Long,
  ) extends DbDto

  final case class TransactionMetering(
      user_id: String,
      action_count: Int,
      metering_timestamp: Long,
      ledger_offset: Long,
  ) extends DbDto

  final case class SequencerIndexMoved(synchronizerId: String) extends DbDto
}
