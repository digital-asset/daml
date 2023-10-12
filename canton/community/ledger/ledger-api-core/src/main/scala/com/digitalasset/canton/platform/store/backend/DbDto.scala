// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.scalautil.NeverEqualsOverride

sealed trait DbDto
    extends NeverEqualsOverride
    with Product
    with Serializable // to aid type inference for case class implementors

object DbDto {

  final case class EventDivulgence(
      event_offset: Option[String],
      command_id: Option[String],
      workflow_id: Option[String],
      application_id: Option[String],
      submitters: Option[Set[String]],
      contract_id: String,
      template_id: Option[String],
      tree_event_witnesses: Set[String],
      create_argument: Option[Array[Byte]],
      create_argument_compression: Option[Int],
      event_sequential_id: Long,
      domain_id: Option[String] = None,
  ) extends DbDto

  final case class EventCreate(
      event_offset: Option[String],
      transaction_id: Option[String],
      ledger_effective_time: Option[Long],
      command_id: Option[String],
      workflow_id: Option[String],
      application_id: Option[String],
      submitters: Option[Set[String]],
      node_index: Option[Int],
      event_id: Option[String],
      contract_id: String,
      template_id: Option[String],
      flat_event_witnesses: Set[String],
      tree_event_witnesses: Set[String],
      create_argument: Option[Array[Byte]],
      create_signatories: Option[Set[String]],
      create_observers: Option[Set[String]],
      create_agreement_text: Option[String],
      create_key_value: Option[Array[Byte]],
      create_key_maintainers: Option[Set[String]],
      create_key_hash: Option[String],
      create_argument_compression: Option[Int],
      create_key_value_compression: Option[Int],
      event_sequential_id: Long,
      driver_metadata: Option[Array[Byte]],
      domain_id: Option[String] = None,
      trace_context: Array[Byte],
  ) extends DbDto

  final case class EventExercise(
      consuming: Boolean,
      event_offset: Option[String],
      transaction_id: Option[String],
      ledger_effective_time: Option[Long],
      command_id: Option[String],
      workflow_id: Option[String],
      application_id: Option[String],
      submitters: Option[Set[String]],
      node_index: Option[Int],
      event_id: Option[String],
      contract_id: String,
      template_id: Option[String],
      flat_event_witnesses: Set[String],
      tree_event_witnesses: Set[String],
      create_key_value: Option[Array[Byte]],
      exercise_choice: Option[String],
      exercise_argument: Option[Array[Byte]],
      exercise_result: Option[Array[Byte]],
      exercise_actors: Option[Set[String]],
      exercise_child_event_ids: Option[Vector[String]],
      create_key_value_compression: Option[Int],
      exercise_argument_compression: Option[Int],
      exercise_result_compression: Option[Int],
      event_sequential_id: Long,
      domain_id: Option[String] = None,
      trace_context: Array[Byte],
  ) extends DbDto

  final case class EventAssign(
      event_offset: String,
      update_id: String,
      command_id: Option[String],
      workflow_id: Option[String],
      submitter: Option[String],
      contract_id: String,
      template_id: String,
      flat_event_witnesses: Set[String],
      create_argument: Array[Byte],
      create_signatories: Set[String],
      create_observers: Set[String],
      create_agreement_text: Option[String],
      create_key_value: Option[Array[Byte]],
      create_key_maintainers: Option[Set[String]],
      create_key_hash: Option[String],
      create_argument_compression: Option[Int],
      create_key_value_compression: Option[Int],
      event_sequential_id: Long,
      ledger_effective_time: Long,
      driver_metadata: Array[Byte],
      source_domain_id: String,
      target_domain_id: String,
      unassign_id: String,
      reassignment_counter: Long,
      trace_context: Array[Byte],
  ) extends DbDto

  final case class EventUnassign(
      event_offset: String,
      update_id: String,
      command_id: Option[String],
      workflow_id: Option[String],
      submitter: Option[String],
      contract_id: String,
      template_id: String,
      flat_event_witnesses: Set[String],
      event_sequential_id: Long,
      source_domain_id: String,
      target_domain_id: String,
      unassign_id: String,
      reassignment_counter: Long,
      assignment_exclusivity: Option[Long],
      trace_context: Array[Byte],
  ) extends DbDto

  final case class ConfigurationEntry(
      ledger_offset: String,
      recorded_at: Long,
      submission_id: String,
      typ: String,
      configuration: Array[Byte],
      rejection_reason: Option[String],
  ) extends DbDto

  final case class PackageEntry(
      ledger_offset: String,
      recorded_at: Long,
      submission_id: Option[String],
      typ: String,
      rejection_reason: Option[String],
  ) extends DbDto

  final case class Package(
      package_id: String,
      upload_id: String,
      source_description: Option[String],
      package_size: Long,
      known_since: Long,
      ledger_offset: String,
      _package: Array[Byte],
  ) extends DbDto

  final case class PartyEntry(
      ledger_offset: String,
      recorded_at: Long,
      submission_id: Option[String],
      party: Option[String],
      display_name: Option[String],
      typ: String,
      rejection_reason: Option[String],
      is_local: Option[Boolean],
  ) extends DbDto

  final case class CommandCompletion(
      completion_offset: String,
      record_time: Long,
      application_id: String,
      submitters: Set[String],
      command_id: String,
      transaction_id: Option[String],
      rejection_status_code: Option[Int],
      rejection_status_message: Option[String],
      rejection_status_details: Option[Array[Byte]],
      submission_id: Option[String],
      deduplication_offset: Option[String],
      deduplication_duration_seconds: Option[Long],
      deduplication_duration_nanos: Option[Int],
      deduplication_start: Option[Long],
      domain_id: Option[String] = None,
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
      party_id: String,
  ) extends DbDto

  final case class IdFilterConsumingStakeholder(
      event_sequential_id: Long,
      template_id: String,
      party_id: String,
  ) extends DbDto

  final case class IdFilterConsumingNonStakeholderInformee(
      event_sequential_id: Long,
      party_id: String,
  ) extends DbDto

  final case class IdFilterNonConsumingInformee(
      event_sequential_id: Long,
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
      transaction_id: String,
      event_offset: String,
      event_sequential_id_first: Long,
      event_sequential_id_last: Long,
  ) extends DbDto

  final case class TransactionMetering(
      application_id: String,
      action_count: Int,
      metering_timestamp: Long,
      ledger_offset: String,
  ) extends DbDto
}
