// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.time.Instant

import com.daml.scalautil.NeverEqualsOverride

sealed trait DBDTOV1
    extends NeverEqualsOverride
    with Product
    with Serializable // to aid type inference for case class implementors

object DBDTOV1 {

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
  ) extends DBDTOV1

  final case class EventCreate(
      event_offset: Option[String],
      transaction_id: Option[String],
      ledger_effective_time: Option[Instant],
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
      create_key_hash: Option[String],
      create_argument_compression: Option[Int],
      create_key_value_compression: Option[Int],
      event_sequential_id: Long,
  ) extends DBDTOV1

  final case class EventExercise(
      consuming: Boolean,
      event_offset: Option[String],
      transaction_id: Option[String],
      ledger_effective_time: Option[Instant],
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
      exercise_child_event_ids: Option[Set[String]],
      create_key_value_compression: Option[Int],
      exercise_argument_compression: Option[Int],
      exercise_result_compression: Option[Int],
      event_sequential_id: Long,
  ) extends DBDTOV1

  final case class ConfigurationEntry(
      ledger_offset: String,
      recorded_at: Instant,
      submission_id: String,
      typ: String,
      configuration: Array[Byte],
      rejection_reason: Option[String],
  ) extends DBDTOV1

  final case class PackageEntry(
      ledger_offset: String,
      recorded_at: Instant,
      submission_id: Option[String],
      typ: String,
      rejection_reason: Option[String],
  ) extends DBDTOV1

  final case class Package(
      package_id: String,
      upload_id: String,
      source_description: Option[String],
      size: Long,
      known_since: Instant,
      ledger_offset: String,
      _package: Array[Byte],
  ) extends DBDTOV1

  final case class PartyEntry(
      ledger_offset: String,
      recorded_at: Instant,
      submission_id: Option[String],
      party: Option[String],
      display_name: Option[String],
      typ: String,
      rejection_reason: Option[String],
      is_local: Option[Boolean],
  ) extends DBDTOV1

  final case class Party(
      party: String,
      display_name: Option[String],
      explicit: Boolean,
      ledger_offset: Option[String],
      is_local: Boolean,
  ) extends DBDTOV1

  final case class CommandCompletion(
      completion_offset: String,
      record_time: Instant,
      application_id: String,
      submitters: Set[String],
      command_id: String,
      transaction_id: Option[String],
      status_code: Option[Int],
      status_message: Option[String],
  ) extends DBDTOV1

  final case class CommandDeduplication(deduplication_key: String) extends DBDTOV1

}
