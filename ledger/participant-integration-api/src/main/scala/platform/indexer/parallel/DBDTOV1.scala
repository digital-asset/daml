// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import java.time.Instant

trait DBDTOV1

object DBDTOV1 {

  case class EventDivulgence(
      event_offset: Option[Array[Byte]],
      command_id: Option[String],
      workflow_id: Option[String],
      application_id: Option[String],
      submitters: Option[Set[String]],
      contract_id: String,
      template_id: Option[String],
      tree_event_witnesses: Set[String],
      create_argument: Option[Array[Byte]],
      create_argument_compression: Option[Int],
      // missing: event_sequential_id: Long - this will be assigned only at batches
  ) extends DBDTOV1

  case class EventCreate(
      event_offset: Option[Array[Byte]],
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
      create_key_hash: Option[Array[Byte]],
      create_argument_compression: Option[Int],
      create_key_value_compression: Option[Int],
      // missing: event_sequential_id: Long - this will be assigned only at batches
  ) extends DBDTOV1

  case class EventExercise(
      consuming: Boolean,
      event_offset: Option[Array[Byte]],
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
      exercise_choice: Option[String],
      exercise_argument: Option[Array[Byte]],
      exercise_result: Option[Array[Byte]],
      exercise_actors: Option[Set[String]],
      exercise_child_event_ids: Option[Set[String]],
      exercise_argument_compression: Option[Int],
      exercise_result_compression: Option[Int],
      // missing: event_sequential_id: Long - this will be assigned only at batches
  ) extends DBDTOV1

  // TODO wartremover complained about having Array-s in case classes. I would prefer case classes. can we work that somehow around? Similarly in other DTO cases...
  class ConfigurationEntry(
      val ledger_offset: Array[Byte],
      val recorded_at: Instant,
      val submission_id: String,
      val typ: String,
      val configuration: Array[Byte],
      val rejection_reason: Option[String],
  ) extends DBDTOV1

  class PackageEntry(
      val ledger_offset: Array[Byte],
      val recorded_at: Instant,
      val submission_id: Option[String],
      val typ: String,
      val rejection_reason: Option[String],
  ) extends DBDTOV1

  class Package(
      val package_id: String,
      val upload_id: String,
      val source_description: Option[String],
      val size: Long,
      val known_since: Instant,
      val ledger_offset: Array[Byte],
      val _package: Array[Byte],
  ) extends DBDTOV1

  class PartyEntry(
      val ledger_offset: Array[Byte],
      val recorded_at: Instant,
      val submission_id: Option[String],
      val party: Option[String],
      val display_name: Option[String],
      val typ: String,
      val rejection_reason: Option[String],
      val is_local: Option[Boolean],
  ) extends DBDTOV1

  class Party(
      val party: String,
      val display_name: Option[String],
      val explicit: Boolean,
      val ledger_offset: Option[Array[Byte]],
      val is_local: Boolean,
  ) extends DBDTOV1

  class CommandCompletion(
      val completion_offset: Array[Byte],
      val record_time: Instant,
      val application_id: String,
      val submitters: Set[String],
      val command_id: String,
      val transaction_id: Option[String],
      val status_code: Option[Int],
      val status_message: Option[String],
  ) extends DBDTOV1

  class CommandDeduplication(val deduplication_key: String) extends DBDTOV1

}
