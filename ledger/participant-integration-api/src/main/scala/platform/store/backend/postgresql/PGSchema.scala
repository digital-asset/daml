// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import com.daml.platform.store.backend.DbDto

private[postgresql] object PGSchema {
  val eventsDivulgence: PGTable[DbDto.EventDivulgence] = PGTable("participant_events_divulgence")(
    "event_offset" -> PGStringOptional(_.event_offset),
    "command_id" -> PGStringOptional(_.command_id),
    "workflow_id" -> PGStringOptional(_.workflow_id),
    "application_id" -> PGStringOptional(_.application_id),
    "submitters" -> PGStringArrayOptional(_.submitters),
    "contract_id" -> PGString(_.contract_id),
    "template_id" -> PGStringOptional(_.template_id),
    "tree_event_witnesses" -> PGStringArray(_.tree_event_witnesses),
    "create_argument" -> PGByteaOptional(_.create_argument),
    "event_sequential_id" -> PGBigint(_.event_sequential_id),
    "create_argument_compression" -> PGSmallintOptional(_.create_argument_compression),
  )

  val eventsCreate: PGTable[DbDto.EventCreate] = PGTable("participant_events_create")(
    "event_offset" -> PGStringOptional(_.event_offset),
    "transaction_id" -> PGStringOptional(_.transaction_id),
    "ledger_effective_time" -> PGTimestampOptional(_.ledger_effective_time),
    "command_id" -> PGStringOptional(_.command_id),
    "workflow_id" -> PGStringOptional(_.workflow_id),
    "application_id" -> PGStringOptional(_.application_id),
    "submitters" -> PGStringArrayOptional(_.submitters),
    "node_index" -> PGIntOptional(_.node_index),
    "event_id" -> PGStringOptional(_.event_id),
    "contract_id" -> PGString(_.contract_id),
    "template_id" -> PGStringOptional(_.template_id),
    "flat_event_witnesses" -> PGStringArray(_.flat_event_witnesses),
    "tree_event_witnesses" -> PGStringArray(_.tree_event_witnesses),
    "create_argument" -> PGByteaOptional(_.create_argument),
    "create_signatories" -> PGStringArrayOptional(_.create_signatories),
    "create_observers" -> PGStringArrayOptional(_.create_observers),
    "create_agreement_text" -> PGStringOptional(_.create_agreement_text),
    "create_key_value" -> PGByteaOptional(_.create_key_value),
    "create_key_hash" -> PGStringOptional(_.create_key_hash),
    "event_sequential_id" -> PGBigint(_.event_sequential_id),
    "create_argument_compression" -> PGSmallintOptional(_.create_argument_compression),
    "create_key_value_compression" -> PGSmallintOptional(_.create_key_value_compression),
  )

  val exerciseFields: Vector[(String, PGField[DbDto.EventExercise, _, _])] =
    Vector[(String, PGField[DbDto.EventExercise, _, _])](
      "event_id" -> PGStringOptional(_.event_id),
      "event_offset" -> PGStringOptional(_.event_offset),
      "contract_id" -> PGString(_.contract_id),
      "transaction_id" -> PGStringOptional(_.transaction_id),
      "ledger_effective_time" -> PGTimestampOptional(_.ledger_effective_time),
      "node_index" -> PGIntOptional(_.node_index),
      "command_id" -> PGStringOptional(_.command_id),
      "workflow_id" -> PGStringOptional(_.workflow_id),
      "application_id" -> PGStringOptional(_.application_id),
      "submitters" -> PGStringArrayOptional(_.submitters),
      "create_key_value" -> PGByteaOptional(_.create_key_value),
      "exercise_choice" -> PGStringOptional(_.exercise_choice),
      "exercise_argument" -> PGByteaOptional(_.exercise_argument),
      "exercise_result" -> PGByteaOptional(_.exercise_result),
      "exercise_actors" -> PGStringArrayOptional(_.exercise_actors),
      "exercise_child_event_ids" -> PGStringArrayOptional(_.exercise_child_event_ids),
      "template_id" -> PGStringOptional(_.template_id),
      "flat_event_witnesses" -> PGStringArray(_.flat_event_witnesses),
      "tree_event_witnesses" -> PGStringArray(_.tree_event_witnesses),
      "event_sequential_id" -> PGBigint(_.event_sequential_id),
      "create_key_value_compression" -> PGSmallintOptional(_.create_key_value_compression),
      "exercise_argument_compression" -> PGSmallintOptional(_.exercise_argument_compression),
      "exercise_result_compression" -> PGSmallintOptional(_.exercise_result_compression),
    )

  val eventsConsumingExercise: PGTable[DbDto.EventExercise] =
    PGTable(tableName = "participant_events_consuming_exercise", exerciseFields)

  val eventsNonConsumingExercise: PGTable[DbDto.EventExercise] =
    PGTable(tableName = "participant_events_non_consuming_exercise", exerciseFields)

  val configurationEntries: PGTable[DbDto.ConfigurationEntry] = PGTable("configuration_entries")(
    "ledger_offset" -> PGString(_.ledger_offset),
    "recorded_at" -> PGTimestamp(_.recorded_at),
    "submission_id" -> PGString(_.submission_id),
    "typ" -> PGString(_.typ),
    "configuration" -> PGBytea(_.configuration),
    "rejection_reason" -> PGStringOptional(_.rejection_reason),
  )

  val packageEntries: PGTable[DbDto.PackageEntry] = PGTable("package_entries")(
    "ledger_offset" -> PGString(_.ledger_offset),
    "recorded_at" -> PGTimestamp(_.recorded_at),
    "submission_id" -> PGStringOptional(_.submission_id),
    "typ" -> PGString(_.typ),
    "rejection_reason" -> PGStringOptional(_.rejection_reason),
  )

  val packages: PGTable[DbDto.Package] = PGTable(
    tableName = "packages",
    insertSuffix = "on conflict (package_id) do nothing",
    fields = Vector[(String, PGField[DbDto.Package, _, _])](
      "package_id" -> PGString(_.package_id),
      "upload_id" -> PGString(_.upload_id),
      "source_description" -> PGStringOptional(_.source_description),
      "size" -> PGBigint(_.size),
      "known_since" -> PGTimestamp(_.known_since),
      "ledger_offset" -> PGString(_.ledger_offset),
      "package" -> PGBytea(_._package),
    ),
  )

  val partyEntries: PGTable[DbDto.PartyEntry] = PGTable("party_entries")(
    "ledger_offset" -> PGString(_.ledger_offset),
    "recorded_at" -> PGTimestamp(_.recorded_at),
    "submission_id" -> PGStringOptional(_.submission_id),
    "party" -> PGStringOptional(_.party),
    "display_name" -> PGStringOptional(_.display_name),
    "typ" -> PGString(_.typ),
    "rejection_reason" -> PGStringOptional(_.rejection_reason),
    "is_local" -> PGBooleanOptional(_.is_local),
  )

  val parties: PGTable[DbDto.Party] = PGTable("parties")(
    "party" -> PGString(_.party),
    "display_name" -> PGStringOptional(_.display_name),
    "explicit" -> PGBoolean(_.explicit),
    "ledger_offset" -> PGStringOptional(_.ledger_offset),
    "is_local" -> PGBoolean(_.is_local),
  )

  val commandCompletions: PGTable[DbDto.CommandCompletion] =
    PGTable("participant_command_completions")(
      "completion_offset" -> PGString(_.completion_offset),
      "record_time" -> PGTimestamp(_.record_time),
      "application_id" -> PGString(_.application_id),
      "submitters" -> PGStringArray(_.submitters),
      "command_id" -> PGString(_.command_id),
      "transaction_id" -> PGStringOptional(_.transaction_id),
      "status_code" -> PGIntOptional(_.status_code),
      "status_message" -> PGStringOptional(_.status_message),
    )
}
