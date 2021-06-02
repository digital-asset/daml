// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import com.daml.platform.store.backend.DBDTOV1

private[postgresql] object PGSchema {
  val eventsDivulgence: PGTable[DBDTOV1.EventDivulgence] = PGTable("participant_events_divulgence")(
    "event_offset" -> PGString(_.event_offset.orNull),
    "command_id" -> PGString(_.command_id.orNull),
    "workflow_id" -> PGString(_.workflow_id.orNull),
    "application_id" -> PGString(_.application_id.orNull),
    "submitters" -> PGStringArray(_.submitters.orNull),
    "contract_id" -> PGString(_.contract_id),
    "template_id" -> PGString(_.template_id.orNull),
    "tree_event_witnesses" -> PGStringArray(_.tree_event_witnesses),
    "create_argument" -> PGBytea(_.create_argument.orNull),
    "event_sequential_id" -> PGBigint(_.event_sequential_id),
    "create_argument_compression" -> PGSmallintOptional(_.create_argument_compression),
  )

  val eventsCreate: PGTable[DBDTOV1.EventCreate] = PGTable("participant_events_create")(
    "event_offset" -> PGString(_.event_offset.orNull),
    "transaction_id" -> PGString(_.transaction_id.orNull),
    "ledger_effective_time" -> PGTimestamp(_.ledger_effective_time.orNull),
    "command_id" -> PGString(_.command_id.orNull),
    "workflow_id" -> PGString(_.workflow_id.orNull),
    "application_id" -> PGString(_.application_id.orNull),
    "submitters" -> PGStringArray(_.submitters.orNull),
    "node_index" -> PGIntOptional(_.node_index),
    "event_id" -> PGString(_.event_id.orNull),
    "contract_id" -> PGString(_.contract_id),
    "template_id" -> PGString(_.template_id.orNull),
    "flat_event_witnesses" -> PGStringArray(_.flat_event_witnesses),
    "tree_event_witnesses" -> PGStringArray(_.tree_event_witnesses),
    "create_argument" -> PGBytea(_.create_argument.orNull),
    "create_signatories" -> PGStringArray(_.create_signatories.orNull),
    "create_observers" -> PGStringArray(_.create_observers.orNull),
    "create_agreement_text" -> PGString(_.create_agreement_text.orNull),
    "create_key_value" -> PGBytea(_.create_key_value.orNull),
    "create_key_hash" -> PGString(_.create_key_hash.orNull),
    "event_sequential_id" -> PGBigint(_.event_sequential_id),
    "create_argument_compression" -> PGSmallintOptional(_.create_argument_compression),
    "create_key_value_compression" -> PGSmallintOptional(_.create_key_value_compression),
  )

  val exerciseFields: Vector[(String, PGField[DBDTOV1.EventExercise, _, _])] =
    Vector[(String, PGField[DBDTOV1.EventExercise, _, _])](
      "event_id" -> PGString(_.event_id.orNull),
      "event_offset" -> PGString(_.event_offset.orNull),
      "contract_id" -> PGString(_.contract_id),
      "transaction_id" -> PGString(_.transaction_id.orNull),
      "ledger_effective_time" -> PGTimestamp(_.ledger_effective_time.orNull),
      "node_index" -> PGIntOptional(_.node_index),
      "command_id" -> PGString(_.command_id.orNull),
      "workflow_id" -> PGString(_.workflow_id.orNull),
      "application_id" -> PGString(_.application_id.orNull),
      "submitters" -> PGStringArray(_.submitters.orNull),
      "create_key_value" -> PGBytea(_.create_key_value.orNull),
      "exercise_choice" -> PGString(_.exercise_choice.orNull),
      "exercise_argument" -> PGBytea(_.exercise_argument.orNull),
      "exercise_result" -> PGBytea(_.exercise_result.orNull),
      "exercise_actors" -> PGStringArray(_.exercise_actors.orNull),
      "exercise_child_event_ids" -> PGStringArray(_.exercise_child_event_ids.orNull),
      "template_id" -> PGString(_.template_id.orNull),
      "flat_event_witnesses" -> PGStringArray(_.flat_event_witnesses),
      "tree_event_witnesses" -> PGStringArray(_.tree_event_witnesses),
      "event_sequential_id" -> PGBigint(_.event_sequential_id),
      "create_key_value_compression" -> PGSmallintOptional(_.create_key_value_compression),
      "exercise_argument_compression" -> PGSmallintOptional(_.exercise_argument_compression),
      "exercise_result_compression" -> PGSmallintOptional(_.exercise_result_compression),
    )

  val eventsConsumingExercise: PGTable[DBDTOV1.EventExercise] =
    PGTable(tableName = "participant_events_consuming_exercise", exerciseFields)

  val eventsNonConsumingExercise: PGTable[DBDTOV1.EventExercise] =
    PGTable(tableName = "participant_events_non_consuming_exercise", exerciseFields)

  val configurationEntries: PGTable[DBDTOV1.ConfigurationEntry] = PGTable("configuration_entries")(
    "ledger_offset" -> PGString(_.ledger_offset),
    "recorded_at" -> PGTimestamp(_.recorded_at),
    "submission_id" -> PGString(_.submission_id),
    "typ" -> PGString(_.typ),
    "configuration" -> PGBytea(_.configuration),
    "rejection_reason" -> PGString(_.rejection_reason.orNull),
  )

  val packageEntries: PGTable[DBDTOV1.PackageEntry] = PGTable("package_entries")(
    "ledger_offset" -> PGString(_.ledger_offset),
    "recorded_at" -> PGTimestamp(_.recorded_at),
    "submission_id" -> PGString(_.submission_id.orNull),
    "typ" -> PGString(_.typ),
    "rejection_reason" -> PGString(_.rejection_reason.orNull),
  )

  val packages: PGTable[DBDTOV1.Package] = PGTable(
    tableName = "packages",
    insertSuffix = "on conflict (package_id) do nothing",
    fields = Vector[(String, PGField[DBDTOV1.Package, _, _])](
      "package_id" -> PGString(_.package_id),
      "upload_id" -> PGString(_.upload_id),
      "source_description" -> PGString(_.source_description.orNull),
      "size" -> PGBigint(_.size),
      "known_since" -> PGTimestamp(_.known_since),
      "ledger_offset" -> PGString(_.ledger_offset),
      "package" -> PGBytea(_._package),
    ),
  )

  val partyEntries: PGTable[DBDTOV1.PartyEntry] = PGTable("party_entries")(
    "ledger_offset" -> PGString(_.ledger_offset),
    "recorded_at" -> PGTimestamp(_.recorded_at),
    "submission_id" -> PGString(_.submission_id.orNull),
    "party" -> PGString(_.party.orNull),
    "display_name" -> PGString(_.display_name.orNull),
    "typ" -> PGString(_.typ),
    "rejection_reason" -> PGString(_.rejection_reason.orNull),
    "is_local" -> PGBooleanOptional(_.is_local),
  )

  val parties: PGTable[DBDTOV1.Party] = PGTable("parties")(
    "party" -> PGString(_.party),
    "display_name" -> PGString(_.display_name.orNull),
    "explicit" -> PGBoolean(_.explicit),
    "ledger_offset" -> PGString(_.ledger_offset.orNull),
    "is_local" -> PGBoolean(_.is_local),
  )

  val commandCompletions: PGTable[DBDTOV1.CommandCompletion] =
    PGTable("participant_command_completions")(
      "completion_offset" -> PGString(_.completion_offset),
      "record_time" -> PGTimestamp(_.record_time),
      "application_id" -> PGString(_.application_id),
      "submitters" -> PGStringArray(_.submitters),
      "command_id" -> PGString(_.command_id),
      "transaction_id" -> PGString(_.transaction_id.orNull),
      "status_code" -> PGIntOptional(_.status_code),
      "status_message" -> PGString(_.status_message.orNull),
    )
}
