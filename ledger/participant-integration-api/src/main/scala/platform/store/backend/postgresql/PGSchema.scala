// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import com.daml.platform.store.backend.DBDTOV1

object PGSchema {
  val eventsDivulgence: PGTable[DBDTOV1.EventDivulgence] = PGTable("participant_events_divulgence")(
    PGString("event_offset", _.event_offset.orNull),
    PGString("command_id", _.command_id.orNull),
    PGString("workflow_id", _.workflow_id.orNull),
    PGString("application_id", _.application_id.orNull),
    PGStringArray("submitters", _.submitters.orNull),
    PGString("contract_id", _.contract_id),
    PGString("template_id", _.template_id.orNull),
    PGStringArray("tree_event_witnesses", _.tree_event_witnesses),
    PGBytea("create_argument", _.create_argument.orNull),
    PGBigint("event_sequential_id", _.event_sequential_id),
    PGSmallintOptional("create_argument_compression", _.create_argument_compression),
  )

  val eventsCreate: PGTable[DBDTOV1.EventCreate] = PGTable("participant_events_create")(
    PGString("event_offset", _.event_offset.orNull),
    PGString("transaction_id", _.transaction_id.orNull),
    PGTimestamp("ledger_effective_time", _.ledger_effective_time.orNull),
    PGString("command_id", _.command_id.orNull),
    PGString("workflow_id", _.workflow_id.orNull),
    PGString("application_id", _.application_id.orNull),
    PGStringArray("submitters", _.submitters.orNull),
    PGIntOptional("node_index", _.node_index),
    PGString("event_id", _.event_id.orNull),
    PGString("contract_id", _.contract_id),
    PGString("template_id", _.template_id.orNull),
    PGStringArray("flat_event_witnesses", _.flat_event_witnesses),
    PGStringArray("tree_event_witnesses", _.tree_event_witnesses),
    PGBytea("create_argument", _.create_argument.orNull),
    PGStringArray("create_signatories", _.create_signatories.orNull),
    PGStringArray("create_observers", _.create_observers.orNull),
    PGString("create_agreement_text", _.create_agreement_text.orNull),
    PGBytea("create_key_value", _.create_key_value.orNull),
    PGString("create_key_hash", _.create_key_hash.orNull),
    PGBigint("event_sequential_id", _.event_sequential_id),
    PGSmallintOptional("create_argument_compression", _.create_argument_compression),
    PGSmallintOptional("create_key_value_compression", _.create_key_value_compression),
  )

  val exerciseFields: Vector[PGField[DBDTOV1.EventExercise, _]] =
    Vector[PGField[DBDTOV1.EventExercise, _]](
      PGString("event_id", _.event_id.orNull),
      PGString("event_offset", _.event_offset.orNull),
      PGString("contract_id", _.contract_id),
      PGString("transaction_id", _.transaction_id.orNull),
      PGTimestamp("ledger_effective_time", _.ledger_effective_time.orNull),
      PGIntOptional("node_index", _.node_index),
      PGString("command_id", _.command_id.orNull),
      PGString("workflow_id", _.workflow_id.orNull),
      PGString("application_id", _.application_id.orNull),
      PGStringArray("submitters", _.submitters.orNull),
      PGBytea("create_key_value", _.create_key_value.orNull),
      PGString("exercise_choice", _.exercise_choice.orNull),
      PGBytea("exercise_argument", _.exercise_argument.orNull),
      PGBytea("exercise_result", _.exercise_result.orNull),
      PGStringArray("exercise_actors", _.exercise_actors.orNull),
      PGStringArray("exercise_child_event_ids", _.exercise_child_event_ids.orNull),
      PGString("template_id", _.template_id.orNull),
      PGStringArray("flat_event_witnesses", _.flat_event_witnesses),
      PGStringArray("tree_event_witnesses", _.tree_event_witnesses),
      PGBigint("event_sequential_id", _.event_sequential_id),
      PGSmallintOptional("create_key_value_compression", _.create_key_value_compression),
      PGSmallintOptional("exercise_argument_compression", _.exercise_argument_compression),
      PGSmallintOptional("exercise_result_compression", _.exercise_result_compression),
    )

  val eventsConsumingExercise: PGTable[DBDTOV1.EventExercise] =
    PGTable(tableName = "participant_events_consuming_exercise", exerciseFields)

  val eventsNonConsumingExercise: PGTable[DBDTOV1.EventExercise] =
    PGTable(tableName = "participant_events_non_consuming_exercise", exerciseFields)

  val configurationEntries: PGTable[DBDTOV1.ConfigurationEntry] = PGTable("configuration_entries")(
    PGString("ledger_offset", _.ledger_offset),
    PGTimestamp("recorded_at", _.recorded_at),
    PGString("submission_id", _.submission_id),
    PGString("typ", _.typ),
    PGBytea("configuration", _.configuration),
    PGString("rejection_reason", _.rejection_reason.orNull),
  )

  val packageEntries: PGTable[DBDTOV1.PackageEntry] = PGTable("package_entries")(
    PGString("ledger_offset", _.ledger_offset),
    PGTimestamp("recorded_at", _.recorded_at),
    PGString("submission_id", _.submission_id.orNull),
    PGString("typ", _.typ),
    PGString("rejection_reason", _.rejection_reason.orNull),
  )

  val packages: PGTable[DBDTOV1.Package] = PGTable(
    tableName = "packages",
    insertSuffix = "on conflict (package_id) do nothing",
    fields = Vector[PGField[DBDTOV1.Package, _]](
      PGString("package_id", _.package_id),
      PGString("upload_id", _.upload_id),
      PGString("source_description", _.source_description.orNull),
      PGBigint("size", _.size),
      PGTimestamp("known_since", _.known_since),
      PGString("ledger_offset", _.ledger_offset),
      PGBytea("package", _._package),
    ),
  )

  val partyEntries: PGTable[DBDTOV1.PartyEntry] = PGTable("party_entries")(
    PGString("ledger_offset", _.ledger_offset),
    PGTimestamp("recorded_at", _.recorded_at),
    PGString("submission_id", _.submission_id.orNull),
    PGString("party", _.party.orNull),
    PGString("display_name", _.display_name.orNull),
    PGString("typ", _.typ),
    PGString("rejection_reason", _.rejection_reason.orNull),
    PGBooleanOptional("is_local", _.is_local),
  )

  val parties: PGTable[DBDTOV1.Party] = PGTable("parties")(
    PGString("party", _.party),
    PGString("display_name", _.display_name.orNull),
    PGBoolean("explicit", _.explicit),
    PGString("ledger_offset", _.ledger_offset.orNull),
    PGBoolean("is_local", _.is_local),
  )

  val commandCompletions: PGTable[DBDTOV1.CommandCompletion] =
    PGTable("participant_command_completions")(
      PGString("completion_offset", _.completion_offset),
      PGTimestamp("record_time", _.record_time),
      PGString("application_id", _.application_id),
      PGStringArray("submitters", _.submitters),
      PGString("command_id", _.command_id),
      PGString("transaction_id", _.transaction_id.orNull),
      PGIntOptional("status_code", _.status_code),
      PGString("status_message", _.status_message.orNull),
    )
}
