// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DbDtoToStringsForInterningSpec extends AnyFlatSpec with Matchers {

  it should "select all relevant strings for interning" in {
    val iterators = DbDtoToStringsForInterning(fixture)
    iterators.templateIds.toList.sorted shouldBe List(
      "14",
      "25",
      "50",
    ).sorted
    iterators.parties.toList.sorted shouldBe List(
      "2",
      "10",
      "11",
      "12",
      "15",
      "16",
      "17",
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
    ).sorted
  }

  private def fixture: List[DbDto] = List(
    DbDto.PartyEntry(
      ledger_offset = "0",
      recorded_at = 0,
      submission_id = Some("1"),
      party = Some("2"),
      display_name = Some("3"),
      typ = "3",
      rejection_reason = Some("4"),
      is_local = None,
    ),
    DbDto.StringInterningDto(
      internalId = 1,
      externalString = "5",
    ),
    DbDto.EventDivulgence(
      event_offset = Some("6"),
      command_id = Some("7"),
      workflow_id = Some("8"),
      application_id = Some("9"),
      submitters = Some(Set("10", "11", "12")),
      contract_id = "13",
      template_id = Some("14"),
      tree_event_witnesses = Set("15", "16", "17"),
      create_argument = None,
      create_argument_compression = Some(1),
      event_sequential_id = 1,
    ),
    DbDto.EventCreate(
      event_offset = Some("15"),
      transaction_id = Some("16"),
      ledger_effective_time = Some(1),
      command_id = Some("17"),
      workflow_id = Some("18"),
      application_id = Some("19"),
      submitters = Some(Set("20", "21", "22")),
      node_index = Some(1),
      event_id = Some("23"),
      contract_id = "24",
      template_id = Some("25"),
      flat_event_witnesses = Set("26", "27", "28"),
      tree_event_witnesses = Set("29", "30", "31"),
      create_argument = None,
      create_signatories = Some(Set("32", "33", "34")),
      create_observers = Some(Set("35", "36", "37")),
      create_agreement_text = Some("38"),
      create_key_value = None,
      create_key_hash = Some("39"),
      create_argument_compression = Some(1),
      create_key_value_compression = Some(1),
      event_sequential_id = 1,
    ),
    DbDto.EventExercise(
      consuming = true,
      event_offset = Some("40"),
      transaction_id = Some("41"),
      ledger_effective_time = Some(1),
      command_id = Some("42"),
      workflow_id = Some("43"),
      application_id = Some("44"),
      submitters = Some(Set("45", "46", "47")),
      node_index = Some(1),
      event_id = Some("48"),
      contract_id = "49",
      template_id = Some("50"),
      flat_event_witnesses = Set("51", "52", "53"),
      tree_event_witnesses = Set("54", "55", "56"),
      exercise_argument = None,
      exercise_actors = Some(Set("57", "58", "59")),
      create_key_value = None,
      exercise_argument_compression = Some(1),
      create_key_value_compression = Some(1),
      event_sequential_id = 1,
      exercise_choice = Some("60"),
      exercise_result = None,
      exercise_child_event_ids = Some(Set("61", "62", "63")),
      exercise_result_compression = Some(1),
    ),
    DbDto.CommandCompletion(
      completion_offset = "64",
      record_time = 2,
      application_id = "65",
      submitters = Set("66", "67", "68"),
      command_id = "69",
      transaction_id = Some("70"),
      rejection_status_code = Some(1),
      rejection_status_message = Some("71"),
      rejection_status_details = None,
      submission_id = Some("72"),
      deduplication_offset = Some("73"),
      deduplication_duration_seconds = Some(1),
      deduplication_duration_nanos = Some(1),
      deduplication_start = Some(1),
    ),
    DbDto.CommandDeduplication(
      deduplication_key = "74"
    ),
    DbDto.ConfigurationEntry(
      ledger_offset = "75",
      recorded_at = 1,
      submission_id = "76",
      typ = "77",
      configuration = Array.empty,
      rejection_reason = Some("78"),
    ),
    DbDto.Package(
      package_id = "79",
      upload_id = "80",
      source_description = Some("81"),
      package_size = 2,
      known_since = 2,
      ledger_offset = "82",
      _package = Array.empty,
    ),
    DbDto.PackageEntry(
      ledger_offset = "83",
      recorded_at = 1,
      submission_id = Some("84"),
      typ = "85",
      rejection_reason = Some("86"),
    ),
  )

}
