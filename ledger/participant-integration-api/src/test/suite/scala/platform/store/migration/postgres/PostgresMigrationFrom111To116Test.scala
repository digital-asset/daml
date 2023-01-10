// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.migration.postgres

import com.daml.platform.store.DbType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.daml.platform.store.migration.DbDataTypes

class PostgresMigrationFrom111To116Test
    extends AnyFlatSpec
    with Matchers
    with PostgresAroundEachForMigrations {
  import com.daml.platform.store.migration.MigrationTestSupport._

  behavior of "Data migrations from version 111 to version 116 (String Interning and Filter Table features)"

  it should "migrate correctly data for a database with transactions" in {
    migrateTo("111")

    val params = row(
      "ledger_id" -> "lid",
      "participant_id" -> "pid",
      "ledger_end" -> Some("abcdefg"),
      "ledger_end_sequential_id" -> Some(200L),
      "participant_pruned_up_to_inclusive" -> None,
      "participant_all_divulged_contracts_pruned_up_to_inclusive" -> None,
    )
    insert(Schema111.parameters, params)

    val partyEntry = row(
      "ledger_offset" -> "abcd",
      "recorded_at" -> 10L,
      "submission_id" -> None,
      "party" -> Some("party1"),
      "display_name" -> None,
      "typ" -> "accept",
      "rejection_reason" -> None,
      "is_local" -> None,
    )
    val partyRejected = partyEntry ++ row(
      "ledger_offset" -> "abce",
      "recorded_at" -> 11L,
      "party" -> None,
      "typ" -> "reject",
      "rejection_reason" -> Some("reason"),
    )
    insert(Schema111.partyEntries, partyEntry, partyRejected)

    val baseCompletion = row(
      "completion_offset" -> "abcd0000",
      "record_time" -> 15L,
      "application_id" -> "application id",
      "submitters" -> Vector("party2", "party3", "party4"),
      "command_id" -> "command id",
      "transaction_id" -> None,
      "submission_id" -> None,
      "deduplication_offset" -> None,
      "deduplication_duration_seconds" -> None,
      "deduplication_duration_nanos" -> None,
      "deduplication_start" -> None,
      "rejection_status_code" -> None,
      "rejection_status_message" -> None,
      "rejection_status_details" -> None,
    )
    val zeroSubmitterCompletion = baseCompletion ++ row(
      "completion_offset" -> "abcd0001",
      "submitters" -> Vector(),
    )
    val oneSubmitterCompletion = baseCompletion ++ row(
      "completion_offset" -> "abcd0002",
      "submitters" -> Vector("party3"),
    )
    val repeatingSubmitterCompletion = baseCompletion ++ row(
      "completion_offset" -> "abcd0003",
      "submitters" -> Vector("party5", "party5", "party6"),
    )
    val completions = Vector(
      baseCompletion,
      zeroSubmitterCompletion,
      oneSubmitterCompletion,
      repeatingSubmitterCompletion,
    )
    insert(Schema111.commandCompletions, completions: _*)

    val baseEvent = row(
      "event_sequential_id" -> 0L, // to be overridden
      "workflow_id" -> None,
      "command_id" -> None,
      "application_id" -> None,
      "submitters" -> None, // to be overridden
      "contract_id" -> "contract id",
      "tree_event_witnesses" -> Vector(), // to be overridden
    )
    val baseDivulgenceEvent = row(
      "event_offset" -> None,
      "template_id" -> None, // to be overridden
      "create_argument" -> None,
      "create_argument_compression" -> None,
    ) ++ baseEvent
    val baseTransactionEvent = row(
      "ledger_effective_time" -> 0L,
      "event_offset" -> "some offset",
      "node_index" -> 0,
      "transaction_id" -> "some tx id",
      "event_id" -> "some event id",
      "template_id" -> "some template id", // to be overridden
      "flat_event_witnesses" -> Vector(), // to be overridden
    ) ++ baseEvent
    val baseCreateEvent = row(
      "create_argument" -> Vector(1.toByte, 2.toByte, 3.toByte),
      "create_argument_compression" -> None,
      "create_key_value" -> None,
      "create_key_value_compression" -> None,
      "create_signatories" -> Vector(), // to be overridden
      "create_observers" -> Vector(), // to be overridden
      "create_agreement_text" -> None,
      "create_key_hash" -> None,
    ) ++ baseTransactionEvent
    val baseExerciseEvent = row(
      "exercise_argument" -> Vector(3.toByte, 2.toByte, 1.toByte),
      "exercise_argument_compression" -> None,
      "exercise_result" -> None,
      "exercise_result_compression" -> None,
      "create_key_value" -> None,
      "create_key_value_compression" -> None,
      "exercise_choice" -> "some choice",
      "exercise_actors" -> Vector(), // to be overridden
      "exercise_child_event_ids" -> Vector(),
    ) ++ baseTransactionEvent

    val divulgences = Vector(
      baseDivulgenceEvent ++ row(
        "event_sequential_id" -> 1000L,
        "submitters" -> None,
        "tree_event_witnesses" -> Vector(),
        "template_id" -> None,
      ),
      baseDivulgenceEvent ++ row(
        "event_sequential_id" -> 1001L,
        "submitters" -> Some(Vector("party1")),
        "tree_event_witnesses" -> Vector("party1"),
        "template_id" -> Some("template1"),
      ),
      baseDivulgenceEvent ++ row(
        "event_sequential_id" -> 1002L,
        "submitters" -> Some(Vector("party7", "party8")),
        "tree_event_witnesses" -> Vector("party11", "party12"),
        "template_id" -> Some("template2"),
      ),
      baseDivulgenceEvent ++ row(
        "event_sequential_id" -> 1003L,
        "submitters" -> Some(Vector("party9", "party8", "party9", "party10")),
        "tree_event_witnesses" -> Vector("party12", "party13", "party13", "party14"),
        "template_id" -> None,
      ),
      baseDivulgenceEvent ++ row(
        "event_sequential_id" -> 1004L,
        "submitters" -> Some(Vector()),
        "tree_event_witnesses" -> Vector(),
        "template_id" -> None,
      ),
    )
    insert(Schema111.divulgenceEvents, divulgences: _*)

    val createEvents = Vector(
      baseCreateEvent ++ row(
        "event_sequential_id" -> 2000L,
        "submitters" -> None,
        "tree_event_witnesses" -> Vector(),
        "template_id" -> "template1",
        "flat_event_witnesses" -> Vector(),
        "create_signatories" -> Vector(),
        "create_observers" -> Vector(),
      ),
      baseCreateEvent ++ row(
        "event_sequential_id" -> 2001L,
        "submitters" -> Some(Vector("party1")),
        "tree_event_witnesses" -> Vector("party1"),
        "template_id" -> "template2",
        "flat_event_witnesses" -> Vector("party1"),
        "create_signatories" -> Vector("party1"),
        "create_observers" -> Vector("party1"),
      ),
      baseCreateEvent ++ row(
        "event_sequential_id" -> 2002L,
        "submitters" -> Some(Vector("party15", "party16")),
        "tree_event_witnesses" -> Vector("party19", "party20"),
        "template_id" -> "template2",
        "flat_event_witnesses" -> Vector("party22", "party23"),
        "create_signatories" -> Vector("party26", "party27"),
        "create_observers" -> Vector("party30"),
      ),
      baseCreateEvent ++ row(
        "event_sequential_id" -> 2003L,
        "submitters" -> Some(Vector("party17", "party16", "party17", "party18")),
        "tree_event_witnesses" -> Vector("party1", "party1", "party21"),
        "template_id" -> "template2",
        "flat_event_witnesses" -> Vector("party1", "party24", "party24", "party25"),
        "create_signatories" -> Vector("party1", "party28", "party28", "party29"),
        "create_observers" -> Vector("party31", "party31"),
      ),
      baseCreateEvent ++ row(
        "event_sequential_id" -> 2004L,
        "submitters" -> Some(Vector()),
        "tree_event_witnesses" -> Vector(),
        "template_id" -> "template2",
        "flat_event_witnesses" -> Vector(),
        "create_signatories" -> Vector(),
        "create_observers" -> Vector(),
      ),
    )
    insert(Schema111.createEvents, createEvents: _*)

    val consumingExerciseEvents = Vector(
      baseExerciseEvent ++ row(
        "event_sequential_id" -> 3000L,
        "submitters" -> None,
        "tree_event_witnesses" -> Vector(),
        "template_id" -> "template1",
        "flat_event_witnesses" -> Vector(),
        "exercise_actors" -> Vector(),
      ),
      baseExerciseEvent ++ row(
        "event_sequential_id" -> 3001L,
        "submitters" -> Some(Vector("party1")),
        "tree_event_witnesses" -> Vector("party1"),
        "template_id" -> "template10",
        "flat_event_witnesses" -> Vector("party1"),
        "exercise_actors" -> Vector("party1"),
      ),
      baseExerciseEvent ++ row(
        "event_sequential_id" -> 3002L,
        "submitters" -> Some(Vector("party32", "party33")),
        "tree_event_witnesses" -> Vector("party34", "party35"),
        "template_id" -> "template11",
        "flat_event_witnesses" -> Vector("party36", "party37"),
        "exercise_actors" -> Vector("party38", "party39"),
      ),
      baseExerciseEvent ++ row(
        "event_sequential_id" -> 3003L,
        "submitters" -> Some(Vector("party40", "party40", "party1", "party41")),
        "tree_event_witnesses" -> Vector("party42", "party42", "party1", "party43"),
        "template_id" -> "template11",
        "flat_event_witnesses" -> Vector("party44", "party44", "party1", "party45"),
        "exercise_actors" -> Vector("party46", "party46", "party1", "party47"),
      ),
      baseExerciseEvent ++ row(
        "event_sequential_id" -> 3004L,
        "submitters" -> Some(Vector()),
        "tree_event_witnesses" -> Vector(),
        "template_id" -> "template1",
        "flat_event_witnesses" -> Vector(),
        "exercise_actors" -> Vector(),
      ),
    )
    insert(Schema111.consumingExerciseEvents, consumingExerciseEvents: _*)

    val nonConsumingExerciseEvents = Vector(
      baseExerciseEvent ++ row(
        "event_sequential_id" -> 4000L,
        "submitters" -> None,
        "tree_event_witnesses" -> Vector(),
        "template_id" -> "template1",
        "flat_event_witnesses" -> Vector(),
        "exercise_actors" -> Vector(),
      ),
      baseExerciseEvent ++ row(
        "event_sequential_id" -> 4001L,
        "submitters" -> Some(Vector("party1")),
        "tree_event_witnesses" -> Vector("party1"),
        "template_id" -> "template20",
        "flat_event_witnesses" -> Vector("party1"),
        "exercise_actors" -> Vector("party1"),
      ),
      baseExerciseEvent ++ row(
        "event_sequential_id" -> 4002L,
        "submitters" -> Some(Vector("party48", "party49")),
        "tree_event_witnesses" -> Vector("party50", "party51"),
        "template_id" -> "template21",
        "flat_event_witnesses" -> Vector("party52", "party53"),
        "exercise_actors" -> Vector("party54", "party55"),
      ),
      baseExerciseEvent ++ row(
        "event_sequential_id" -> 4003L,
        "submitters" -> Some(Vector("party56", "party56", "party1", "party57")),
        "tree_event_witnesses" -> Vector("party58", "party58", "party1", "party59"),
        "template_id" -> "template21",
        "flat_event_witnesses" -> Vector("party60", "party60", "party1", "party61"),
        "exercise_actors" -> Vector("party62", "party62", "party1", "party63"),
      ),
      baseExerciseEvent ++ row(
        "event_sequential_id" -> 4004L,
        "submitters" -> Some(Vector()),
        "tree_event_witnesses" -> Vector(),
        "template_id" -> "template1",
        "flat_event_witnesses" -> Vector(),
        "exercise_actors" -> Vector(),
      ),
    )
    insert(Schema111.nonConsumingExerciseEvents, nonConsumingExerciseEvents: _*)

    migrateTo("116")

    val (unInternParty, unInternTemplate) = fetchTable(Schema116.stringInterning)
      .foldLeft((Map.empty[Int, String], Map.empty[Int, String])) {
        case ((partyMap, templateIdMap), dbEntry) =>
          val id = dbEntry("internal_id").asInstanceOf[Int]
          dbEntry("external_string").asInstanceOf[String].split("\\|").toList match {
            case "p" :: party :: Nil => (partyMap + (id -> party), templateIdMap)
            case "t" :: templateId :: Nil => (partyMap, templateIdMap + (id -> templateId))
            case _ =>
              throw new Exception(s"external_string ${dbEntry("external_string")} is invalid")
          }
      }

    fetchTable(Schema116.parameters) shouldBe Vector(
      params + ("ledger_end_string_interning_id" -> Some(
        unInternParty.size + unInternTemplate.size
      ))
    )

    fetchTable(Schema116.partyEntries)
      .updateInAll[Option[Int]]("party_id")(_.map(unInternParty)) shouldBe Vector(
      partyEntry + ("party_id" -> Some("party1")),
      partyRejected + ("party_id" -> None),
    )

    fetchTable(Schema116.commandCompletions)
      .updateInAll[Vector[Int]]("submitters")(_.map(unInternParty)) shouldBe completions

    fetchTable(Schema116.divulgenceEvents)
      .updateInAll[Option[Vector[Int]]]("submitters")(_.map(_.map(unInternParty)))
      .updateInAll[Option[Int]]("template_id")(_.map(unInternTemplate))
      .updateInAll[Vector[Int]]("tree_event_witnesses")(_.map(unInternParty)) shouldBe divulgences

    fetchTable(Schema116.createEvents)
      .updateInAll[Option[Vector[Int]]]("submitters")(_.map(_.map(unInternParty)))
      .updateInAll[Int]("template_id")(unInternTemplate)
      .updateInAll[Vector[Int]]("tree_event_witnesses")(_.map(unInternParty))
      .updateInAll[Vector[Int]]("flat_event_witnesses")(_.map(unInternParty))
      .updateInAll[Vector[Int]]("create_signatories")(_.map(unInternParty))
      .updateInAll[Vector[Int]]("create_observers")(_.map(unInternParty)) shouldBe createEvents

    fetchTable(Schema116.createEventsFilter)
      .updateInAll[Int]("template_id")(unInternTemplate)
      .updateInAll[Int]("party_id")(unInternParty)
      .toSet shouldBe createEvents
      .flatMap(createEvent =>
        createEvent("flat_event_witnesses")
          .asInstanceOf[Vector[String]]
          .map(flatWitness =>
            row(
              "event_sequential_id" -> createEvent("event_sequential_id"),
              "template_id" -> createEvent("template_id"),
              "party_id" -> flatWitness,
            )
          )
      )
      .toSet

    fetchTable(Schema116.consumingExerciseEvents)
      .updateInAll[Option[Vector[Int]]]("submitters")(_.map(_.map(unInternParty)))
      .updateInAll[Int]("template_id")(unInternTemplate)
      .updateInAll[Vector[Int]]("tree_event_witnesses")(_.map(unInternParty))
      .updateInAll[Vector[Int]]("flat_event_witnesses")(_.map(unInternParty))
      .updateInAll[Vector[Int]]("exercise_actors")(
        _.map(unInternParty)
      ) shouldBe consumingExerciseEvents

    fetchTable(Schema116.nonConsumingExerciseEvents)
      .updateInAll[Option[Vector[Int]]]("submitters")(_.map(_.map(unInternParty)))
      .updateInAll[Int]("template_id")(unInternTemplate)
      .updateInAll[Vector[Int]]("tree_event_witnesses")(_.map(unInternParty))
      .updateInAll[Vector[Int]]("flat_event_witnesses")(_.map(unInternParty))
      .updateInAll[Vector[Int]]("exercise_actors")(
        _.map(unInternParty)
      ) shouldBe nonConsumingExerciseEvents
  }

  it should "migrate correctly data for a database without transactions and without party entries" in {
    migrateTo("111")

    val params = row(
      "ledger_id" -> "lid",
      "participant_id" -> "pid",
      "ledger_end" -> Some("abcdefg"),
      "ledger_end_sequential_id" -> Some(200L),
      "participant_pruned_up_to_inclusive" -> None,
      "participant_all_divulged_contracts_pruned_up_to_inclusive" -> None,
    )
    insert(Schema111.parameters, params)

    migrateTo("116")
    fetchTable(Schema116.parameters) shouldBe Vector(
      params + ("ledger_end_string_interning_id" -> Some(0))
    )
  }

  it should "migrate correctly data for a database without transactions and with party entries" in {
    migrateTo("111")

    val params = row(
      "ledger_id" -> "lid",
      "participant_id" -> "pid",
      "ledger_end" -> Some("abcdefg"),
      "ledger_end_sequential_id" -> Some(200L),
      "participant_pruned_up_to_inclusive" -> None,
      "participant_all_divulged_contracts_pruned_up_to_inclusive" -> None,
    )
    insert(Schema111.parameters, params)

    val partyEntry = row(
      "ledger_offset" -> "abcd",
      "recorded_at" -> 10L,
      "submission_id" -> None,
      "party" -> Some("party1"),
      "display_name" -> None,
      "typ" -> "accept",
      "rejection_reason" -> None,
      "is_local" -> None,
    )
    insert(Schema111.partyEntries, partyEntry)

    migrateTo("116")

    fetchTable(Schema116.parameters) shouldBe Vector(
      params + ("ledger_end_string_interning_id" -> Some(1))
    )

    fetchTable(Schema116.partyEntries) shouldBe Vector(
      partyEntry + ("party_id" -> Some(1))
    )
  }
}

object Schema111 {
  private val dataTypes = new DbDataTypes(DbType.Postgres)
  import dataTypes._
  import com.daml.platform.store.migration.MigrationTestSupport._

  val parameters: TableSchema = TableSchema("parameters", "ledger_id")(
    "ledger_id" -> Str,
    "participant_id" -> Str,
    "ledger_end" -> Str.optional,
    "ledger_end_sequential_id" -> BigInt.optional,
    "participant_pruned_up_to_inclusive" -> Str.optional,
    "participant_all_divulged_contracts_pruned_up_to_inclusive" -> Str.optional,
  )

  val partyEntries: TableSchema = TableSchema("party_entries", "ledger_offset")(
    "ledger_offset" -> Str,
    "recorded_at" -> BigInt,
    "submission_id" -> Str.optional,
    "party" -> Str.optional,
    "display_name" -> Str.optional,
    "typ" -> Str,
    "rejection_reason" -> Str.optional,
    "is_local" -> Bool.optional,
  )

  val commandCompletions: TableSchema =
    TableSchema("participant_command_completions", "completion_offset")(
      "completion_offset" -> Str,
      "record_time" -> BigInt,
      "application_id" -> Str,
      "submitters" -> StringArray,
      "command_id" -> Str,
      "transaction_id" -> Str.optional,
      "submission_id" -> Str.optional,
      "deduplication_offset" -> Str.optional,
      "deduplication_duration_seconds" -> BigInt.optional,
      "deduplication_duration_nanos" -> Integer.optional,
      "deduplication_start" -> BigInt.optional,
      "rejection_status_code" -> Integer.optional,
      "rejection_status_message" -> Str.optional,
      "rejection_status_details" -> Bytea.optional,
    )

  private val commonEventsColumns: List[(String, DbDataType)] = List(
    "event_sequential_id" -> BigInt,
    "workflow_id" -> Str.optional,
    "command_id" -> Str.optional,
    "application_id" -> Str.optional,
    "submitters" -> StringArray.optional,
    "contract_id" -> Str,
    "tree_event_witnesses" -> StringArray,
  )

  private val commonTransactionEventsColumns: List[(String, DbDataType)] =
    commonEventsColumns ::: List[(String, DbDataType)](
      "ledger_effective_time" -> BigInt,
      "event_offset" -> Str,
      "node_index" -> Integer,
      "transaction_id" -> Str,
      "event_id" -> Str,
      "template_id" -> Str,
      "flat_event_witnesses" -> StringArray,
    )

  val divulgenceEvents: TableSchema =
    TableSchema("participant_events_divulgence", "event_sequential_id")(commonEventsColumns: _*)
      .++(
        "event_offset" -> Str.optional,
        "template_id" -> Str.optional,
        "create_argument" -> Bytea.optional,
        "create_argument_compression" -> Integer.optional,
      )

  val createEvents: TableSchema = TableSchema("participant_events_create", "event_sequential_id")(
    commonTransactionEventsColumns: _*
  )
    .++(
      "create_argument" -> Bytea,
      "create_argument_compression" -> Integer.optional,
      "create_key_value" -> Bytea.optional,
      "create_key_value_compression" -> Integer.optional,
      "create_signatories" -> StringArray,
      "create_observers" -> StringArray,
      "create_agreement_text" -> Str.optional,
      "create_key_hash" -> Str.optional,
    )

  private val commonExerciseEventsColumns: List[(String, DbDataType)] =
    commonTransactionEventsColumns ::: List(
      "exercise_argument" -> Bytea,
      "exercise_argument_compression" -> Integer.optional,
      "exercise_result" -> Bytea.optional,
      "exercise_result_compression" -> Integer.optional,
      "create_key_value" -> Bytea.optional,
      "create_key_value_compression" -> Integer.optional,
      "exercise_choice" -> Str,
      "exercise_actors" -> StringArray,
      "exercise_child_event_ids" -> StringArray,
    )

  val consumingExerciseEvents: TableSchema =
    TableSchema("participant_events_consuming_exercise", "event_sequential_id")(
      commonExerciseEventsColumns: _*
    )

  val nonConsumingExerciseEvents: TableSchema =
    TableSchema("participant_events_non_consuming_exercise", "event_sequential_id")(
      commonExerciseEventsColumns: _*
    )
}

object Schema116 {
  private val dataTypes = new DbDataTypes(DbType.Postgres)
  import dataTypes._
  import com.daml.platform.store.migration.MigrationTestSupport._

  val parameters: TableSchema = Schema111.parameters
    .++("ledger_end_string_interning_id" -> Integer.optional)

  val partyEntries: TableSchema = Schema111.partyEntries
    .++("party_id" -> Integer.optional)

  val commandCompletions: TableSchema = Schema111.commandCompletions
    .++("submitters" -> IntArray)

  val divulgenceEvents: TableSchema = Schema111.divulgenceEvents
    .++(
      "submitters" -> IntArray.optional,
      "template_id" -> Integer.optional,
      "tree_event_witnesses" -> IntArray,
    )

  val createEvents: TableSchema = Schema111.createEvents
    .++(
      "submitters" -> IntArray.optional,
      "template_id" -> Integer,
      "tree_event_witnesses" -> IntArray,
      "flat_event_witnesses" -> IntArray,
      "create_signatories" -> IntArray,
      "create_observers" -> IntArray,
    )

  val consumingExerciseEvents: TableSchema = Schema111.consumingExerciseEvents
    .++(
      "submitters" -> IntArray.optional,
      "template_id" -> Integer,
      "tree_event_witnesses" -> IntArray,
      "flat_event_witnesses" -> IntArray,
      "exercise_actors" -> IntArray,
    )

  val nonConsumingExerciseEvents: TableSchema = Schema111.nonConsumingExerciseEvents
    .++(
      "submitters" -> IntArray.optional,
      "template_id" -> Integer,
      "tree_event_witnesses" -> IntArray,
      "flat_event_witnesses" -> IntArray,
      "exercise_actors" -> IntArray,
    )

  val stringInterning: TableSchema = TableSchema("string_interning", "internal_id")(
    "internal_id" -> Integer,
    "external_string" -> Str,
  )

  val createEventsFilter: TableSchema =
    TableSchema("participant_events_create_filter", "event_sequential_id")(
      "event_sequential_id" -> BigInt,
      "template_id" -> Integer,
      "party_id" -> Integer,
    )
}
