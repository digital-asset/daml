// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package platform.store.migration.postgres

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PostgresMigrationFrom111To116Test
    extends AnyFlatSpec
    with Matchers
    with PostgresConnectionSupport {
  import com.daml.platform.store.migration.MigrationTestSupport._

  behavior of "Data migrations from version 125 to version 126 (ETQ: Efficient Transaction Queries)"

  private val baseEvent = row(
    "event_sequential_id" -> 0L, // to be overridden
    "workflow_id" -> None,
    "command_id" -> None,
    "application_id" -> None,
    "submitters" -> None, // to be overridden
    "contract_id" -> "contract id",
    "tree_event_witnesses" -> Vector(), // to be overridden
  )

  private val baseDivulgenceEvent = row(
    "event_offset" -> None,
    "template_id" -> None, // to be overridden
    "create_argument" -> None,
    "create_argument_compression" -> None,
  ) ++ baseEvent

  private val baseTransactionEvent = row(
    "ledger_effective_time" -> 0L,
    "event_offset" -> "some offset",
    "node_index" -> 0,
    "transaction_id" -> "some tx id",
    "event_id" -> "some event id",
    "template_id" -> "some template id", // to be overridden
    "flat_event_witnesses" -> Vector(), // to be overridden
  ) ++ baseEvent

  private val baseCreateEvent = row(
    "create_argument" -> Vector(1.toByte, 2.toByte, 3.toByte),
    "create_argument_compression" -> None,
    "create_key_value" -> None,
    "create_key_value_compression" -> None,
    "create_signatories" -> Vector(), // to be overridden
    "create_observers" -> Vector(), // to be overridden
    "create_agreement_text" -> None,
    "create_key_hash" -> None,
  ) ++ baseTransactionEvent

  it should "migrate correctly data for a database with transactions" in {
    migrateTo("125")

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

    def newDivulgenceEvent_125(
        event_sequential_id: Long,
        contract_id: String,
        event_offset: Option[String],
    ): Row = {
      baseDivulgenceEvent ++ row(
        "event_sequential_id" -> event_sequential_id,
        "contract_id" -> contract_id,
        "event_offset" -> event_offset,
      )
    }

    def newCreateEvent_125(
        transaction_id: String,
        contract_id: String,
        event_sequential_id: Long,
        event_offset: String,
        flat_event_witnesses: Vector[Int],
        tree_event_witnesses: Vector[Int],
    ): Row = {
      baseCreateEvent ++ row(
        "transaction_id" -> transaction_id,
        "contract_id" -> contract_id,
        "event_sequential_id" -> event_sequential_id,
        "event_offset" -> event_offset,
        "flat_event_witnesses" -> flat_event_witnesses,
        "tree_event_witnesses" -> tree_event_witnesses,
      )
    }

    insert(
      Schema125.divulgenceEvents,
      newDivulgenceEvent_125(
        event_sequential_id = 1L,
        contract_id = "cid1",
        event_offset = Some("eventOffset1"),
      ),
      newDivulgenceEvent_125(
        event_sequential_id = 2L,
        contract_id = "cid2NullEventOffset",
        event_offset = None,
      ),
      newDivulgenceEvent_125(
        event_sequential_id = 2L,
        contract_id = "cid3NonExistent",
        event_offset = None,
      ),
    )

    insert(
      Schema111.createEvents,
      newCreateEvent_125(
        transaction_id = "txId1",
        contract_id = "cid1",
        event_offset = "eventOffset1",
        event_sequential_id = 3L,
        flat_event_witnesses = Vector(1, 2),
        tree_event_witnesses = Vector(1, 2, 3, 4),
      ),
      newCreateEvent_125(
        transaction_id = "txId2",
        contract_id = "cid2",
        event_offset = "eventOffset2",
        event_sequential_id = 4L,
        flat_event_witnesses = Vector(1, 2),
        tree_event_witnesses = Vector(1, 2),
      ),
    )

    def newConsumingEvent_125(
        transaction_id: String,
        event_sequential_id: Long,
        event_offset: String,
        flat_event_witnesses: Vector[Int],
        tree_event_witnesses: Vector[Int],
    ): Row = {
      baseExerciseEvent ++ row(
        "transaction_id" -> transaction_id,
        "event_sequential_id" -> event_sequential_id,
        "event_offset" -> event_offset,
        "flat_event_witnesses" -> flat_event_witnesses,
        "tree_event_witnesses" -> tree_event_witnesses,
      )
    }

    def newNonConsumingEvent_125(
        transaction_id: String,
        event_sequential_id: Long,
        event_offset: String,
        flat_event_witnesses: Vector[Int],
        tree_event_witnesses: Vector[Int],
    ): Row = {
      baseExerciseEvent ++ row(
        "transaction_id" -> transaction_id,
        "event_sequential_id" -> event_sequential_id,
        "event_offset" -> event_offset,
        "flat_event_witnesses" -> flat_event_witnesses,
        "tree_event_witnesses" -> tree_event_witnesses,
      )
    }

    insert(
      Schema111.consumingExerciseEvents,
      newConsumingEvent_125(
        transaction_id = "txId2",
        event_offset = "eventOffset2",
        event_sequential_id = 4L,
        flat_event_witnesses = Vector(1, 2),
        tree_event_witnesses = Vector(1, 2),
      ),
    )

    insert(
      Schema111.nonConsumingExerciseEvents,
      newNonConsumingEvent_125(
        transaction_id = "txId2",
        event_offset = "eventOffset2",
        event_sequential_id = 4L,
        flat_event_witnesses = Vector(1, 2),
        tree_event_witnesses = Vector(1, 2),
      ),
    )

    migrateTo("126")

//    val (unInternParty, unInternTemplate) = fetchTable(Schema116.stringInterning)
//      .foldLeft((Map.empty[Int, String], Map.empty[Int, String])) {
//        case ((partyMap, templateIdMap), dbEntry) =>
//          val id = dbEntry("internal_id").asInstanceOf[Int]
//          dbEntry("external_string").asInstanceOf[String].split("\\|").toList match {
//            case "p" :: party :: Nil => (partyMap + (id -> party), templateIdMap)
//            case "t" :: templateId :: Nil => (partyMap, templateIdMap + (id -> templateId))
//            case _ =>
//              throw new Exception(s"external_string ${dbEntry("external_string")} is invalid")
//          }
//      }
//
//    fetchTable(Schema116.parameters) shouldBe Vector(
//      params + ("ledger_end_string_interning_id" -> Some(
//        unInternParty.size + unInternTemplate.size
//      ))
//    )
//
//    fetchTable(Schema116.partyEntries)
//      .updateInAll[Option[Int]]("party_id")(_.map(unInternParty)) shouldBe Vector(
//      partyEntry + ("party_id" -> Some("party1")),
//      partyRejected + ("party_id" -> None),
//    )
//
//    fetchTable(Schema116.commandCompletions)
//      .updateInAll[Vector[Int]]("submitters")(_.map(unInternParty)) shouldBe completions
//
//    fetchTable(Schema116.divulgenceEvents)
//      .updateInAll[Option[Vector[Int]]]("submitters")(_.map(_.map(unInternParty)))
//      .updateInAll[Option[Int]]("template_id")(_.map(unInternTemplate))
//      .updateInAll[Vector[Int]]("tree_event_witnesses")(_.map(unInternParty)) shouldBe divulgences
//
//    fetchTable(Schema116.createEvents)
//      .updateInAll[Option[Vector[Int]]]("submitters")(_.map(_.map(unInternParty)))
//      .updateInAll[Int]("template_id")(unInternTemplate)
//      .updateInAll[Vector[Int]]("tree_event_witnesses")(_.map(unInternParty))
//      .updateInAll[Vector[Int]]("flat_event_witnesses")(_.map(unInternParty))
//      .updateInAll[Vector[Int]]("create_signatories")(_.map(unInternParty))
//      .updateInAll[Vector[Int]]("create_observers")(_.map(unInternParty)) shouldBe createEvents
//
//    fetchTable(Schema116.createEventsFilter)
//      .updateInAll[Int]("template_id")(unInternTemplate)
//      .updateInAll[Int]("party_id")(unInternParty)
//      .toSet shouldBe createEvents
//      .flatMap(createEvent =>
//        createEvent("flat_event_witnesses")
//          .asInstanceOf[Vector[String]]
//          .map(flatWitness =>
//            row(
//              "event_sequential_id" -> createEvent("event_sequential_id"),
//              "template_id" -> createEvent("template_id"),
//              "party_id" -> flatWitness,
//            )
//          )
//      )
//      .toSet
//
//    fetchTable(Schema116.consumingExerciseEvents)
//      .updateInAll[Option[Vector[Int]]]("submitters")(_.map(_.map(unInternParty)))
//      .updateInAll[Int]("template_id")(unInternTemplate)
//      .updateInAll[Vector[Int]]("tree_event_witnesses")(_.map(unInternParty))
//      .updateInAll[Vector[Int]]("flat_event_witnesses")(_.map(unInternParty))
//      .updateInAll[Vector[Int]]("exercise_actors")(
//        _.map(unInternParty)
//      ) shouldBe consumingExerciseEvents
//
//    fetchTable(Schema116.nonConsumingExerciseEvents)
//      .updateInAll[Option[Vector[Int]]]("submitters")(_.map(_.map(unInternParty)))
//      .updateInAll[Int]("template_id")(unInternTemplate)
//      .updateInAll[Vector[Int]]("tree_event_witnesses")(_.map(unInternParty))
//      .updateInAll[Vector[Int]]("flat_event_witnesses")(_.map(unInternParty))
//      .updateInAll[Vector[Int]]("exercise_actors")(
//        _.map(unInternParty)
//      ) shouldBe nonConsumingExerciseEvents
  }

  // TODO pbatko
//  it should "migrate correctly data for a database without transactions" in {
//    migrateTo("111")
//
//    val params = row(
//      "ledger_id" -> "lid",
//      "participant_id" -> "pid",
//      "ledger_end" -> Some("abcdefg"),
//      "ledger_end_sequential_id" -> Some(200L),
//      "participant_pruned_up_to_inclusive" -> None,
//      "participant_all_divulged_contracts_pruned_up_to_inclusive" -> None,
//    )
//    insert(Schema111.parameters, params)
//
//    migrateTo("116")
//    fetchTable(Schema116.parameters) shouldBe Vector(
//      params + ("ledger_end_string_interning_id" -> Some(0))
//    )
//  }
}

object Schema125 {
  import PostgresDbDataType._
  import com.daml.platform.store.migration.MigrationTestSupport._

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

  val divulgenceEvents: TableSchema =
    TableSchema("participant_events_divulgence", "event_sequential_id")(commonEventsColumns: _*)
      .++(
        "event_offset" -> Str.optional,
        "template_id" -> Str.optional,
        "create_argument" -> Bytea.optional,
        "create_argument_compression" -> Integer.optional,
      ) ++ (
      "submitters" -> IntArray.optional,
      "template_id" -> Integer.optional,
      "tree_event_witnesses" -> IntArray,
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
    .++(
      "submitters" -> IntArray.optional,
      "template_id" -> Integer,
      "tree_event_witnesses" -> IntArray,
      "flat_event_witnesses" -> IntArray,
      "create_signatories" -> IntArray,
      "create_observers" -> IntArray,
    )

  val consumingExerciseEvents: TableSchema =
    TableSchema("participant_events_consuming_exercise", "event_sequential_id")(
      commonExerciseEventsColumns: _*
    )
      .++(
        "submitters" -> IntArray.optional,
        "template_id" -> Integer,
        "tree_event_witnesses" -> IntArray,
        "flat_event_witnesses" -> IntArray,
        "exercise_actors" -> IntArray,
      )

  val nonConsumingExerciseEvents: TableSchema =
    TableSchema("participant_events_non_consuming_exercise", "event_sequential_id")(
      commonExerciseEventsColumns: _*
    )
      .++(
        "submitters" -> IntArray.optional,
        "template_id" -> Integer,
        "tree_event_witnesses" -> IntArray,
        "flat_event_witnesses" -> IntArray,
        "exercise_actors" -> IntArray,
      )

  val createEventsFilter: TableSchema =
    TableSchema("participant_events_create_filter", "event_sequential_id")(
      "event_sequential_id" -> BigInt,
      "template_id" -> Integer,
      "party_id" -> Integer,
    )
}
