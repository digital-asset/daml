// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.migration.tests

import com.daml.platform.store.migration.MigrationTestSupport.{Row, row}
import com.daml.platform.store.migration.{DbConnectionAroundEachBase, DbDataTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

abstract class MigrationEtqTests extends AnyFlatSpec with Matchers with DbConnectionAroundEachBase {
  import MigrationEtqTests._
  import com.daml.platform.store.migration.MigrationTestSupport._

  def srcMigration: String
  def dstMigration: String

  behavior of s"Data migrations from v$srcMigration to v$dstMigration (ETQ)"

  it should "migrate 1 - create, consuming, non-consuming and divulgence (w/ event offset) events; and distinct flat and tree witnesses" in {
    migrateTo(srcMigration)
    insertMany(
      Schema125.createEvents ->
        // 1. a create event that matches a divulgence event by the contract id
        // 2. differing sets of tree and flat witnesses
        Seq(
          newCreateEvent_125(
            transaction_id = "txId1",
            contract_id = "cId1",
            event_offset = "eventOffset1",
            event_sequential_id = 101L,
            flat_event_witnesses = Vector(111),
            tree_event_witnesses = Vector(111, 112, 113),
            template_id = 121,
          )
        ),
      Schema125.nonConsumingExerciseEvents -> Seq(
        newNonConsumingEvent_125(
          transaction_id = "txId1",
          event_offset = "eventOffset1",
          event_sequential_id = 102L,
          tree_event_witnesses = Vector(114, 115),
          template_id = 122,
        )
      ),
      Schema125.consumingExerciseEvents ->
        Seq(
          newConsumingEvent_125(
            transaction_id = "txId1",
            event_offset = "eventOffset1",
            event_sequential_id = 103L,
            flat_event_witnesses = Vector(116, 117),
            tree_event_witnesses = Vector(116, 117, 118, 119),
            template_id = 123,
          )
        ),
      Schema125.divulgenceEvents -> Seq(
        // 1. a divulgence event that matches a create event by the contract id
        // 2. present event offset
        newDivulgenceEvent_125(
          event_sequential_id = 104L,
          contract_id = "cId1",
          event_offset = Some("eventOffset1"),
        )
      ),
    )
    migrateTo(dstMigration)
    migrateTo(dstMigration)
    fetchTable(
      Schema126.idFilterCreateStakeholder
    ) shouldBe empty
    fetchTable(
      Schema126.idFilterCreateNonStakeholderInformee
    ) shouldBe Seq(
      row("event_sequential_id" -> 101L, "party_id" -> 112),
      row("event_sequential_id" -> 101L, "party_id" -> 113),
    )
    fetchTable(
      Schema126.idFilterNonConsumingInformee
    ) shouldBe Seq(
      row("event_sequential_id" -> 102L, "party_id" -> 114),
      row("event_sequential_id" -> 102L, "party_id" -> 115),
    )
    fetchTable(
      Schema126.idFilterConsumingStakeholder
    ) shouldBe Seq(
      row("event_sequential_id" -> 103L, "template_id" -> 123, "party_id" -> 116),
      row("event_sequential_id" -> 103L, "template_id" -> 123, "party_id" -> 117),
    )
    fetchTable(
      Schema126.idFilterConsumingNonStakeholderInformee
    ) shouldBe Seq(
      row("event_sequential_id" -> 103L, "party_id" -> 118),
      row("event_sequential_id" -> 103L, "party_id" -> 119),
    )
    fetchTable(
      Schema126.transactionMeta
    ) shouldBe Seq(
      row(
        "transaction_id" -> "txId1",
        "event_offset" -> "eventOffset1",
        "event_sequential_id_from" -> 101L,
        "event_sequential_id_to" -> 104L,
      )
    )
  }

  it should "migrate 2 - create, consuming and divulgence (w/o event offset) events; and identical flat and tree witnesses" in {
    migrateTo(srcMigration)
    insertMany(
      Schema125.createEvents ->
        // 1. a create event that matches a divulgence event by the contract id
        // 2. equal sets of tree and flat witnesses
        Seq(
          newCreateEvent_125(
            transaction_id = "txId2",
            contract_id = "cId2",
            event_offset = "eventOffset2",
            event_sequential_id = 201L,
            flat_event_witnesses = Vector(211, 212),
            tree_event_witnesses = Vector(211, 212),
            template_id = 221,
          )
        ),
      Schema125.consumingExerciseEvents ->
        Seq(
          newConsumingEvent_125(
            transaction_id = "txId2",
            event_offset = "eventOffset2",
            event_sequential_id = 202L,
            flat_event_witnesses = Vector(213, 214),
            tree_event_witnesses = Vector(213, 214),
            template_id = 222,
          )
        ),
      Schema125.divulgenceEvents -> Seq(
        // 1. a divulgence event that matches a create event by the contract id
        // 2. missing event offset
        newDivulgenceEvent_125(
          event_sequential_id = 203L,
          contract_id = "cId2",
          event_offset = None,
        )
      ),
    )
    migrateTo(dstMigration)
    fetchTable(
      Schema126.idFilterCreateStakeholder
    ) shouldBe empty
    fetchTable(
      Schema126.idFilterCreateNonStakeholderInformee
    ) shouldBe empty
    fetchTable(
      Schema126.idFilterNonConsumingInformee
    ) shouldBe empty
    fetchTable(
      Schema126.idFilterConsumingStakeholder
    ) shouldBe Seq(
      row("event_sequential_id" -> 202L, "template_id" -> 222, "party_id" -> 213),
      row("event_sequential_id" -> 202L, "template_id" -> 222, "party_id" -> 214),
    )
    fetchTable(
      Schema126.idFilterConsumingNonStakeholderInformee
    ) shouldBe empty
    fetchTable(
      Schema126.transactionMeta
    ) shouldBe Seq(
      row(
        "transaction_id" -> "txId2",
        "event_offset" -> "eventOffset2",
        "event_sequential_id_from" -> 201L,
        "event_sequential_id_to" -> 203L,
      )
    )
  }

  it should "migrate 3 - divulgence events without corresponding create events; w/ and w/o event offset" in {
    migrateTo(srcMigration)
    insertMany(
      Schema125.divulgenceEvents -> Seq(
        // 1. divulgence contract id not matching an create event
        // 2. event offset is present
        newDivulgenceEvent_125(
          event_sequential_id = 301L,
          contract_id = "cId301notExistent",
          event_offset = Some("eventOffset3"),
        )
      ),
      Schema125.divulgenceEvents -> Seq(
        // 1. divulgence contract id not matching an create event
        // 2. event offset is missing
        newDivulgenceEvent_125(
          event_sequential_id = 302L,
          contract_id = "cId302notExistent",
          event_offset = None,
        )
      ),
    )
    migrateTo(dstMigration)
    // Testing that all new tables are still empty we didn't crash
    fetchTable(
      Schema126.idFilterCreateStakeholder
    ) shouldBe empty
    fetchTable(
      Schema126.idFilterCreateNonStakeholderInformee
    ) shouldBe empty
    fetchTable(
      Schema126.idFilterNonConsumingInformee
    ) shouldBe empty
    fetchTable(
      Schema126.idFilterConsumingStakeholder
    ) shouldBe empty
    fetchTable(
      Schema126.idFilterConsumingNonStakeholderInformee
    ) shouldBe empty
    fetchTable(
      Schema126.transactionMeta
    ) shouldBe empty
  }

  it should "migrate 4 - id filter for event create stakeholders" in {
    migrateTo(srcMigration)
    insertMany(
      Schema125.createEventsFilter -> Seq(
        createEventsFilter_125(
          event_sequential_id = 401L,
          template_id = 411,
          party_id = 421,
        )
      )
    )
    migrateTo(dstMigration)
    fetchTable(
      Schema126.idFilterCreateStakeholder
    ) shouldBe Seq(
      row(
        "event_sequential_id" -> 401L,
        "template_id" -> 411,
        "party_id" -> 421,
      )
    )
  }

  it should "migrate 5 - two transactions" in {
    migrateTo(srcMigration)
    insertMany(
      Schema125.createEvents ->
        Seq(
          newCreateEvent_125(
            transaction_id = "txId501",
            contract_id = "cId501",
            event_offset = "eventOffset501",
            event_sequential_id = 501L,
            flat_event_witnesses = Vector.empty,
            tree_event_witnesses = Vector.empty,
            template_id = 0,
          ),
          newCreateEvent_125(
            transaction_id = "txId502",
            contract_id = "cId502",
            event_offset = "eventOffset502",
            event_sequential_id = 502L,
            flat_event_witnesses = Vector.empty,
            tree_event_witnesses = Vector.empty,
            template_id = 0,
          ),
        )
    )
    migrateTo(dstMigration)
    fetchTable(
      Schema126.idFilterCreateStakeholder
    ) shouldBe empty
    fetchTable(
      Schema126.idFilterCreateNonStakeholderInformee
    ) shouldBe empty
    fetchTable(
      Schema126.idFilterNonConsumingInformee
    ) shouldBe empty
    fetchTable(
      Schema126.idFilterConsumingStakeholder
    ) shouldBe empty
    fetchTable(
      Schema126.idFilterConsumingNonStakeholderInformee
    ) shouldBe empty
    fetchTable(
      Schema126.transactionMeta
    ) shouldBe Seq(
      row(
        "transaction_id" -> "txId501",
        "event_offset" -> "eventOffset501",
        "event_sequential_id_from" -> 501L,
        "event_sequential_id_to" -> 501L,
      ),
      row(
        "transaction_id" -> "txId502",
        "event_offset" -> "eventOffset502",
        "event_sequential_id_from" -> 502L,
        "event_sequential_id_to" -> 502L,
      ),
    )
  }

  private val dbDataTypes = new DbDataTypes(dbType)
  object Schema125 {
    import com.daml.platform.store.migration.MigrationTestSupport._
    import dbDataTypes._

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
  object Schema126 {
    import com.daml.platform.store.migration.MigrationTestSupport._
    import dbDataTypes._

    val idFilterCreateStakeholder: TableSchema =
      TableSchema("participant_events_create_filter", "event_sequential_id, template_id, party_id")(
        "event_sequential_id" -> BigInt,
        "template_id" -> Integer,
        "party_id" -> Integer,
      )

    val idFilterCreateNonStakeholderInformee: TableSchema = {
      TableSchema("pe_create_filter_nonstakeholder_informees", "event_sequential_id, party_id")(
        "event_sequential_id" -> BigInt,
        "party_id" -> Integer,
      )
    }

    val idFilterConsumingStakeholder: TableSchema = {
      TableSchema(
        "pe_consuming_exercise_filter_stakeholders",
        "event_sequential_id, template_id, party_id",
      )(
        "event_sequential_id" -> BigInt,
        "template_id" -> Integer,
        "party_id" -> Integer,
      )
    }

    val idFilterConsumingNonStakeholderInformee: TableSchema = {
      TableSchema(
        "pe_consuming_exercise_filter_nonstakeholder_informees",
        "event_sequential_id, party_id",
      )(
        "event_sequential_id" -> BigInt,
        "party_id" -> Integer,
      )
    }

    val idFilterNonConsumingInformee: TableSchema = {
      TableSchema("pe_non_consuming_exercise_filter_informees", "event_sequential_id, party_id")(
        "event_sequential_id" -> BigInt,
        "party_id" -> Integer,
      )
    }

    val transactionMeta: TableSchema = {
      TableSchema("participant_transaction_meta", "transaction_id, event_offset")(
        "transaction_id" -> Str,
        "event_offset" -> Str,
        "event_sequential_id_from" -> BigInt,
        "event_sequential_id_to" -> BigInt,
      )
    }

  }
}

private object MigrationEtqTests {

  val baseEvent: Row = row(
    "event_sequential_id" -> 0L,
    "workflow_id" -> None,
    "command_id" -> None,
    "application_id" -> None,
    "submitters" -> None,
    "contract_id" -> "contract id",
    "tree_event_witnesses" -> Vector(),
  )

  val baseDivulgenceEvent: Row = row(
    "event_offset" -> None,
    "template_id" -> None,
    "create_argument" -> None,
    "create_argument_compression" -> None,
  ) ++ baseEvent

  val baseTransactionEvent: Row = row(
    "ledger_effective_time" -> 0L,
    "event_offset" -> "some offset",
    "node_index" -> 0,
    "transaction_id" -> "some tx id",
    "event_id" -> "some event id",
    "template_id" -> 1,
    "flat_event_witnesses" -> Vector(),
  ) ++ baseEvent

  val baseCreateEvent: Row = row(
    "create_argument" -> Vector(1.toByte, 2.toByte, 3.toByte),
    "create_argument_compression" -> None,
    "create_key_value" -> None,
    "create_key_value_compression" -> None,
    "create_signatories" -> Vector(),
    "create_observers" -> Vector(),
    "create_agreement_text" -> None,
    "create_key_hash" -> None,
  ) ++ baseTransactionEvent

  val baseExerciseEvent: Row = row(
    "exercise_argument" -> Vector(3.toByte, 2.toByte, 1.toByte),
    "exercise_argument_compression" -> None,
    "exercise_result" -> None,
    "exercise_result_compression" -> None,
    "create_key_value" -> None,
    "create_key_value_compression" -> None,
    "exercise_choice" -> "some choice",
    "exercise_actors" -> Vector(),
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
      template_id: Int,
  ): Row = {
    baseCreateEvent ++ row(
      "transaction_id" -> transaction_id,
      "contract_id" -> contract_id,
      "event_sequential_id" -> event_sequential_id,
      "event_offset" -> event_offset,
      "flat_event_witnesses" -> flat_event_witnesses,
      "tree_event_witnesses" -> tree_event_witnesses,
      "template_id" -> template_id,
    )
  }

  def newConsumingEvent_125(
      transaction_id: String,
      event_sequential_id: Long,
      event_offset: String,
      flat_event_witnesses: Vector[Int],
      tree_event_witnesses: Vector[Int],
      template_id: Int,
  ): Row = {
    baseExerciseEvent ++ row(
      "transaction_id" -> transaction_id,
      "event_sequential_id" -> event_sequential_id,
      "event_offset" -> event_offset,
      "flat_event_witnesses" -> flat_event_witnesses,
      "tree_event_witnesses" -> tree_event_witnesses,
      "template_id" -> template_id,
    )
  }

  def newNonConsumingEvent_125(
      transaction_id: String,
      event_sequential_id: Long,
      event_offset: String,
      tree_event_witnesses: Vector[Int],
      template_id: Int,
  ): Row = {
    baseExerciseEvent ++ row(
      "transaction_id" -> transaction_id,
      "event_sequential_id" -> event_sequential_id,
      "event_offset" -> event_offset,
      "flat_event_witnesses" -> Vector.empty,
      "tree_event_witnesses" -> tree_event_witnesses,
      "template_id" -> template_id,
    )
  }

  def createEventsFilter_125(
      event_sequential_id: Long,
      template_id: Int,
      party_id: Int,
  ): Row = {
    row(
      "event_sequential_id" -> event_sequential_id,
      "template_id" -> template_id,
      "party_id" -> party_id,
    )
  }

}
