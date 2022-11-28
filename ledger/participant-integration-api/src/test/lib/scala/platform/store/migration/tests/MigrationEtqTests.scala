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
      SchemaSrc.createEvents ->
        // 1. a create event that matches a divulgence event by the contract id
        // 2. differing sets of tree and flat witnesses
        Seq(
          newCreateEvent(
            transaction_id = "txId1",
            contract_id = "cId1",
            event_offset = "eventOffset1",
            event_sequential_id = 101L,
            flat_event_witnesses = Vector(111),
            tree_event_witnesses = Vector(111, 112, 113),
            template_id = 121,
          )
        ),
      SchemaSrc.nonConsumingExerciseEvents -> Seq(
        newNonConsumingEvent(
          transaction_id = "txId1",
          event_offset = "eventOffset1",
          event_sequential_id = 102L,
          tree_event_witnesses = Vector(114, 115),
          template_id = 122,
        )
      ),
      SchemaSrc.consumingExerciseEvents ->
        Seq(
          newConsumingEvent(
            transaction_id = "txId1",
            event_offset = "eventOffset1",
            event_sequential_id = 103L,
            flat_event_witnesses = Vector(116, 117),
            tree_event_witnesses = Vector(116, 117, 118, 119),
            template_id = 123,
          )
        ),
      SchemaSrc.divulgenceEvents -> Seq(
        // 1. a divulgence event that matches a create event by the contract id
        // 2. present event offset
        newDivulgenceEvent(
          event_sequential_id = 104L,
          contract_id = "cId1",
          event_offset = Some("eventOffset1"),
        )
      ),
    )
    migrateTo(dstMigration)
    fetchTable(
      SchemaDst.idFilterCreateStakeholder
    ) shouldBe empty
    fetchTable(
      SchemaDst.idFilterCreateNonStakeholderInformee
    ) shouldBe Seq(
      row("event_sequential_id" -> 101L, "party_id" -> 112),
      row("event_sequential_id" -> 101L, "party_id" -> 113),
    )
    fetchTable(
      SchemaDst.idFilterNonConsumingInformee
    ) shouldBe Seq(
      row("event_sequential_id" -> 102L, "party_id" -> 114),
      row("event_sequential_id" -> 102L, "party_id" -> 115),
    )
    fetchTable(
      SchemaDst.idFilterConsumingStakeholder
    ) shouldBe Seq(
      row("event_sequential_id" -> 103L, "template_id" -> 123, "party_id" -> 116),
      row("event_sequential_id" -> 103L, "template_id" -> 123, "party_id" -> 117),
    )
    fetchTable(
      SchemaDst.idFilterConsumingNonStakeholderInformee
    ) shouldBe Seq(
      row("event_sequential_id" -> 103L, "party_id" -> 118),
      row("event_sequential_id" -> 103L, "party_id" -> 119),
    )
    fetchTable(
      SchemaDst.transactionMeta
    ) shouldBe Seq(
      row(
        "transaction_id" -> "txId1",
        "event_offset" -> "eventOffset1",
        "event_sequential_id_first" -> 101L,
        "event_sequential_id_last" -> 104L,
      )
    )
  }

  it should "migrate 2 - create, consuming and divulgence (w/o event offset) events; and identical flat and tree witnesses" in {
    migrateTo(srcMigration)
    insertMany(
      SchemaSrc.createEvents ->
        // 1. a create event that matches a divulgence event by the contract id
        // 2. equal sets of tree and flat witnesses
        Seq(
          newCreateEvent(
            transaction_id = "txId2",
            contract_id = "cId2",
            event_offset = "eventOffset2",
            event_sequential_id = 201L,
            flat_event_witnesses = Vector(211, 212),
            tree_event_witnesses = Vector(211, 212),
            template_id = 221,
          )
        ),
      SchemaSrc.consumingExerciseEvents ->
        Seq(
          newConsumingEvent(
            transaction_id = "txId2",
            event_offset = "eventOffset2",
            event_sequential_id = 202L,
            flat_event_witnesses = Vector(213, 214),
            tree_event_witnesses = Vector(213, 214),
            template_id = 222,
          )
        ),
      SchemaSrc.divulgenceEvents -> Seq(
        // 1. a divulgence event that matches a create event by the contract id
        // 2. missing event offset
        newDivulgenceEvent(
          event_sequential_id = 203L,
          contract_id = "cId2",
          event_offset = None,
        )
      ),
    )
    migrateTo(dstMigration)
    fetchTable(
      SchemaDst.idFilterCreateStakeholder
    ) shouldBe empty
    fetchTable(
      SchemaDst.idFilterCreateNonStakeholderInformee
    ) shouldBe empty
    fetchTable(
      SchemaDst.idFilterNonConsumingInformee
    ) shouldBe empty
    fetchTable(
      SchemaDst.idFilterConsumingStakeholder
    ) shouldBe Seq(
      row("event_sequential_id" -> 202L, "template_id" -> 222, "party_id" -> 213),
      row("event_sequential_id" -> 202L, "template_id" -> 222, "party_id" -> 214),
    )
    fetchTable(
      SchemaDst.idFilterConsumingNonStakeholderInformee
    ) shouldBe empty
    fetchTable(
      SchemaDst.transactionMeta
    ) shouldBe Seq(
      row(
        "transaction_id" -> "txId2",
        "event_offset" -> "eventOffset2",
        "event_sequential_id_first" -> 201L,
        "event_sequential_id_last" -> 203L,
      )
    )
  }

  it should "migrate 3 - divulgence events without corresponding create events; w/ and w/o event offset" in {
    migrateTo(srcMigration)
    insertMany(
      SchemaSrc.divulgenceEvents -> Seq(
        // 1. divulgence contract id not matching an create event
        // 2. event offset is present
        newDivulgenceEvent(
          event_sequential_id = 301L,
          contract_id = "cId301notExistent",
          event_offset = Some("eventOffset3"),
        )
      ),
      SchemaSrc.divulgenceEvents -> Seq(
        // 1. divulgence contract id not matching an create event
        // 2. event offset is missing
        newDivulgenceEvent(
          event_sequential_id = 302L,
          contract_id = "cId302notExistent",
          event_offset = None,
        )
      ),
    )
    migrateTo(dstMigration)
    // Testing that all new tables are still empty we didn't crash
    fetchTable(
      SchemaDst.idFilterCreateStakeholder
    ) shouldBe empty
    fetchTable(
      SchemaDst.idFilterCreateNonStakeholderInformee
    ) shouldBe empty
    fetchTable(
      SchemaDst.idFilterNonConsumingInformee
    ) shouldBe empty
    fetchTable(
      SchemaDst.idFilterConsumingStakeholder
    ) shouldBe empty
    fetchTable(
      SchemaDst.idFilterConsumingNonStakeholderInformee
    ) shouldBe empty
    fetchTable(
      SchemaDst.transactionMeta
    ) shouldBe empty
  }

  it should "migrate 4 - id filter for event create stakeholders" in {
    migrateTo(srcMigration)
    insertMany(
      SchemaSrc.createEventsFilter -> Seq(
        createEventsFilter(
          event_sequential_id = 401L,
          template_id = 411,
          party_id = 421,
        )
      )
    )
    migrateTo(dstMigration)
    fetchTable(
      SchemaDst.idFilterCreateStakeholder
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
      SchemaSrc.createEvents ->
        Seq(
          newCreateEvent(
            transaction_id = "txId501",
            contract_id = "cId501",
            event_offset = "eventOffset501",
            event_sequential_id = 501L,
            flat_event_witnesses = Vector.empty,
            tree_event_witnesses = Vector.empty,
            template_id = 0,
          ),
          newCreateEvent(
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
      SchemaDst.idFilterCreateStakeholder
    ) shouldBe empty
    fetchTable(
      SchemaDst.idFilterCreateNonStakeholderInformee
    ) shouldBe empty
    fetchTable(
      SchemaDst.idFilterNonConsumingInformee
    ) shouldBe empty
    fetchTable(
      SchemaDst.idFilterConsumingStakeholder
    ) shouldBe empty
    fetchTable(
      SchemaDst.idFilterConsumingNonStakeholderInformee
    ) shouldBe empty
    fetchTable(
      SchemaDst.transactionMeta
    ) shouldBe Seq(
      row(
        "transaction_id" -> "txId501",
        "event_offset" -> "eventOffset501",
        "event_sequential_id_first" -> 501L,
        "event_sequential_id_last" -> 501L,
      ),
      row(
        "transaction_id" -> "txId502",
        "event_offset" -> "eventOffset502",
        "event_sequential_id_first" -> 502L,
        "event_sequential_id_last" -> 502L,
      ),
    )
  }

  private val dbDataTypes = new DbDataTypes(dbType)
  object SchemaSrc {
    import com.daml.platform.store.migration.MigrationTestSupport._
    import dbDataTypes._

    private val commonEventsColumns: List[(String, DbDataType)] = List(
      "application_id" -> Str.optional,
      "command_id" -> Str.optional,
      "contract_id" -> Str,
      "event_sequential_id" -> BigInt,
      "submitters" -> IntArray.optional,
      "tree_event_witnesses" -> IntArray,
      "workflow_id" -> Str.optional,
    )

    private val commonTransactionEventsColumns: List[(String, DbDataType)] =
      commonEventsColumns ::: List[(String, DbDataType)](
        "event_id" -> Str,
        "event_offset" -> Str,
        "flat_event_witnesses" -> IntArray,
        "ledger_effective_time" -> BigInt,
        "node_index" -> Integer,
        "template_id" -> Integer.optional,
        "transaction_id" -> Str,
      )

    private val commonExerciseEventsColumns: List[(String, DbDataType)] =
      commonTransactionEventsColumns ::: List(
        "create_key_value" -> Bytea.optional,
        "create_key_value_compression" -> Integer.optional,
        "exercise_actors" -> IntArray,
        "exercise_argument" -> Bytea,
        "exercise_argument_compression" -> Integer.optional,
        "exercise_child_event_ids" -> StringArray,
        "exercise_choice" -> Str,
        "exercise_result" -> Bytea.optional,
        "exercise_result_compression" -> Integer.optional,
      )

    val divulgenceEvents: TableSchema =
      TableSchema("participant_events_divulgence", "event_sequential_id")(commonEventsColumns: _*)
        .++(
          "create_argument" -> Bytea.optional,
          "create_argument_compression" -> Integer.optional,
          "event_offset" -> Str.optional,
          "template_id" -> Integer.optional,
        )

    val createEvents: TableSchema = TableSchema("participant_events_create", "event_sequential_id")(
      commonTransactionEventsColumns: _*
    )
      .++(
        "create_argument" -> Bytea,
        "create_argument_compression" -> Integer.optional,
        "create_key_value" -> Bytea.optional,
        "create_key_value_compression" -> Integer.optional,
        "create_signatories" -> IntArray,
        "create_observers" -> IntArray,
        "create_agreement_text" -> Str.optional,
        "create_key_hash" -> Str.optional,
      )

    val consumingExerciseEvents: TableSchema =
      TableSchema("participant_events_consuming_exercise", "event_sequential_id")(
        commonExerciseEventsColumns: _*
      )

    val nonConsumingExerciseEvents: TableSchema =
      TableSchema("participant_events_non_consuming_exercise", "event_sequential_id")(
        commonExerciseEventsColumns: _*
      )

    val createEventsFilter: TableSchema =
      TableSchema("participant_events_create_filter", "event_sequential_id")(
        "event_sequential_id" -> BigInt,
        "template_id" -> Integer,
        "party_id" -> Integer,
      )

  }
  object SchemaDst {
    import com.daml.platform.store.migration.MigrationTestSupport._
    import dbDataTypes._

    val idFilterCreateStakeholder: TableSchema =
      TableSchema("pe_create_id_filter_stakeholder", "event_sequential_id, template_id, party_id")(
        "event_sequential_id" -> BigInt,
        "template_id" -> Integer,
        "party_id" -> Integer,
      )

    val idFilterCreateNonStakeholderInformee: TableSchema = {
      TableSchema("pe_create_id_filter_non_stakeholder_informee", "event_sequential_id, party_id")(
        "event_sequential_id" -> BigInt,
        "party_id" -> Integer,
      )
    }

    val idFilterConsumingStakeholder: TableSchema = {
      TableSchema(
        "pe_consuming_id_filter_stakeholder",
        "event_sequential_id, template_id, party_id",
      )(
        "event_sequential_id" -> BigInt,
        "template_id" -> Integer,
        "party_id" -> Integer,
      )
    }

    val idFilterConsumingNonStakeholderInformee: TableSchema = {
      TableSchema(
        "pe_consuming_id_filter_non_stakeholder_informee",
        "event_sequential_id, party_id",
      )(
        "event_sequential_id" -> BigInt,
        "party_id" -> Integer,
      )
    }

    val idFilterNonConsumingInformee: TableSchema = {
      TableSchema("pe_non_consuming_id_filter_informee", "event_sequential_id, party_id")(
        "event_sequential_id" -> BigInt,
        "party_id" -> Integer,
      )
    }

    val transactionMeta: TableSchema = {
      TableSchema("participant_transaction_meta", "transaction_id, event_offset")(
        "transaction_id" -> Str,
        "event_offset" -> Str,
        "event_sequential_id_first" -> BigInt,
        "event_sequential_id_last" -> BigInt,
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
    "template_id" -> Some(1),
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

  def newDivulgenceEvent(
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

  def newCreateEvent(
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
      "template_id" -> Some(template_id),
    )
  }

  def newConsumingEvent(
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
      "template_id" -> Some(template_id),
    )
  }

  def newNonConsumingEvent(
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
      "template_id" -> Some(template_id),
    )
  }

  def createEventsFilter(
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
