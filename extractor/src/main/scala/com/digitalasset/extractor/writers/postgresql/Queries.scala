// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.writers.postgresql

import com.digitalasset.extractor.json.JsonConverters._
import com.digitalasset.extractor.Types._
import com.digitalasset.extractor.ledger.types._
import doobie._
import doobie.implicits._
import java.time.{Instant, LocalDate}
import java.util.concurrent.TimeUnit

import scalaz._
import Scalaz._

object Queries {

  implicit val timeStampWrite: Write[LedgerValue.Timestamp] =
    Write[Instant].contramap[LedgerValue.Timestamp] { micros =>
      val seconds = TimeUnit.MICROSECONDS.toSeconds(micros.value)
      val deltaMicros = micros.value - TimeUnit.SECONDS.toMicros(seconds)
      Instant.ofEpochSecond(seconds, TimeUnit.MICROSECONDS.toNanos(deltaMicros))
    }

  def createSchema(schema: String): Fragment =
    Fragment.const(s"CREATE SCHEMA IF NOT EXISTS ${schema}")

  def setSchemaComment(schema: String, comment: String): Fragment =
    setComment("SCHEMA", schema, comment)

  def setTableComment(table: String, comment: String): Fragment =
    setComment("TABLE", table, comment)

  /**
    * PostgreSQL doesn't support DDL queries like this one as prepared statement,
    * thus parameters can't be escaped. We have to make sure to use sensible comments (no 's, etc.).
    */
  private def setComment(obj: String, name: String, comment: String): Fragment =
    Fragment.const(s"COMMENT ON ${obj} ${name} IS '${comment}'")

  val dropTransactionsTable: Fragment = dropTableIfExists("transaction")

  val createTransactionsTable: Fragment = sql"""
        CREATE TABLE
          transaction
          (transaction_id TEXT PRIMARY KEY NOT NULL
          ,seq BIGSERIAL UNIQUE NOT NULL
          ,workflow_id TEXT
          ,effective_at TIMESTAMP NOT NULL
          ,extracted_at TIMESTAMP DEFAULT NOW()
          ,ledger_offset TEXT NOT NULL
          )
      """

  val dropStateTable: Fragment = dropTableIfExists("state")

  val createStateTable: Fragment = sql"""
        CREATE TABLE IF NOT EXISTS
          state
          (key TEXT PRIMARY KEY NOT NULL,
          value TEXT NOT NULL
          )
      """

  val checkStateTableExists: Fragment = isTableExists("state")

  def getState(key: String): Fragment = {
    sql"""
      SELECT value FROM state WHERE key = ${key} LIMIT 1
    """
  }

  def setState(key: String, value: String): Fragment = {
    sql"""
      INSERT INTO
        state (key, value)
      VALUES
        (${key}, ${value})
      ON CONFLICT (key) DO UPDATE
        SET value = excluded.value
    """
  }

  def deleteState(key: String): Fragment = {
    sql"""
        DELETE FROM state WHERE key = ${key} LIMIT 1
    """
  }

  val transactionsIndex: Fragment = createIndex("transaction", NonEmptyList("workflow_id"))

  def insertTransaction(t: TransactionTree): Fragment = {
    sql"""
       INSERT INTO
         transaction
         (transaction_id, workflow_id, effective_at, ledger_offset)
         VALUES (${t.transactionId}, ${t.workflowId}, ${t.effectiveAt}, ${t.offset})
    """
  }

  def lastOffset: Fragment = {
    sql"""
       SELECT ledger_offset FROM transaction ORDER BY seq DESC LIMIT 1
    """
  }

  def dropTableIfExists(table: String): Fragment = Fragment.const(s"DROP TABLE IF EXISTS ${table}")

  def isTableExists(table: String): Fragment =
    sql"""SELECT EXISTS (
      SELECT 1
      FROM   pg_tables
      WHERE  tablename = ${table}
    );"""

  def createIndex(table: String, columns: NonEmptyList[String]): Fragment =
    Fragment.const(s"CREATE INDEX ON ${table} (${columns.stream.mkString(", ")})")

  val createExerciseTable: Fragment = sql"""
        CREATE TABLE
          exercise
          (event_id TEXT PRIMARY KEY NOT NULL
          ,transaction_id TEXT NOT NULL
          ,is_root_event BOOLEAN NOT NULL
          ,contract_id TEXT NOT NULL
          ,package_id TEXT NOT NULL
          ,template TEXT NOT NULL
          ,contract_creating_event_id TEXT NOT NULL
          ,choice TEXT NOT NULL
          ,choice_argument JSONB NOT NULL
          ,acting_parties JSONB NOT NULL
          ,consuming BOOLEAN NOT NULL
          ,witness_parties JSONB NOT NULL
          ,child_event_ids JSONB NOT NULL
          )
      """

  def insertExercise(event: ExercisedEvent, transactionId: String, isRoot: Boolean): Fragment = {
    sql"""
        INSERT INTO exercise
        VALUES (
          ${event.eventId},
          ${transactionId},
          ${isRoot},
          ${event.contractId},
          ${event.templateId.packageId},
          ${event.templateId.name},
          ${event.contractCreatingEventId},
          ${event.choice},
          ${toJsonString(event.choiceArgument)}::jsonb,
          ${toJsonString(event.actingParties)}::jsonb,
          ${event.consuming},
          ${toJsonString(event.witnessParties)}::jsonb,
          ${toJsonString(event.childEventIds)}::jsonb
        )
      """
  }

  object SingleTable {
    val dropContractsTable: Fragment = dropTableIfExists("contract")

    val createContractsTable: Fragment = sql"""
      CREATE TABLE
        contract
        (event_id TEXT PRIMARY KEY NOT NULL
        ,archived_by_event_id TEXT DEFAULT NULL
        ,contract_id TEXT NOT NULL
        ,transaction_id TEXT NOT NULL
        ,archived_by_transaction_id TEXT DEFAULT NULL
        ,is_root_event BOOLEAN NOT NULL
        ,package_id TEXT NOT NULL
        ,template TEXT NOT NULL
        ,create_arguments JSONB NOT NULL
        ,witness_parties JSONB NOT NULL
        )
    """

    def setContractArchived(
        eventId: String,
        transactionId: String,
        archivedByEventId: String): Fragment =
      sql"""
        UPDATE contract
        SET
          archived_by_transaction_id = ${transactionId},
          archived_by_event_id = ${archivedByEventId}
        WHERE event_id = ${eventId}
      """

    def insertContract(event: CreatedEvent, transactionId: String, isRoot: Boolean): Fragment =
      sql"""
        INSERT INTO contract
        VALUES (
          ${event.eventId},
          DEFAULT, -- archived_by_event_id
          ${event.contractId},
          ${transactionId},
          DEFAULT, -- archived_by_transaction_id
          ${isRoot},
          ${event.templateId.packageId},
          ${event.templateId.name},
          ${toJsonString(event.createArguments)}::jsonb,
          ${toJsonString(event.witnessParties)}::jsonb
        )
      """
  }

  object MultiTable {
    def createContractTable(table: String, columns: List[(String, String)]): Fragment = {
      val columnDefs = columns.map { case (name, typeDef) => s"$name $typeDef" } mkString (", ", ", \n", "")

      val query =
        s"""CREATE TABLE
            ${table}
            (
              _event_id TEXT PRIMARY KEY NOT NULL
              ,_archived_by_event_id TEXT DEFAULT NULL
              ,_contract_id TEXT NOT NULL
              ,_transaction_id TEXT NOT NULL
              ,_archived_by_transaction_id TEXT DEFAULT NULL
              ,_is_root_event BOOLEAN NOT NULL
              ,_witness_parties JSONB NOT NULL
              ${columnDefs}
            )
        """

      Fragment.const(query)
    }

    def setContractArchived(
        table: String,
        eventId: String,
        transactionId: String,
        archivedByEventId: String
    ): Fragment =
      Fragment.const(s"UPDATE ${table} SET ") ++
        fr"_archived_by_transaction_id = ${transactionId}, " ++
        fr"_archived_by_event_id = ${archivedByEventId} WHERE _event_id = ${eventId}"

    def insertContract(
        table: String,
        event: CreatedEvent,
        transactionId: String,
        isRoot: Boolean): Fragment = {
      // using `DEFAULT`s so there's no need to explicitly list field names (which btw aren't available in the event)
      val baseColumns = List(
        Fragment("?", event.eventId), // _event_id
        Fragment.const("DEFAULT"), // _archived_by_event_id
        Fragment("?", event.contractId), // _contract_id
        Fragment("?", transactionId), // _transaction_id
        Fragment.const("DEFAULT"), // _archived_by_transaction_id
        Fragment.const(if (isRoot) "TRUE" else "FALSE"), // _is_root_event
        Fragment("?::jsonb", toJsonString(event.witnessParties)) // _witness_parties
      )

      val contractArgColumns = event.createArguments.fields.map(f => toFragmentNullable(f.value))

      val columns = baseColumns ++ contractArgColumns

      val base = Fragment.const(
        s"INSERT INTO ${table} VALUES ("
      )

      val valueFragments = columns.intersperse(Fragment.const(", "))

      (base +: valueFragments :+ Fragment.const(")")).suml
    }

    private def toFragmentNullable(valueSum: LedgerValue): Fragment = {
      valueSum match {
        case LedgerValue.Optional(None) => Fragment.const("NULL")
        case LedgerValue.Optional(Some(innerVal)) => toFragment(innerVal)
        case _ => toFragment(valueSum)
      }
    }

    private def toFragment(valueSum: LedgerValue): Fragment = {
      valueSum match {
        case LedgerValue.Bool(value) =>
          Fragment.const(if (value) "TRUE" else "FALSE")
        case LedgerValue.Empty => Fragment.const("")
        case r @ LedgerValue.Record(_) =>
          Fragment(
            "?::jsonb",
            toJsonString(r)
          )
        case v @ LedgerValue.Variant(_, _) =>
          Fragment(
            "?::jsonb",
            toJsonString(v)
          )
        case o @ LedgerValue.Optional(_) =>
          Fragment(
            "?::jsonb",
            toJsonString(o)
          )
        case LedgerValue.ContractId(value) => Fragment("?", value)
        case l @ LedgerValue.ValueList(_) =>
          Fragment(
            "?::jsonb",
            toJsonString(l)
          )
        case LedgerValue.Int64(value) => Fragment("?", value)
        case LedgerValue.Decimal(value) => Fragment("?::numeric(38,10)", value)
        case LedgerValue.Text(value) => Fragment("?", value)
        case LedgerValue.Timestamp(value) =>
          Fragment(
            "?",
            LedgerValue.Timestamp(value)
          )
        case LedgerValue.Party(value) => Fragment("?", value)
        case LedgerValue.Unit => Fragment.const("FALSE")
        case LedgerValue.Date(value) => Fragment("?", LocalDate.ofEpochDay(value.toLong))
        case LedgerValue.ValueMap(m) =>
          Fragment(
            "?::jsonb",
            toJsonString(m)
          )
      }
    }
  }
}
