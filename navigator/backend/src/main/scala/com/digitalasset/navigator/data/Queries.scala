// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.data

import doobie._
import doobie.implicits._

object Queries {

  private def createIndex(table: String, columns: List[String], name: String): Fragment =
    Fragment.const(s"CREATE INDEX $name ON ${table} (${columns.mkString(", ")})")

  val createContractTable: Fragment = sql"""
        CREATE TABLE
          contract 
          (
            id TEXT PRIMARY KEY NOT NULL,
            template_id TEXT NOT NULL,
            archive_transaction_id TEXT DEFAULT NULL,
            argument JSON NOT NULL,
            agreement_text TEXT DEFAULT NULL,
            signatories JSON DEFAULT NULL,
            observers JSON DEFAULT NULL,
            contract_key JSON DEFAULT NULL
          )
      """

  val contractIdIndex = createIndex("contract", List("id"), "contract_id_idx")
  val contractIsActive =
    createIndex("contract", List("archive_transaction_id"), "contract_active_idx")
  val contractTemplateIdIsActive = createIndex(
    "contract",
    List("template_id", "archive_transaction_id"),
    "contract_tmplt_active_idx",
  )

  val createEventTable: Fragment = sql"""
        CREATE TABLE
          event 
          (
            id TEXT PRIMARY KEY NOT NULL,
            transaction_id TEXT NOT NULL,
            workflow_id TEXT NOT NULL,
            parent_id TEXT DEFAULT NULL,
            contract_id TEXT NOT NULL,
            witness_parties JSON NOT NULL,
            subclass_type TEXT NOT NULL,
            template_id TEXT DEFAULT NULL,
            record_argument JSON DEFAULT NULL,
            choice TEXT DEFAULT NULL,
            argument_value JSON DEFAULT NULL,
            acting_parties JSON DEFAULT NULL,
            is_consuming INTEGER DEFAULT NULL,
            agreement_text TEXT DEFAULT NULL,
            signatories JSON DEFAULT NULL,
            observers JSON DEFAULT NULL,
            contract_key JSON DEFAULT NULL
          )
      """

  val eventIdIndex = createIndex("event", List("id"), "event_id_idx")
  val eventTransactionIdParentId =
    createIndex("event", List("transaction_id", "parent_id"), "event_tx_id_idx")
  val eventContractIdSubclass =
    createIndex("event", List("contract_id", "subclass_type"), "event_cid_idx")

  val createTransactionTable: Fragment = sql"""
        CREATE TABLE
          transactions 
          (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            id TEXT UNIQUE NOT NULL,
            command_id TEXT DEFAULT NULL,
            effective_at TEXT NOT NULL,
            offset TEXT NOT NULL
          )
      """

  val transactionIdIndex = createIndex("transactions", List("id"), "tx_id_idx")

  val createCommandStatusTable: Fragment = sql"""
        CREATE TABLE
          command_status 
          (
            command_id TEXT PRIMARY KEY NOT NULL,
            is_completed INTEGER NOT NULL,
            subclass_type TEXT NOT NULL,
            code TEXT DEFAULT NULL,
            details TEXT DEFAULT NULL,
            transaction_id TEXT DEFAULT NULL
          )
      """

  val commandStatusCommandIdIndex =
    createIndex("command_status", List("command_id"), "cmd_st_id_idx")

  val createCommandTable: Fragment = sql"""
        CREATE TABLE
          command 
          (
            id TEXT PRIMARY KEY NOT NULL,
            idx INTEGER UNIQUE NOT NULL,
            workflow_id TEXT NOT NULL,
            platform_time TEXT NOT NULL,
            subclass_type TEXT NOT NULL,
            template TEXT DEFAULT NULL,
            record_argument JSON DEFAULT NULL,
            contract_id TEXT DEFAULT NULL,
            choice TEXT DEFAULT NULL,
            argument_value JSON DEFAULT NULL
          )
      """

  val commandIdIndex = createIndex("command", List("id"), "cmd_id_idx")

  def schema(): Fragment =
    sql"""SELECT sql FROM sqlite_master
      ORDER BY tbl_name, type DESC, name
    """

  def query(query: String): Fragment = {
    Fragment.const(query)
  }

  def insertEvent(row: EventRow): Fragment =
    sql"""
      INSERT INTO 
        event 
        (id, transaction_id, workflow_id, parent_id, contract_id, witness_parties, subclass_type, 
        template_id, record_argument, choice, argument_value, acting_parties, is_consuming, agreement_text, signatories, observers, contract_key)
      VALUES 
        (${row.id}, ${row.transactionId}, ${row.workflowId}, ${row.parentId}, ${row.contractId}, ${row.witnessParties}, ${row.subclassType}, 
        ${row.templateId}, ${row.recordArgument}, ${row.choice}, ${row.argumentValue}, ${row.actingParties}, ${row.isConsuming}, ${row.agreementText}, ${row.signatories}, ${row.observers}, ${row.key})
    """

  def eventById(id: String): Fragment =
    sql"""
      SELECT * FROM event WHERE id = $id LIMIT 1
    """

  def eventsByParentId(id: String): Fragment =
    sql"""
      SELECT * FROM event WHERE parent_id = $id
    """

  def topLevelEventsByTransactionId(id: String): Fragment =
    sql"""
      SELECT * FROM event WHERE transaction_id = $id AND parent_id is NULL
    """

  def eventByTypeAndContractId(subclassType: String, contractId: String): Fragment =
    sql"""
      SELECT * FROM event WHERE subclass_type = $subclassType and contract_id = $contractId
    """

  def insertTransaction(row: TransactionRow): Fragment =
    sql"""
      INSERT INTO 
        transactions 
        (id, command_id, effective_at, offset)
      VALUES 
        (${row.id}, ${row.commandId}, ${row.effectiveAt}, ${row.offset})
    """

  def transactionById(id: String): Fragment =
    sql"""
      SELECT * FROM transactions WHERE id = $id LIMIT 1
    """

  def lastTransaction(): Fragment =
    sql"""
      SELECT * FROM transactions ORDER BY seq DESC LIMIT 1
    """

  def upsertCommandStatus(row: CommandStatusRow): Fragment =
    sql"""
      INSERT INTO 
        command_status 
        (command_id, is_completed, subclass_type, code, details, transaction_id)
      VALUES 
        (${row.commandId}, ${row.isCompleted}, ${row.subclassType}, ${row.code}, ${row.details}, ${row.transactionId})
      ON CONFLICT(command_id)
      DO UPDATE 
      SET is_completed = ${row.isCompleted},
          subclass_type = ${row.subclassType},
          code = ${row.code},
          details = ${row.details},
          transaction_id = ${row.transactionId}
    """

  def updateCommandStatus(row: CommandStatusRow): Fragment =
    sql"""
      UPDATE command_status 
      SET is_completed = ${row.isCompleted},
          subclass_type = ${row.subclassType},
          code = ${row.code},
          details = ${row.details},
          transaction_id = ${row.transactionId}
       WHERE command_id = ${row.commandId}
    """

  def commandStatusByCommandId(commandId: String): Fragment =
    sql"""
      SELECT * FROM command_status WHERE command_id = $commandId LIMIT 1
    """

  def insertCommand(row: CommandRow): Fragment =
    sql"""
      INSERT INTO 
        command 
        (id, idx, workflow_id, platform_time, subclass_type, template, record_argument, contract_id, choice, argument_value)
      VALUES 
        (${row.id}, ${row.index}, ${row.workflowId}, ${row.platformTime}, ${row.subclassType}, ${row.template}, ${row.recordArgument}, ${row.contractId}, ${row.choice}, ${row.argumentValue})
    """

  def commandById(id: String): Fragment =
    sql"""
      SELECT * FROM command WHERE id = $id LIMIT 1
    """

  def allCommands(): Fragment =
    sql"""
      SELECT * FROM command
    """

  def insertContract(row: ContractRow): Fragment =
    sql"""
      INSERT INTO 
        contract 
        (id, template_id, archive_transaction_id, argument, agreement_text, signatories, observers, contract_key)
      VALUES
        (${row.id}, ${row.templateId}, ${row.archiveTransactionId}, ${row.argument}, ${row.agreementText}, ${row.signatories}, ${row.observers}, ${row.key})
    """

  def archiveContract(contractId: String, archiveTransactionId: String): Fragment =
    sql"""
        UPDATE contract SET archive_transaction_id = $archiveTransactionId WHERE id = $contractId
    """

  def contractCount(): Fragment =
    sql"""
      SELECT COUNT(*) FROM contract
    """

  def activeContractCount(): Fragment =
    sql"""
      SELECT COUNT(*) FROM contract WHERE archive_transaction_id IS NULL
    """

  def contract(id: String): Fragment =
    sql"""
      SELECT * FROM contract WHERE id = $id LIMIT 1
    """

  def contracts: Fragment =
    sql"""
      SELECT * FROM contract
    """

  def activeContracts: Fragment =
    sql"""
      SELECT * FROM contract WHERE archive_transaction_id IS NULL
    """

  def contractsForTemplate(tId: String): Fragment =
    sql"""
      SELECT * FROM contract WHERE template_id = $tId
    """

  def activeContractsForTemplate(tId: String): Fragment =
    sql"""
      SELECT * FROM contract WHERE template_id = $tId and archive_transaction_id IS NULL
    """
}
