// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.dao

import java.sql.Connection

import anorm.SqlParser.{str, _}
import anorm.{BatchSql, NamedParameter, SQL, SqlParser}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.transaction.Node.NodeCreate
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, VersionedValue}
import com.digitalasset.ledger.backend.api.v1.RejectionReason
import com.digitalasset.ledger.backend.api.v1.RejectionReason._
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry.{
  Checkpoint,
  Rejection,
  Transaction
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation.{
  ContractSerializer,
  TransactionSerializer
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.util.DbDispatcher
import com.google.common.io.ByteStreams

import scala.collection.immutable
import scala.concurrent.Future

private class PostgresLedgerDao(
    dbDispatcher: DbDispatcher,
    contractSerializer: ContractSerializer,
    transactionSerializer: TransactionSerializer)
    extends LedgerDao {

  private val LedgerIdKey = "LedgerId"
  private val LedgerEndKey = "LedgerEnd"

  override def lookupLedgerEnd(): Future[Long] =
    lookupParameter(LedgerEndKey)
      .map(_.map(_.toLong).getOrElse(sys.error("No ledger end found in database!")))(
        DirectExecutionContext)

  override def storeInitialLedgerEnd(ledgerEnd: Long): Future[Unit] =
    storeParameter(LedgerEndKey, ledgerEnd.toString)

  override def lookupLedgerId(): Future[Option[String]] =
    lookupParameter(LedgerIdKey)

  override def storeLedgerId(ledgerId: String): Future[Unit] =
    storeParameter(LedgerIdKey, ledgerId)

  private val SQL_INSERT_PARAM = "insert into parameters(key, value) values ({k}, {v})"

  private val SQL_UPDATE_PARAM = "update parameters set value = {v} where key = {k}"

  private def storeParameter(key: String, value: String): Future[Unit] =
    dbDispatcher
      .executeSql(
        implicit conn =>
          SQL(SQL_INSERT_PARAM)
            .on("k" -> key)
            .on("v" -> value)
            .execute()
      )
      .map(_ => ())(DirectExecutionContext)

  private def updateParameter(key: String, value: String)(implicit conn: Connection): Unit = {
    SQL(SQL_UPDATE_PARAM)
      .on("k" -> key)
      .on("v" -> value)
      .execute()
    ()
  }

  private val SQL_SELECT_PARAM = "select value from parameters where key = {key}"

  private def lookupParameter(key: String): Future[Option[String]] =
    dbDispatcher.executeSql(
      implicit conn =>
        SQL(SQL_SELECT_PARAM)
          .on("key" -> key)
          .as(SqlParser.str("value").singleOpt)
    )

  override def storeContract(contract: Contract): Future[Unit] = storeContracts(List(contract))

  private val SQL_INSERT_CONTRACT =
    """insert into contracts(id, transaction_id, workflow_id, package_id, module_name, entity_name, created_at, contract)
      |values({id}, {transaction_id}, {workflow_id}, {package_id}, {module_name}, {entity_name}, {created_at}, {contract})""".stripMargin

  private val SQL_INSERT_CONTRACT_WITNESS =
    "insert into contract_witnesses(contract_id, witness) values({contract_id}, {witness})"

  override def storeContracts(contracts: immutable.Seq[Contract]): Future[Unit] = {
    val namedContractParams = contracts
      .map(
        c =>
          Seq[NamedParameter](
            "id" -> c.contractId.coid,
            "transaction_id" -> c.transactionId,
            "workflow_id" -> c.workflowId,
            "package_id" -> c.coinst.template.packageId.underlyingString,
            "module_name" -> c.coinst.template.qualifiedName.module.dottedName,
            "entity_name" -> c.coinst.template.qualifiedName.name.dottedName,
            "created_at" -> c.let,
            "contract" -> contractSerializer
              .serialiseContractInstance(c.coinst)
              .getOrElse(sys.error(s"failed to serialise contract! cid:${c.contractId.coid}"))
        )
      )

    val batchInsertContracts = BatchSql(
      SQL_INSERT_CONTRACT,
      namedContractParams.head,
      namedContractParams.drop(1).toArray: _*)

    val namedWitnessesParams = contracts
      .flatMap(
        c =>
          c.witnesses.map(
            w =>
              Seq[NamedParameter](
                "contract_id" -> c.contractId.coid,
                "witness" -> w.underlyingString
            ))
      )
      .toArray

    val batchInsertWitnesses = BatchSql(
      SQL_INSERT_CONTRACT_WITNESS,
      namedWitnessesParams.head,
      namedWitnessesParams.drop(1).toArray: _*
    )

    dbDispatcher
      .executeSql { implicit conn =>
        batchInsertContracts.execute()
        batchInsertWitnesses.execute()
      }
      .map(_ => ())(DirectExecutionContext)
  }

  private val SQL_INSERT_TRANSACTION =
    """insert into ledger_entries(typ, ledger_offset, transaction_id, command_id, application_id, submitter, workflow_id, effective_at, recorded_at, transaction)
      |values('transaction', {ledger_offset}, {transaction_id}, {command_id}, {application_id}, {submitter}, {workflow_id}, {effective_at}, {recorded_at}, {transaction})""".stripMargin

  private val SQL_INSERT_REJECTION =
    """insert into ledger_entries(typ, ledger_offset, command_id, application_id, submitter, recorded_at, rejection_type, rejection_description)
      |values('rejection', {ledger_offset}, {command_id}, {application_id}, {submitter}, {recorded_at}, {rejection_type}, {rejection_description})""".stripMargin

  private val SQL_BATCH_INSERT_DISCLOSURES =
    "insert into disclosures(transaction_id, event_id, party) values({transaction_id}, {event_id}, {party})"

  private val SQL_INSERT_CHECKPOINT =
    "insert into ledger_entries(typ, ledger_offset, recorded_at) values('checkpoint', {ledger_offset}, {recorded_at})"

  override def storeLedgerEntry(offset: Long, ledgerEntry: LedgerEntry): Future[Unit] = {
    def insertBlock()(implicit conn: Connection): Unit = ledgerEntry match {
      case Transaction(
          commandId,
          transactionId,
          applicationId,
          submitter,
          workflowId,
          ledgerEffectiveTime,
          recordedAt,
          transaction,
          explicitDisclosure) =>
        // we do not support contract keys, for now
        // TODO for some reason the tests use null transactions sometimes, remove this check
        if (transaction != null) {
          transaction.foreach(
            GenTransaction.TopDown, {
              case (_, node) =>
                node match {
                  case nc: NodeCreate[AbsoluteContractId, VersionedValue[AbsoluteContractId]] =>
                    nc.key match {
                      case Some(_) => sys.error("contract keys not supported yet in SQL backend")
                      case None => ()
                    }
                  case _ => ()
                }
            }
          )
        }

        val disclosureParams = explicitDisclosure.flatMap {
          case (eventId, parties) =>
            parties.map(
              p =>
                Seq[NamedParameter](
                  "transaction_id" -> transactionId,
                  "event_id" -> eventId,
                  "party" -> p
              ))
        }

        val batchInsertDisclosures =
          BatchSql(
            SQL_BATCH_INSERT_DISCLOSURES,
            disclosureParams.head,
            disclosureParams.drop(1).toArray: _*)

        SQL(SQL_INSERT_TRANSACTION)
          .on(
            "ledger_offset" -> offset,
            "transaction_id" -> transactionId,
            "command_id" -> commandId,
            "application_id" -> applicationId,
            "submitter" -> submitter,
            "workflow_id" -> workflowId,
            "effective_at" -> ledgerEffectiveTime,
            "recorded_at" -> recordedAt,
            "transaction" -> transactionSerializer
              .serialiseTransaction(transaction)
              .getOrElse(sys.error(s"failed to serialise transaction! trId: ${transactionId}"))
          )
          .execute()
        batchInsertDisclosures.execute()
        ()

      case Rejection(recordTime, commandId, applicationId, submitter, rejectionReason) =>
        val (rejectionDescription, rejectionType) = writeRejectionReason(rejectionReason)
        SQL(SQL_INSERT_REJECTION)
          .on(
            "ledger_offset" -> offset,
            "command_id" -> commandId,
            "application_id" -> applicationId,
            "submitter" -> submitter,
            "recorded_at" -> recordTime,
            "rejection_description" -> rejectionDescription,
            "rejection_type" -> rejectionType
          )
          .execute()
        ()

      case Checkpoint(recordedAt) =>
        SQL(SQL_INSERT_CHECKPOINT)
          .on("ledger_offset" -> offset, "recorded_at" -> recordedAt)
          .execute()
        ()
    }

    dbDispatcher
      .executeSql { implicit conn =>
        insertBlock()
        updateParameter(LedgerEndKey, offset.toString)
      }
  }

  private def writeRejectionReason(rejectionReason: RejectionReason) =
    (rejectionReason.description, rejectionReason match {
      case _: Inconsistent => "Inconsistent"
      case _: OutOfQuota => "OutOfQuota"
      case _: TimedOut => "TimedOut"
      case _: Disputed => "Disputed"
      case _: DuplicateCommandId => "DuplicateCommandId"
    })

  private def readRejectionReason(rejectionType: String, description: String): RejectionReason =
    rejectionType match {
      case "Inconsistent" => Inconsistent(description)
      case "OutOfQuota" => OutOfQuota(description)
      case "TimedOut" => TimedOut(description)
      case "Disputed" => Disputed(description)
      case "DuplicateCommandId" => DuplicateCommandId(description)
      case typ => sys.error(s"unknown rejection reason: $typ")
    }

  private val SQL_SELECT_ENTRY =
    "select * from ledger_entries t where ledger_offset={ledger_offset}"

  private val SQL_SELECT_DISCLOSURE =
    "select * from disclosures where transaction_id={transaction_id}"

  private val EntryParser = (str("typ")
    ~ str("transaction_id").?
    ~ str("command_id").?
    ~ str("application_id").?
    ~ str("submitter").?
    ~ str("workflow_id").?
    ~ date("effective_at").?
    ~ date("recorded_at")
    ~ binaryStream("transaction").?
    ~ str("rejection_type").?
    ~ str("rejection_description").?
    map (flatten))

  private val DisclosureParser = (str("event_id") ~ str("party") map (flatten))

  override def lookupLedgerEntry(offset: Long): Future[Option[LedgerEntry]] = {
    dbDispatcher
      .executeSql { implicit conn =>
        SQL(SQL_SELECT_ENTRY)
          .on("ledger_offset" -> offset)
          .as(EntryParser.singleOpt)
          .map {
            case (
                "transaction",
                Some(transactionId),
                Some(commandId),
                Some(applicationId),
                Some(submitter),
                Some(workflowId),
                Some(effectiveAt),
                recordedAt,
                Some(transactionStream),
                None,
                None) =>
              val disclosure = SQL(SQL_SELECT_DISCLOSURE)
                .on("transaction_id" -> transactionId)
                .as(DisclosureParser.*)
                .groupBy(_._1)
                .mapValues(_.map(_._2).toSet)

              LedgerEntry.Transaction(
                commandId,
                transactionId,
                applicationId,
                submitter,
                workflowId,
                effectiveAt.toInstant,
                recordedAt.toInstant,
                transactionSerializer
                  .deserializeTransaction(ByteStreams.toByteArray(transactionStream))
                  .getOrElse(
                    sys.error(s"failed to deserialise transaction! trId: ${transactionId}")),
                disclosure
              )
            case (
                "rejection",
                None,
                Some(commandId),
                Some(applicationId),
                Some(submitter),
                None,
                None,
                recordedAt,
                None,
                Some(rejectionType),
                Some(rejectionDescription)) =>
              val rejectionReason = readRejectionReason(rejectionType, rejectionDescription)
              LedgerEntry.Rejection(
                recordedAt.toInstant,
                commandId,
                applicationId,
                submitter,
                rejectionReason)
            case ("checkpoint", None, None, None, None, None, None, recordedAt, None, None, None) =>
              LedgerEntry.Checkpoint(recordedAt.toInstant)
            case invalidRow =>
              sys.error(s"invalid ledger entry for offset: ${offset}. database row: $invalidRow")
          }

      }
  }

  private val ContractDataParser = (str("id")
    ~ str("transaction_id")
    ~ str("workflow_id")
    ~ str("package_id")
    ~ str("module_name")
    ~ str("entity_name")
    ~ date("created_at")
    ~ binaryStream("contract") map (flatten))

  private val SQL_SELECT_CONTRACT =
    "select * from contracts c where id={contract_id} and archived_at is null"

  private val SQL_SELECT_WITNESS =
    "select witness from contract_witnesses where contract_id={contract_id}"

  override def lookupActiveContract(contractId: AbsoluteContractId): Future[Option[Contract]] =
    dbDispatcher
      .executeSql { implicit conn =>
        val contractDetails =
          SQL(SQL_SELECT_CONTRACT)
            .on("contract_id" -> contractId.coid)
            .as(ContractDataParser.singleOpt)

        contractDetails.map {
          case (
              id,
              transactionId,
              workflowId,
              packageId,
              moduleName,
              entityName,
              createdAt,
              contractStream) =>
            val witnesses =
              SQL(SQL_SELECT_WITNESS)
                .on("contract_id" -> contractId.coid)
                .as(SqlParser.str("witness").*)
                .toSet
            Contract(
              AbsoluteContractId(id),
              createdAt.toInstant,
              transactionId,
              workflowId,
              witnesses.map(Ref.Party.assertFromString),
              contractSerializer
                .deserialiseContractInstance(ByteStreams.toByteArray(contractStream))
                .getOrElse(sys.error(s"failed to deserialise contract! cid:${contractId.coid}"))
            )
        }
      }

}

object PostgresLedgerDao {
  def apply(
      dbDispatcher: DbDispatcher,
      contractSerializer: ContractSerializer,
      transactionSerializer: TransactionSerializer): LedgerDao =
    new PostgresLedgerDao(dbDispatcher, contractSerializer, transactionSerializer)
}
