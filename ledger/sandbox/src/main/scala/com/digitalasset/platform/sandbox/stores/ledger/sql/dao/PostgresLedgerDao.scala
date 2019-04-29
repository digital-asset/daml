// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.digitalasset.platform.sandbox.stores.ledger.sql.dao

import java.io.InputStream
import java.sql.Connection
import java.util.Date

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import anorm.SqlParser.{str, _}
import anorm.{AkkaStream, BatchSql, Macro, NamedParameter, RowParser, SQL, SqlParser}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.transaction.Node.{GlobalKey, KeyWithMaintainers}
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.backend.api.v1.RejectionReason
import com.digitalasset.ledger.backend.api.v1.RejectionReason._
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.sandbox.stores._
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry.{
  Checkpoint,
  Rejection,
  Transaction
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation.{
  ContractSerializer,
  TransactionSerializer,
  KeyHasher,
  ValueSerializer
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.util.DbDispatcher
import com.google.common.io.ByteStreams
import org.slf4j.LoggerFactory

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Try
import scala.util.control.NonFatal

private class PostgresLedgerDao(
    dbDispatcher: DbDispatcher,
    contractSerializer: ContractSerializer,
    transactionSerializer: TransactionSerializer,
    valueSerializer: ValueSerializer,
    keyHasher: KeyHasher)
    extends LedgerDao {

  private val logger = LoggerFactory.getLogger(getClass)
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

  private val SQL_INSERT_PARAM = SQL("insert into parameters(key, value) values ({k}, {v})")

  private val SQL_UPDATE_PARAM = SQL("update parameters set value = {v} where key = {k}")

  private def storeParameter(key: String, value: String): Future[Unit] =
    dbDispatcher
      .executeSql(
        implicit conn =>
          SQL_INSERT_PARAM
            .on("k" -> key)
            .on("v" -> value)
            .execute()
      )
      .map(_ => ())(DirectExecutionContext)

  private def updateParameter(key: String, value: String)(implicit conn: Connection): Unit = {
    SQL_UPDATE_PARAM
      .on("k" -> key)
      .on("v" -> value)
      .execute()
    ()
  }

  private val SQL_SELECT_PARAM = SQL("select value from parameters where key = {key}")

  private def lookupParameter(key: String): Future[Option[String]] =
    dbDispatcher.executeSql(
      implicit conn =>
        SQL_SELECT_PARAM
          .on("key" -> key)
          .as(SqlParser.str("value").singleOpt)
    )

  private val SQL_INSERT_CONTRACT_KEY =
    SQL(
      "insert into contract_keys(package_id, name, value_hash, contract_id) values({package_id}, {name}, {value_hash}, {contract_id})")

  private val SQL_SELECT_CONTRACT_KEY =
    SQL(
      "select contract_id from contract_keys where package_id={package_id} and name={name} and value_hash={value_hash}")

  private val SQL_REMOVE_CONTRACT_KEY =
    SQL(
      "delete from contract_keys where package_id={package_id} and name={name} and value_hash={value_hash}")

  private[this] def storeContractKey(key: GlobalKey, cid: AbsoluteContractId)(
      implicit connection: Connection): Boolean =
    SQL_INSERT_CONTRACT_KEY
      .on(
        "package_id" -> key.templateId.packageId.underlyingString,
        "name" -> key.templateId.qualifiedName.toString,
        "value_hash" -> keyHasher.hashKeyString(key),
        "contract_id" -> cid.coid
      )
      .execute()

  private[this] def removeContractKey(key: GlobalKey)(implicit connection: Connection): Boolean =
    SQL_REMOVE_CONTRACT_KEY
      .on(
        "package_id" -> key.templateId.packageId.underlyingString,
        "name" -> key.templateId.qualifiedName.toString,
        "value_hash" -> keyHasher.hashKeyString(key)
      )
      .execute()

  private[this] def selectContractKey(key: GlobalKey)(
      implicit connection: Connection): Option[AbsoluteContractId] =
    SQL_SELECT_CONTRACT_KEY
      .on(
        "package_id" -> key.templateId.packageId.underlyingString,
        "name" -> key.templateId.qualifiedName.toString,
        "value_hash" -> keyHasher.hashKeyString(key)
      )
      .as(SqlParser.str("contract_id").singleOpt)
      .map(s => AbsoluteContractId(s))

  override def lookupKey(key: Node.GlobalKey): Future[Option[AbsoluteContractId]] =
    dbDispatcher.executeSql(implicit conn => selectContractKey(key))

  private def storeContract(offset: Long, contract: Contract)(
      implicit connection: Connection): Unit = storeContracts(offset, List(contract))

  private def archiveContract(offset: Long, cid: AbsoluteContractId)(
      implicit connection: Connection): Boolean =
    SQL_ARCHIVE_CONTRACT
      .on(
        "id" -> cid.coid,
        "archive_offset" -> offset
      )
      .execute()

  private val SQL_INSERT_CONTRACT =
    """insert into contracts(id, transaction_id, workflow_id, package_id, module_name, entity_name, create_offset, contract, key)
      |values({id}, {transaction_id}, {workflow_id}, {package_id}, {module_name}, {entity_name}, {create_offset}, {contract}, {key})""".stripMargin

  private val SQL_INSERT_CONTRACT_WITNESS =
    "insert into contract_witnesses(contract_id, witness) values({contract_id}, {witness})"

  private val SQL_INSERT_CONTRACT_KEY_MAINTAINERS =
    "insert into contract_key_maintainers(contract_id, maintainer) values({contract_id}, {maintainer})"

  private def storeContracts(offset: Long, contracts: immutable.Seq[Contract])(
      implicit connection: Connection): Unit = {

    if (!contracts.isEmpty) {
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
              "create_offset" -> offset,
              "contract" -> contractSerializer
                .serialiseContractInstance(c.coinst)
                .getOrElse(sys.error(s"failed to serialise contract! cid:${c.contractId.coid}")),
              "key" -> c.key
                .map(
                  k =>
                    valueSerializer
                      .serialiseValue(k.key)
                      .getOrElse(sys.error(
                        s"failed to serialise contract key value! cid:${c.contractId.coid}")))
          )
        )

      val batchInsertContracts = BatchSql(
        SQL_INSERT_CONTRACT,
        namedContractParams.head,
        namedContractParams.drop(1).toArray: _*)

      batchInsertContracts.execute()

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

      if (!namedWitnessesParams.isEmpty) {
        val batchInsertWitnesses = BatchSql(
          SQL_INSERT_CONTRACT_WITNESS,
          namedWitnessesParams.head,
          namedWitnessesParams.drop(1).toArray: _*
        )
        batchInsertWitnesses.execute()
      }

      val namedKeyMaintainerParams = contracts
        .flatMap(
          c =>
            c.key
              .map(
                k =>
                  k.maintainers.map(
                    p =>
                      Seq[NamedParameter](
                        "contract_id" -> c.contractId.coid,
                        "maintainer" -> p.underlyingString
                    )))
              .getOrElse(Set.empty)
        )
        .toArray

      if (!namedKeyMaintainerParams.isEmpty) {
        val batchInsertKeyMaintainers = BatchSql(
          SQL_INSERT_CONTRACT_KEY_MAINTAINERS,
          namedKeyMaintainerParams.head,
          namedKeyMaintainerParams.drop(1).toArray: _*
        )
        batchInsertKeyMaintainers.execute()
      }
    }
    ()
  }

  private val SQL_ARCHIVE_CONTRACT =
    SQL("""update contracts set archive_offset = {archive_offset} where id = {id}""")

  private val SQL_INSERT_TRANSACTION =
    SQL(
      """insert into ledger_entries(typ, ledger_offset, transaction_id, command_id, application_id, submitter, workflow_id, effective_at, recorded_at, transaction)
        |values('transaction', {ledger_offset}, {transaction_id}, {command_id}, {application_id}, {submitter}, {workflow_id}, {effective_at}, {recorded_at}, {transaction})""".stripMargin)

  private val SQL_INSERT_REJECTION =
    SQL(
      """insert into ledger_entries(typ, ledger_offset, command_id, application_id, submitter, recorded_at, rejection_type, rejection_description)
        |values('rejection', {ledger_offset}, {command_id}, {application_id}, {submitter}, {recorded_at}, {rejection_type}, {rejection_description})""".stripMargin)

  private val SQL_BATCH_INSERT_DISCLOSURES =
    "insert into disclosures(transaction_id, event_id, party) values({transaction_id}, {event_id}, {party})"

  private val SQL_INSERT_CHECKPOINT =
    SQL(
      "insert into ledger_entries(typ, ledger_offset, recorded_at) values('checkpoint', {ledger_offset}, {recorded_at})")

  /**
    * Updates the active contract set from the given DAML transaction.
    * Note: This involves checking the validity of the given DAML transaction.
    * Invalid transactions trigger a rollback of the current SQL transaction.
    */
  private def updateActiveContractSet(offset: Long, tx: Transaction)(
      implicit connection: Connection): Option[RejectionReason] = tx match {
    case Transaction(
        _,
        transactionId,
        _,
        _,
        workflowId,
        ledgerEffectiveTime,
        _,
        transaction,
        explicitDisclosure) =>
      val mappedDisclosure = explicitDisclosure
        .map {
          case (nodeId, party) =>
            nodeId -> party.map(p => Ref.Party.assertFromString(p))
        }

      final class AcsStoreAcc extends ActiveContracts[AcsStoreAcc] {

        def lookupContract(cid: AbsoluteContractId) =
          lookupActiveContractSync(cid).map(_.toActiveContract)

        def keyExists(key: GlobalKey): Boolean = selectContractKey(key).isDefined

        def addContract(
            cid: AbsoluteContractId,
            c: ActiveContracts.ActiveContract,
            keyO: Option[GlobalKey]) = {
          storeContract(offset, Contract.fromActiveContract(cid, c))
          keyO.foreach(key => storeContractKey(key, cid))
          this
        }

        def removeContract(cid: AbsoluteContractId, keyO: Option[GlobalKey]) = {
          archiveContract(offset, cid)
          keyO.foreach(key => removeContractKey(key))
          this
        }

        // TODO need another table for this, or alter same table as `addContract` uses
        def implicitlyDisclose(global: Relation[AbsoluteContractId, Ref.Party]) =
          this
      }

      //this should be a class member field, we can't move it out yet as the functions above are closing over to the implicit Connection
      val acsManager = new ActiveContractsManager(new AcsStoreAcc)

      // Note: ACS is typed as Unit here, as the ACS is given implicitly by the current database state
      // within the current SQL transaction. All of the given functions perform side effects to update the database.
      val atr = acsManager.addTransaction[LedgerEntry.EventId](
        ledgerEffectiveTime,
        transactionId,
        workflowId,
        transaction,
        mappedDisclosure,
        // TODO blind `transaction` for these two maps, reenable SandboxSemanticTestsLfRunner and Memory-only test in CommandTransactionChecks
        Map.empty,
        Map.empty
      )

      atr match {
        case Left(err) =>
          Some(RejectionReason.Inconsistent(s"Reason: ${err.mkString("[", ", ", "]")}"))
        case Right(_) => None
      }
  }

  //TODO: test it for failures..
  override def storeLedgerEntry(
      offset: Long,
      newLedgerEnd: Long,
      ledgerEntry: LedgerEntry): Future[PersistenceResponse] = {
    import PersistenceResponse._

    def insertEntry(le: LedgerEntry)(implicit conn: Connection): PersistenceResponse = le match {
      case tx @ Transaction(
            commandId,
            transactionId,
            applicationId,
            submitter,
            workflowId,
            ledgerEffectiveTime,
            recordedAt,
            transaction,
            explicitDisclosure) =>
        Try {
          SQL_INSERT_TRANSACTION
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
          if (!disclosureParams.isEmpty) {
            val batchInsertDisclosures =
              BatchSql(
                SQL_BATCH_INSERT_DISCLOSURES,
                disclosureParams.head,
                disclosureParams.drop(1).toArray: _*)
            batchInsertDisclosures.execute()
          }

          updateActiveContractSet(offset, tx).fold[PersistenceResponse](Ok) { rejectionReason =>
            // we need to rollback the existing sql transaction
            conn.rollback()
            insertEntry(
              Rejection(
                recordedAt,
                commandId,
                applicationId,
                submitter,
                rejectionReason
              ))
          }
        }.recover {
          case NonFatal(e) if (e.getMessage.contains("duplicate key")) =>
            logger.warn(
              "Ignoring duplicate submission for applicationId {}, commandId {}",
              tx.applicationId: Any,
              tx.commandId)
            conn.rollback()
            Duplicate
        }.get

      case Rejection(recordTime, commandId, applicationId, submitter, rejectionReason) =>
        val (rejectionDescription, rejectionType) = writeRejectionReason(rejectionReason)
        SQL_INSERT_REJECTION
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
        Ok

      case Checkpoint(recordedAt) =>
        SQL_INSERT_CHECKPOINT
          .on("ledger_offset" -> offset, "recorded_at" -> recordedAt)
          .execute()
        Ok
    }

    dbDispatcher
      .executeSql { implicit conn =>
        val resp = insertEntry(ledgerEntry)
        updateParameter(LedgerEndKey, newLedgerEnd.toString)
        resp
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
    SQL("select * from ledger_entries where ledger_offset={ledger_offset}")

  private val SQL_SELECT_DISCLOSURE =
    SQL("select * from disclosures where transaction_id={transaction_id}")

  case class ParsedEntry(
      typ: String,
      transactionId: Option[String],
      commandId: Option[String],
      applicationId: Option[String],
      submitter: Option[String],
      workflowId: Option[String],
      effectiveAt: Option[Date],
      recordedAt: Option[Date],
      transaction: Option[Array[Byte]],
      rejectionType: Option[String],
      rejectionDesc: Option[String],
      offset: Long)

  private val EntryParser: RowParser[ParsedEntry] =
    Macro.parser[ParsedEntry](
      "typ",
      "transaction_id",
      "command_id",
      "application_id",
      "submitter",
      "workflow_id",
      "effective_at",
      "recorded_at",
      "transaction",
      "rejection_type",
      "rejection_description",
      "ledger_offset"
    )

  private val DisclosureParser = (str("event_id") ~ str("party") map (flatten))

  private def toLedgerEntry(parsedEntry: ParsedEntry)(
      implicit conn: Connection): (Long, LedgerEntry) = parsedEntry match {
    case ParsedEntry(
        "transaction",
        Some(transactionId),
        Some(commandId),
        Some(applicationId),
        Some(submitter),
        Some(workflowId),
        Some(effectiveAt),
        Some(recordedAt),
        Some(transactionStream),
        None,
        None,
        offset) =>
      val disclosure = SQL_SELECT_DISCLOSURE
        .on("transaction_id" -> transactionId)
        .as(DisclosureParser.*)
        .groupBy(_._1)
        .mapValues(_.map(_._2).toSet)

      offset -> LedgerEntry.Transaction(
        commandId,
        transactionId,
        applicationId,
        submitter,
        workflowId,
        effectiveAt.toInstant,
        recordedAt.toInstant,
        transactionSerializer
          .deserializeTransaction(transactionStream)
          .getOrElse(sys.error(s"failed to deserialise transaction! trId: ${transactionId}")),
        disclosure
      )
    case ParsedEntry(
        "rejection",
        None,
        Some(commandId),
        Some(applicationId),
        Some(submitter),
        None,
        None,
        Some(recordedAt),
        None,
        Some(rejectionType),
        Some(rejectionDescription),
        offset) =>
      val rejectionReason = readRejectionReason(rejectionType, rejectionDescription)
      offset -> LedgerEntry
        .Rejection(recordedAt.toInstant, commandId, applicationId, submitter, rejectionReason)
    case ParsedEntry(
        "checkpoint",
        None,
        None,
        None,
        None,
        None,
        None,
        Some(recordedAt),
        None,
        None,
        None,
        offset) =>
      offset -> LedgerEntry.Checkpoint(recordedAt.toInstant)
    case invalidRow =>
      sys.error(s"invalid ledger entry for offset: ${invalidRow.offset}. database row: $invalidRow")
  }

  override def lookupLedgerEntry(offset: Long): Future[Option[LedgerEntry]] = {
    dbDispatcher
      .executeSql { implicit conn =>
        SQL_SELECT_ENTRY
          .on("ledger_offset" -> offset)
          .as(EntryParser.singleOpt)
          .map(toLedgerEntry)
          .map(_._2)
      }
  }

  private val ContractDataParser = (str("id")
    ~ str("transaction_id")
    ~ str("workflow_id")
    ~ date("recorded_at")
    ~ binaryStream("contract")
    ~ binaryStream("key").? map (flatten))

  private val SQL_SELECT_CONTRACT =
    SQL(
      "select c.*, le.recorded_at from contracts c inner join ledger_entries le on c.transaction_id = le.transaction_id where id={contract_id} and archive_offset is null ")

  private val SQL_SELECT_WITNESS =
    SQL("select witness from contract_witnesses where contract_id={contract_id}")

  private val SQL_SELECT_KEY_MAINTAINERS =
    SQL("select maintainer from contract_key_maintainers where contract_id={contract_id}")

  private def lookupActiveContractSync(contractId: AbsoluteContractId)(
      implicit conn: Connection): Option[Contract] =
    SQL_SELECT_CONTRACT
      .on("contract_id" -> contractId.coid)
      .as(ContractDataParser.singleOpt)
      .map(mapContractDetails)

  override def lookupActiveContract(contractId: AbsoluteContractId): Future[Option[Contract]] =
    dbDispatcher.executeSql { implicit conn =>
      lookupActiveContractSync(contractId)
    }

  private def mapContractDetails(
      contractResult: (String, String, String, Date, InputStream, Option[InputStream]))(
      implicit conn: Connection) =
    contractResult match {
      case (coid, transactionId, workflowId, createdAt, contractStream, keyStreamO) =>
        val witnesses = lookupWitnesses(coid)

        Contract(
          AbsoluteContractId(coid),
          createdAt.toInstant,
          transactionId,
          workflowId,
          witnesses.map(Ref.Party.assertFromString),
          contractSerializer
            .deserialiseContractInstance(ByteStreams.toByteArray(contractStream))
            .getOrElse(sys.error(s"failed to deserialise contract! cid:$coid")),
          keyStreamO.map(keyStream => {
            val keyMaintainers = lookupKeyMaintainers(coid)
            val keyValue = valueSerializer
              .deserialiseValue(ByteStreams.toByteArray(keyStream))
              .getOrElse(sys.error(s"failed to deserialise key value! cid:$coid"))
            KeyWithMaintainers(keyValue, keyMaintainers)
          })
        )
    }

  private def lookupWitnesses(coid: String)(implicit conn: Connection) =
    SQL_SELECT_WITNESS
      .on("contract_id" -> coid)
      .as(SqlParser.str("witness").*)
      .toSet

  private def lookupKeyMaintainers(coid: String)(implicit conn: Connection) =
    SQL_SELECT_KEY_MAINTAINERS
      .on("contract_id" -> coid)
      .as(SqlParser.str("maintainer").*)
      .toSet
      .map(Ref.Party.assertFromString)

  private val SQL_GET_LEDGER_ENTRIES = SQL(
    "select * from ledger_entries where ledger_offset>={startInclusive} and ledger_offset<{endExclusive} order by ledger_offset asc")

  //TODO we should use paging instead, check if Alpakka can do that?
  override def getLedgerEntries(
      startInclusive: Long,
      endExclusive: Long): Source[(Long, LedgerEntry), NotUsed] =
    Source
      .fromFuture(dbDispatcher.executeSql { implicit conn =>
        SQL_GET_LEDGER_ENTRIES
          .on("startInclusive" -> startInclusive, "endExclusive" -> endExclusive)
          .as(EntryParser.*)
          .map(toLedgerEntry)
      })
      .flatMapConcat(Source(_))

  private val SQL_SELECT_ACTIVE_CONTRACTS =
    SQL(
      "select c.*, le.recorded_at from contracts c inner join ledger_entries le on c.transaction_id = le.transaction_id where create_offset <= {offset} and (archive_offset is null or archive_offset > {offset})")

  override def getActiveContractSnapshot()(implicit mat: Materializer): Future[LedgerSnapshot] = {

    def contractStream(conn: Connection, offset: Long) = {
      //TODO: investigate where Akka Streams is actually iterating on the JDBC ResultSet (because, that is blocking IO!)
      AkkaStream
        .source(SQL_SELECT_ACTIVE_CONTRACTS.on("offset" -> offset), ContractDataParser)(mat, conn)
        .mapAsync(dbDispatcher.noOfShortLivedConnections) { contractResult =>
          // it's ok to not have query isolation as witnesses cannot change once we saved them
          dbDispatcher
            .executeSql { implicit conn =>
              mapContractDetails(contractResult)
            }
        }
    }.mapMaterializedValue(_.map(_ => Done)(DirectExecutionContext))

    lookupLedgerEnd()
      .map(offset =>
        LedgerSnapshot(offset, dbDispatcher.runStreamingSql(conn => contractStream(conn, offset))))(
        DirectExecutionContext)
  }

  private val SQL_TRUNCATE_ALL_TABLES =
    SQL("""
        |truncate ledger_entries cascade;
        |truncate disclosures cascade;
        |truncate contracts cascade;
        |truncate contract_witnesses cascade;
        |truncate contract_key_maintainers cascade;
        |truncate parameters cascade;
        |truncate contract_keys cascade;
      """.stripMargin)

  override def reset(): Future[Unit] =
    dbDispatcher.executeSql { implicit conn =>
      val _ = SQL_TRUNCATE_ALL_TABLES.execute()
      ()
    }

  override def close(): Unit =
    dbDispatcher.close()

}

object PostgresLedgerDao {
  def apply(
      dbDispatcher: DbDispatcher,
      contractSerializer: ContractSerializer,
      transactionSerializer: TransactionSerializer,
      valueSerializer: ValueSerializer,
      keyHasher: KeyHasher): LedgerDao =
    new PostgresLedgerDao(
      dbDispatcher,
      contractSerializer,
      transactionSerializer,
      valueSerializer,
      keyHasher)
}
