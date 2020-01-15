// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Note: package name must correspond exactly to the flyway 'locations' setting, which defaults to
// 'db.migration.postgres' for postgres migrations
package db.migration.postgres

import java.sql.Connection
import java.util.Date

import akka.NotUsed
import akka.stream.scaladsl.Source
import anorm.SqlParser._
import anorm.{BatchSql, Macro, NamedParameter, RowParser, SQL, SqlParser}
import com.daml.ledger.participant.state.v1.AbsoluteContractInst
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractId}
import com.digitalasset.ledger._
import com.digitalasset.ledger.api.domain.RejectionReason
import com.digitalasset.ledger.api.domain.RejectionReason._
import com.digitalasset.platform.sandbox.EventIdFormatter
import com.digitalasset.platform.sandbox.stores.ActiveLedgerState.ActiveContract
import com.digitalasset.platform.sandbox.stores._
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry
import com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation.{
  ContractSerializer,
  KeyHasher,
  TransactionSerializer,
  ValueSerializer
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.util.Conversions._
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}
import org.slf4j.LoggerFactory

import scala.collection.immutable

/**
  * V1 was missing divulgence info
  * V2.0 adds corresponding new tables
  * V2.1 fills the new tables
  */
class V2_1__Rebuild_Acs extends BaseJavaMigration {

  // Serializers used in SqlLedger/PostgresLedgerDao
  private val keyHasher = KeyHasher
  private val contractSerializer = ContractSerializer
  private val transactionSerializer = TransactionSerializer
  private val valueSerializer = ValueSerializer

  private val logger = LoggerFactory.getLogger(getClass)

  private val SQL_SELECT_LEDGER_ID = SQL("select ledger_id from parameters")

  private def lookupLedgerId()(implicit conn: Connection): Option[String] =
    SQL_SELECT_LEDGER_ID
      .as(ledgerString("ledger_id").singleOpt)

  private val SQL_SELECT_LEDGER_END = SQL("select ledger_end from parameters")

  private def lookupLedgerEnd()(implicit conn: Connection): Option[Long] =
    SQL_SELECT_LEDGER_END
      .as(SqlParser.long("ledger_end").singleOpt)

  private val SQL_INITIALIZE = SQL(
    "insert into parameters(ledger_id, ledger_end) VALUES({LedgerId}, {LedgerEnd})")

  // Note that the ledger entries grow monotonically, however we store many ledger entries in parallel,
  // and thus we need to make sure to only update the ledger end when the ledger entry we're committing
  // is advancing it.
  private val SQL_UPDATE_LEDGER_END = SQL(
    "update parameters set ledger_end = {LedgerEnd} where ledger_end < {LedgerEnd}")

  private def updateLedgerEnd(ledgerEnd: Long)(implicit conn: Connection): Unit = {
    SQL_UPDATE_LEDGER_END
      .on("LedgerEnd" -> ledgerEnd)
      .execute()
    ()
  }

  private val SQL_INSERT_CONTRACT_KEY =
    SQL(
      "insert into contract_keys(package_id, name, value_hash, contract_id) values({package_id}, {name}, {value_hash}, {contract_id})")

  private val SQL_SELECT_CONTRACT_KEY =
    SQL(
      "select contract_id from contract_keys where package_id={package_id} and name={name} and value_hash={value_hash}")

  private val SQL_REMOVE_CONTRACT_KEY =
    SQL("delete from contract_keys where contract_id={contract_id}")

  private[this] def storeContractKey(key: GlobalKey, cid: AbsoluteContractId)(
      implicit connection: Connection): Boolean =
    SQL_INSERT_CONTRACT_KEY
      .on(
        "package_id" -> key.templateId.packageId,
        "name" -> key.templateId.qualifiedName.toString,
        "value_hash" -> keyHasher.hashKeyString(key),
        "contract_id" -> cid.coid
      )
      .execute()

  private[this] def removeContractKey(cid: AbsoluteContractId)(
      implicit connection: Connection): Boolean =
    SQL_REMOVE_CONTRACT_KEY
      .on(
        "contract_id" -> cid.coid,
      )
      .execute()

  private[this] def selectContractKey(key: GlobalKey)(
      implicit connection: Connection): Option[AbsoluteContractId] =
    SQL_SELECT_CONTRACT_KEY
      .on(
        "package_id" -> key.templateId.packageId,
        "name" -> key.templateId.qualifiedName.toString,
        "value_hash" -> keyHasher.hashKeyString(key)
      )
      .as(ledgerString("contract_id").singleOpt)
      .map(AbsoluteContractId)

  private def storeContract(offset: Long, contract: ActiveContract)(
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
    """insert into contracts(id, transaction_id, workflow_id, package_id, name, create_offset, contract, key)
      |values({id}, {transaction_id}, {workflow_id}, {package_id}, {name}, {create_offset}, {contract}, {key})""".stripMargin

  private val SQL_INSERT_CONTRACT_WITNESS =
    "insert into contract_witnesses(contract_id, witness) values({contract_id}, {witness})"

  private val SQL_INSERT_CONTRACT_KEY_MAINTAINERS =
    "insert into contract_key_maintainers(contract_id, maintainer) values({contract_id}, {maintainer})"

  private def storeContracts(offset: Long, contracts: immutable.Seq[ActiveContract])(
      implicit connection: Connection): Unit = {

    // A ACS contract contaixns several collections (e.g., witnesses or divulgences).
    // The contract is therefore stored in several SQL tables.

    // Part 1: insert the contract data into the 'contracts' table
    if (contracts.nonEmpty) {
      val namedContractParams = contracts
        .map(
          c =>
            Seq[NamedParameter](
              "id" -> c.id.coid,
              "transaction_id" -> c.transactionId,
              "workflow_id" -> c.workflowId.getOrElse(""),
              "package_id" -> c.contract.template.packageId,
              "name" -> c.contract.template.qualifiedName.toString,
              "create_offset" -> offset,
              "contract" -> contractSerializer
                .serializeContractInstance(c.contract)
                .getOrElse(sys.error(s"failed to serialize contract! cid:${c.id.coid}")),
              "key" -> c.key
                .map(
                  k =>
                    valueSerializer
                      .serializeValue(k.key)
                      .getOrElse(
                        sys.error(s"failed to serialize contract key value! cid:${c.id.coid}")))
          )
        )

      executeBatchSql(
        SQL_INSERT_CONTRACT,
        namedContractParams
      )

      // Part 2: insert witnesses into the 'contract_witnesses' table
      val namedWitnessesParams = contracts
        .flatMap(
          c =>
            c.witnesses.map(
              w =>
                Seq[NamedParameter](
                  "contract_id" -> c.id.coid,
                  "witness" -> w
              ))
        )
        .toArray

      if (!namedWitnessesParams.isEmpty) {
        executeBatchSql(
          SQL_INSERT_CONTRACT_WITNESS,
          namedWitnessesParams
        )
      }

      // Part 3: insert divulgences into the 'contract_divulgences' table
      val hasNonLocalDivulgence =
        contracts.exists(c => c.divulgences.exists(d => d._2 != c.transactionId))
      if (hasNonLocalDivulgence) {
        // There is at least one contract that was divulged to some party after it was commited.
        // This happens when writing contracts produced by the scenario loader.
        // Since we only have the transaction IDs when the contract was divulged, we need to look up the corresponding
        // ledger offsets.
        val namedDivulgenceParams = contracts
          .flatMap(
            c =>
              c.divulgences.map(
                w =>
                  Seq[NamedParameter](
                    "contract_id" -> c.id.coid,
                    "party" -> (w._1: String),
                    "transaction_id" -> w._2
                ))
          )
          .toArray

        if (!namedDivulgenceParams.isEmpty) {
          executeBatchSql(
            SQL_BATCH_INSERT_DIVULGENCES_FROM_TRANSACTION_ID,
            namedDivulgenceParams
          )
        }
      } else {
        val namedDivulgenceParams = contracts
          .flatMap(
            c =>
              c.divulgences.map(
                w =>
                  Seq[NamedParameter](
                    "contract_id" -> c.id.coid,
                    "party" -> (w._1: String),
                    "ledger_offset" -> offset,
                    "transaction_id" -> c.transactionId
                ))
          )
          .toArray

        if (!namedDivulgenceParams.isEmpty) {
          executeBatchSql(
            SQL_BATCH_INSERT_DIVULGENCES,
            namedDivulgenceParams
          )
        }
      }

      // Part 4: insert key maintainers into the 'contract_key_maintainers' table
      val namedKeyMaintainerParams = contracts
        .flatMap(
          c =>
            c.key
              .map(
                k =>
                  k.maintainers.map(
                    p =>
                      Seq[NamedParameter](
                        "contract_id" -> c.id.coid,
                        "maintainer" -> p
                    )))
              .getOrElse(Set.empty)
        )
        .toArray

      if (!namedKeyMaintainerParams.isEmpty) {
        executeBatchSql(
          SQL_INSERT_CONTRACT_KEY_MAINTAINERS,
          namedKeyMaintainerParams
        )
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

  // Note: the SQL backend may receive divulgence information for the same (contract, party) tuple
  // more than once through BlindingInfo.globalDivulgence.
  // The ledger offsets for the same (contract, party) tuple should always be increasing, and the database
  // stores the offset at which the contract was first disclosed.
  // We therefore don't need to update anything if there is already some data for the given (contract, party) tuple.
  private val SQL_BATCH_INSERT_DIVULGENCES =
    """insert into contract_divulgences(contract_id, party, ledger_offset, transaction_id)
    |values({contract_id}, {party}, {ledger_offset}, {transaction_id})
    |on conflict on constraint contract_divulgences_idx
    |do nothing""".stripMargin

  private val SQL_BATCH_INSERT_DIVULGENCES_FROM_TRANSACTION_ID =
    """insert into contract_divulgences(contract_id, party, ledger_offset, transaction_id)
      |select {contract_id}, {party}, ledger_offset, {transaction_id}
      |from ledger_entries
      |where transaction_id={transaction_id}
      |on conflict on constraint contract_divulgences_idx
      |do nothing""".stripMargin

  private val SQL_INSERT_CHECKPOINT =
    SQL(
      "insert into ledger_entries(typ, ledger_offset, recorded_at) values('checkpoint', {ledger_offset}, {recorded_at})")

  /**
    * Updates the active contract set from the given DAML transaction.
    * Note: This involves checking the validity of the given DAML transaction.
    * Invalid transactions trigger a rollback of the current SQL transaction.
    */
  private def updateActiveContractSet(
      offset: Long,
      tx: LedgerEntry.Transaction,
      localDivulgence: Relation[EventId, Party],
      globalDivulgence: Relation[AbsoluteContractId, Party])(
      implicit connection: Connection): Unit = tx match {
    case LedgerEntry.Transaction(
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
        .mapValues(parties => parties.map(Party.assertFromString))

      final class AcsStoreAcc extends ActiveLedgerState[AcsStoreAcc] {

        override def lookupContractByKey(key: GlobalKey): Option[AbsoluteContractId] =
          selectContractKey(key)

        override def lookupContractLet(cid: AbsoluteContractId) =
          lookupActiveContractLetSync(cid)

        override def addContract(c: ActiveLedgerState.ActiveContract, keyO: Option[GlobalKey]) = {
          storeContract(offset, c)
          keyO.foreach(key => storeContractKey(key, c.id))
          this
        }

        override def removeContract(cid: AbsoluteContractId) = {
          archiveContract(offset, cid)
          removeContractKey(cid)
          this
        }

        override def addParties(parties: Set[Party]): AcsStoreAcc = {
          // Implemented in a future migration
          this
        }

        override def divulgeAlreadyCommittedContracts(
            transactionId: TransactionIdString,
            global: Relation[AbsoluteContractId, Party],
            referencedContracts: List[(Value.AbsoluteContractId, AbsoluteContractInst)]) = {
          val divulgenceParams = global
            .flatMap {
              case (cid, parties) =>
                parties.map(
                  p =>
                    Seq[NamedParameter](
                      "contract_id" -> cid.coid,
                      "party" -> p,
                      "ledger_offset" -> offset,
                      "transaction_id" -> transactionId
                  ))
            }
          // Note: the in-memory ledger only stores divulgence for contracts in the ACS.
          // Do we need here the equivalent to 'contracts.intersectWith(global)', used in the in-memory
          // implementation of implicitlyDisclose?
          if (divulgenceParams.nonEmpty) {
            executeBatchSql(SQL_BATCH_INSERT_DIVULGENCES, divulgenceParams)
          }
          this
        }
      }

      // this should be a class member field, we can't move it out yet as the functions above are closing over to the implicit Connection
      val acsManager = new ActiveLedgerStateManager(new AcsStoreAcc)

      // Note: ACS is typed as Unit here, as the ACS is given implicitly by the current database state
      // within the current SQL transaction. All of the given functions perform side effects to update the database.
      val atr = acsManager.addTransaction(
        ledgerEffectiveTime,
        transactionId,
        workflowId,
        tx.submittingParty,
        transaction,
        mappedDisclosure,
        localDivulgence,
        globalDivulgence,
        List.empty
      )

      atr match {
        case Left(errs) =>
          // Unclear how to automatically handle this, aborting the migration.
          sys.error(
            s"""Failed to update the active contract set for transaction '$transactionId' at offset $offset.
               |This is most likely because the transaction should have been rejected, see https://github.com/digital-asset/daml/issues/10.
               |If this is the case, you will need to manually fix the transaction history and then retry.
               |Details: ${errs.map(_.toString).mkString(", ")}.
               |Aborting migration.
             """.stripMargin)
        case Right(_) =>
          ()
      }
  }

  private def storeTransaction(offset: Long, tx: LedgerEntry.Transaction)(
      implicit connection: Connection): Unit = {
    SQL_INSERT_TRANSACTION
      .on(
        "ledger_offset" -> offset,
        "transaction_id" -> tx.transactionId,
        "command_id" -> tx.commandId,
        "application_id" -> tx.applicationId,
        "submitter" -> (tx.submittingParty: Option[String]),
        "workflow_id" -> tx.workflowId,
        "effective_at" -> tx.ledgerEffectiveTime,
        "recorded_at" -> tx.recordedAt,
        "transaction" -> transactionSerializer
          .serializeTransaction(tx.transaction)
          .getOrElse(sys.error(s"failed to serialize transaction! trId: ${tx.transactionId}"))
      )
      .execute()

    val disclosureParams = tx.explicitDisclosure.flatMap {
      case (eventId, parties) =>
        parties.map(
          p =>
            Seq[NamedParameter](
              "transaction_id" -> tx.transactionId,
              "event_id" -> eventId,
              "party" -> p
          ))
    }
    if (disclosureParams.nonEmpty) {
      executeBatchSql(
        SQL_BATCH_INSERT_DISCLOSURES,
        disclosureParams
      )
    }

    ()
  }

  private def storeCheckpoint(offset: Long, checkpoint: LedgerEntry.Checkpoint)(
      implicit connection: Connection): Unit = {
    SQL_INSERT_CHECKPOINT
      .on("ledger_offset" -> offset, "recorded_at" -> checkpoint.recordedAt)
      .execute()

    ()
  }

  private def readRejectionReason(rejectionType: String, description: String): RejectionReason =
    rejectionType match {
      case "Inconsistent" => Inconsistent(description)
      case "OutOfQuota" => OutOfQuota(description)
      case "TimedOut" => TimedOut(description)
      case "Disputed" => Disputed(description)
      case typ => sys.error(s"unknown rejection reason: $typ")
    }

  private val SQL_SELECT_ENTRY =
    SQL("select * from ledger_entries where ledger_offset={ledger_offset}")

  private val SQL_SELECT_DISCLOSURE =
    SQL("select * from disclosures where transaction_id={transaction_id}")

  case class ParsedEntry(
      typ: String,
      transactionId: Option[TransactionIdString],
      commandId: Option[CommandId],
      applicationId: Option[ApplicationId],
      submitter: Option[Party],
      workflowId: Option[WorkflowId],
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

  private val DisclosureParser = (ledgerString("event_id") ~ party("party") map (flatten))

  private def toLedgerEntry(parsedEntry: ParsedEntry)(
      implicit conn: Connection): (Long, LedgerEntry) = parsedEntry match {
    case ParsedEntry(
        "transaction",
        Some(transactionId),
        Some(commandId),
        Some(applicationId),
        Some(submitter),
        workflowId,
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
        .transform((_, v) => v.map(_._2).toSet)

      offset -> LedgerEntry.Transaction(
        Some(commandId),
        transactionId,
        Some(applicationId),
        Some(submitter),
        workflowId,
        effectiveAt.toInstant,
        recordedAt.toInstant,
        transactionSerializer
          .deserializeTransaction(transactionStream)
          .getOrElse(sys.error(s"failed to deserialize transaction! trId: $transactionId")),
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

  private def lookupLedgerEntry(offset: Long)(implicit conn: Connection): Option[LedgerEntry] =
    SQL_SELECT_ENTRY
      .on("ledger_offset" -> offset)
      .as(EntryParser.singleOpt)
      .map(toLedgerEntry)
      .map(_._2)

  private val ContractDataParser = (ledgerString("id")
    ~ ledgerString("transaction_id")
    ~ ledgerString("workflow_id")
    ~ date("recorded_at")
    ~ binaryStream("contract")
    ~ binaryStream("key").?
    ~ binaryStream("transaction") map (flatten))

  private val SQL_SELECT_CONTRACT_LET =
    SQL(
      "select c.*, le.recorded_at, le.transaction from contracts c inner join ledger_entries le on c.transaction_id = le.transaction_id where id={contract_id} and archive_offset is null ")

  private val SQL_SELECT_WITNESS =
    SQL("select witness from contract_witnesses where contract_id={contract_id}")

  private val DivulgenceParser = (party("party")
    ~ long("ledger_offset")
    ~ ledgerString("transaction_id") map flatten)

  private val SQL_SELECT_DIVULGENCE =
    SQL(
      "select party, ledger_offset, transaction_id from contract_divulgences where contract_id={contract_id}")

  private val SQL_SELECT_KEY_MAINTAINERS =
    SQL("select maintainer from contract_key_maintainers where contract_id={contract_id}")

  /** Note: at the time this migration was written, divulged contracts were not stored separately from active contracts.
    * This method therefore treats all contracts as active contracts.
    */
  private def lookupActiveContractLetSync(contractId: AbsoluteContractId)(
      implicit conn: Connection): Option[LetLookup] =
    SQL_SELECT_CONTRACT_LET
      .on("contract_id" -> contractId.coid)
      .as(ContractDataParser.singleOpt)
      .map {
        case (_, _, _, let, _, _, _) => Let(let.toInstant)
      }

  private val SQL_GET_LEDGER_ENTRIES = SQL(
    "select * from ledger_entries where ledger_offset>={startInclusive} and ledger_offset<{endExclusive} order by ledger_offset asc")

  // Note that here we are reading, non transactionally, the stream in chunks. The reason why this is
  // safe is that
  // * The ledger entries are never removed;
  // * We fix the ledger end at the beginning.
  private def paginatingStream[T](
      startInclusive: Long,
      endExclusive: Long,
      pageSize: Int,
      queryPage: (Long, Long) => Source[T, NotUsed]): Source[T, NotUsed] =
    Source
      .lazySource[T, NotUsed] { () =>
        if (endExclusive - startInclusive <= pageSize)
          queryPage(startInclusive, endExclusive)
        else
          queryPage(startInclusive, startInclusive + pageSize)
            .concat(paginatingStream(startInclusive + pageSize, endExclusive, pageSize, queryPage))
      }
      .mapMaterializedValue(_ => NotUsed)

  private val PageSize = 100

  private val SQL_SELECT_ACTIVE_CONTRACTS =
    SQL(
      "select c.*, le.recorded_at, le.transaction from contracts c inner join ledger_entries le on c.transaction_id = le.transaction_id where create_offset <= {offset} and (archive_offset is null or archive_offset > {offset})")

  private def executeBatchSql(query: String, params: Iterable[Seq[NamedParameter]])(
      implicit con: Connection) = {
    require(params.size > 0, "batch sql statement must have at least one set of name parameters")
    BatchSql(query, params.head, params.drop(1).toArray: _*).execute()
  }

  private val SQL_TRUNCATE_ACS_TABLES =
    SQL(
      "truncate contracts, contract_witnesses, contract_key_maintainers, contract_keys, contract_divulgences restrict")

  private def resetAcs()(implicit conn: Connection): Unit = {
    SQL_TRUNCATE_ACS_TABLES.execute()
    ()
  }

  @throws[Exception]
  override def migrate(context: Context): Unit = {
    implicit val connection: Connection = context.getConnection

    lookupLedgerEnd().fold({
      logger.info(s"No ledger end found, assuming empty database. Skipping migration.")
    })(ledgerEnd => {
      // Instead of throwing away all ACS data and recomputing it, we could only fill the new contract_divulgences
      // table. However, it seems safer to reuse the tested ACS implementation from PostgresLedgerDao to
      // recompute the entire ACS.
      logger.info(
        s"Recomputing all active contract set data in order to add the missing divulgence info")
      resetAcs()

      logger.info(s"Processing ledger entries 0 to $ledgerEnd. This may take a while.")

      // The database might contain a large number of ledger entries, more than fits into memory.
      // Process them one by one, even if this is inefficient.
      for (offset <- 0L to ledgerEnd) {
        lookupLedgerEntry(offset)
          .collect { case tx: LedgerEntry.Transaction => tx }
          .foreach(tx => {
            // Recover the original transaction that can be used as input to Blinding.blind.
            // Here we do not convert absolute contract IDs back to relative ones,
            // as this should not affect the blinding.
            val toCoid: AbsoluteContractId => ContractId = identity
            val unmappedTx: Transaction.Transaction = tx.transaction
              .mapNodeId(EventIdFormatter.split(_).get.nodeId)
              .mapContractIdAndValue(toCoid, _.mapContractId(toCoid))

            val blindingInfo = Blinding.blind(unmappedTx)
            val mappedLocalDivulgence = blindingInfo.localDivulgence.map {
              case (k, v) => EventIdFormatter.fromTransactionId(tx.transactionId, k) -> v
            }

            updateActiveContractSet(
              offset,
              tx,
              mappedLocalDivulgence,
              blindingInfo.globalDivulgence)
          })
      }
    })
  }
}
