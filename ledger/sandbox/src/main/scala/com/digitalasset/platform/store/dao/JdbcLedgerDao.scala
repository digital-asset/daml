// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.dao

import java.io.InputStream
import java.sql.Connection
import java.time.Instant
import java.util.concurrent.Executors
import java.util.{Date, UUID}

import akka.NotUsed
import akka.stream.scaladsl.Source
import anorm.SqlParser._
import anorm.ToStatement.optionToStatement
import anorm.{BatchSql, Macro, NamedParameter, ResultSetParser, RowParser, SQL, SqlParser}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.index.v2.{
  CommandDeduplicationDuplicate,
  CommandDeduplicationNew,
  CommandDeduplicationResult,
  PackageDetails
}
import com.daml.ledger.participant.state.v1._
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{PackageId, Party}
import com.daml.lf.data.Relation.Relation
import com.daml.lf.transaction.Node
import com.daml.lf.transaction.Node.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{AbsoluteContractId, ContractInst, NodeId}
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.RejectionReason._
import com.daml.ledger.api.domain.{
  Filters,
  InclusiveFilters,
  LedgerId,
  PartyDetails,
  RejectionReason,
  TransactionFilter
}
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.{ApplicationId, CommandId, EventId, WorkflowId}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.daml.platform.configuration.ServerRole
import com.daml.platform.events.EventIdFormatter.split
import com.daml.platform.store.Contract.ActiveContract
import com.daml.platform.store.Conversions._
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store._
import com.daml.platform.store.dao.JdbcLedgerDao.{H2DatabaseQueries, PostgresQueries}
import com.daml.platform.store.dao.events.{ContractsReader, TransactionsReader, TransactionsWriter}
import com.daml.platform.store.entries.LedgerEntry.Transaction
import com.daml.platform.store.entries.{
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry
}
import com.daml.platform.store.serialization.{
  ContractSerializer,
  KeyHasher,
  TransactionSerializer,
  ValueSerializer
}
import com.daml.resources.ResourceOwner
import com.google.common.util.concurrent.ThreadFactoryBuilder
import scalaz.syntax.tag._

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

private final case class ParsedEntry(
    typ: String,
    transactionId: Option[TransactionId],
    commandId: Option[CommandId],
    applicationId: Option[ApplicationId],
    submitter: Option[Party],
    workflowId: Option[WorkflowId],
    effectiveAt: Option[Date],
    recordedAt: Option[Date],
    transaction: Option[InputStream],
    rejectionType: Option[String],
    rejectionDesc: Option[String],
    offset: Offset,
)

private final case class ParsedPartyData(
    party: String,
    displayName: Option[String],
    ledgerOffset: Offset,
    explicit: Boolean)

private final case class ParsedPackageData(
    packageId: String,
    sourceDescription: Option[String],
    size: Long,
    knownSince: Date)

private final case class ParsedCommandData(deduplicateUntil: Instant)

private class JdbcLedgerDao(
    override val maxConcurrentConnections: Int,
    dbDispatcher: DbDispatcher,
    contractSerializer: ContractSerializer,
    transactionSerializer: TransactionSerializer,
    keyHasher: KeyHasher,
    dbType: DbType,
    executionContext: ExecutionContext,
    eventsPageSize: Int,
)(implicit logCtx: LoggingContext)
    extends LedgerDao {

  private val queries = dbType match {
    case DbType.Postgres => PostgresQueries
    case DbType.H2Database => H2DatabaseQueries
  }

  private val logger = ContextualizedLogger.get(this.getClass)

  private val SQL_SELECT_LEDGER_ID = SQL("select ledger_id from parameters")

  override def currentHealth(): HealthStatus = dbDispatcher.currentHealth()

  override def lookupLedgerId(): Future[Option[LedgerId]] =
    dbDispatcher
      .executeSql("get_ledger_id") { implicit conn =>
        SQL_SELECT_LEDGER_ID
          .as(ledgerString("ledger_id").map(id => LedgerId(id.toString)).singleOpt)
      }

  private val SQL_SELECT_LEDGER_END = SQL("select ledger_end from parameters")

  override def lookupLedgerEnd(): Future[Offset] =
    dbDispatcher.executeSql("get_ledger_end") { implicit conn =>
      SQL_SELECT_LEDGER_END
        .as(offset("ledger_end").single)
    }

  private val SQL_SELECT_INITIAL_LEDGER_END = SQL("select ledger_end from parameters")

  override def lookupInitialLedgerEnd(): Future[Option[Offset]] =
    dbDispatcher.executeSql("get_initial_ledger_end") { implicit conn =>
      SQL_SELECT_INITIAL_LEDGER_END
        .as(offset("ledger_end").?.single)
    }

  private val SQL_INITIALIZE = SQL(
    "insert into parameters(ledger_id, ledger_end) VALUES({LedgerId}, {LedgerEnd})")

  override def initializeLedger(ledgerId: LedgerId, ledgerEnd: Offset): Future[Unit] =
    dbDispatcher.executeSql("initialize_ledger_parameters") { implicit conn =>
      val _ = SQL_INITIALIZE
        .on("LedgerId" -> ledgerId.unwrap, "LedgerEnd" -> ledgerEnd)
        .execute()
      ()
    }

  // Note that the ledger entries grow monotonically, however we store many ledger entries in parallel,
  // and thus we need to make sure to only update the ledger end when the ledger entry we're committing
  // is advancing it.
  private val SQL_UPDATE_LEDGER_END = SQL(
    "update parameters set ledger_end = {LedgerEnd} where ledger_end < {LedgerEnd}")

  private def updateLedgerEnd(ledgerEnd: Offset)(implicit conn: Connection): Unit = {
    SQL_UPDATE_LEDGER_END
      .on("LedgerEnd" -> ledgerEnd)
      .execute()
    ()
  }

  private val SQL_UPDATE_CURRENT_CONFIGURATION = SQL(
    "update parameters set configuration={configuration}"
  )
  private val SQL_SELECT_CURRENT_CONFIGURATION = SQL(
    "select ledger_end, configuration from parameters")

  private val SQL_GET_CONFIGURATION_ENTRIES = SQL(
    "select * from configuration_entries where ledger_offset > {startExclusive} and ledger_offset <= {endInclusive} order by ledger_offset asc limit {pageSize} offset {queryOffset}")

  private def updateCurrentConfiguration(configBytes: Array[Byte])(
      implicit conn: Connection): Unit = {
    SQL_UPDATE_CURRENT_CONFIGURATION
      .on("configuration" -> configBytes)
      .execute()
    ()
  }

  private val currentConfigurationParser: ResultSetParser[Option[(Offset, Configuration)]] =
    (offset("ledger_end") ~
      byteArray("configuration").? map flatten).single
      .map {
        case (_, None) => None
        case (offset, Some(configBytes)) =>
          Configuration
            .decode(configBytes)
            .toOption
            .map(config => offset -> config)
      }

  private def selectLedgerConfiguration(implicit conn: Connection) =
    SQL_SELECT_CURRENT_CONFIGURATION.as(currentConfigurationParser)

  override def lookupLedgerConfiguration(): Future[Option[(Offset, Configuration)]] =
    dbDispatcher.executeSql("lookup_configuration")(implicit conn => selectLedgerConfiguration)

  private val acceptType = "accept"
  private val rejectType = "reject"

  private val configurationEntryParser: RowParser[(Offset, ConfigurationEntry)] =
    (offset("ledger_offset") ~
      str("typ") ~
      str("submission_id") ~
      str("participant_id") ~
      str("rejection_reason").map(s => if (s.isEmpty) null else s).? ~
      byteArray("configuration"))
      .map(flatten)
      .map {
        case (offset, typ, submissionId, participantIdRaw, rejectionReason, configBytes) =>
          val config = Configuration
            .decode(configBytes)
            .fold(err => sys.error(s"Failed to decode configuration: $err"), identity)
          val participantId = ParticipantId
            .fromString(participantIdRaw)
            .fold(
              err => sys.error(s"Failed to decode participant id in configuration entry: $err"),
              identity)

          offset ->
            (typ match {
              case `acceptType` =>
                ConfigurationEntry.Accepted(
                  submissionId = submissionId,
                  participantId = participantId,
                  configuration = config
                )
              case `rejectType` =>
                ConfigurationEntry.Rejected(
                  submissionId = submissionId,
                  participantId = participantId,
                  rejectionReason = rejectionReason.getOrElse("<missing reason>"),
                  proposedConfiguration = config
                )

              case _ =>
                sys.error(s"getConfigurationEntries: Unknown configuration entry type: $typ")
            })
      }

  override def getConfigurationEntries(
      startExclusive: Offset,
      endInclusive: Offset): Source[(Offset, ConfigurationEntry), NotUsed] =
    PaginatingAsyncStream(PageSize) { queryOffset =>
      dbDispatcher.executeSql(
        "load_configuration_entries",
        Some(
          s"bounds: ]${startExclusive.toApiString}, ${endInclusive.toApiString}] queryOffset $queryOffset")) {
        implicit conn =>
          SQL_GET_CONFIGURATION_ENTRIES
            .on(
              "startExclusive" -> startExclusive,
              "endInclusive" -> endInclusive,
              "pageSize" -> PageSize,
              "queryOffset" -> queryOffset)
            .asVectorOf(configurationEntryParser)
      }
    }

  private val SQL_INSERT_CONFIGURATION_ENTRY =
    SQL(
      """insert into configuration_entries(ledger_offset, recorded_at, submission_id, participant_id, typ, rejection_reason, configuration)
        |values({ledger_offset}, {recorded_at}, {submission_id}, {participant_id}, {typ}, {rejection_reason}, {configuration})
        |""".stripMargin)

  override def storeConfigurationEntry(
      offset: Offset,
      recordedAt: Instant,
      submissionId: String,
      participantId: ParticipantId,
      configuration: Configuration,
      rejectionReason: Option[String]
  ): Future[PersistenceResponse] = {
    dbDispatcher.executeSql("store_configuration_entry", Some(s"submissionId=$submissionId")) {
      implicit conn =>
        val optCurrentConfig = selectLedgerConfiguration
        val optExpectedGeneration: Option[Long] =
          optCurrentConfig.map { case (_, c) => c.generation + 1 }
        val finalRejectionReason: Option[String] =
          optExpectedGeneration match {
            case Some(expGeneration)
                if rejectionReason.isEmpty && expGeneration != configuration.generation =>
              // If we're not storing a rejection and the new generation is not succ of current configuration, then
              // we store a rejection. This code path is only expected to be taken in sandbox. This follows the same
              // pattern as with transactions.
              Some(
                s"Generation mismatch: expected=$expGeneration, actual=${configuration.generation}")

            case _ =>
              // Rejection reason was set, or we have no previous configuration generation, in which case we accept any
              // generation.
              rejectionReason
          }

        updateLedgerEnd(offset)
        val configurationBytes = Configuration.encode(configuration).toByteArray
        val typ = if (finalRejectionReason.isEmpty) {
          acceptType
        } else {
          rejectType
        }

        Try({
          SQL_INSERT_CONFIGURATION_ENTRY
            .on(
              "ledger_offset" -> offset,
              "recorded_at" -> recordedAt,
              "submission_id" -> submissionId,
              "participant_id" -> participantId,
              "typ" -> typ,
              "rejection_reason" -> finalRejectionReason.orNull,
              "configuration" -> configurationBytes
            )
            .execute()

          if (typ == acceptType) {
            updateCurrentConfiguration(configurationBytes)
          }

          PersistenceResponse.Ok
        }).recover {
          case NonFatal(e) if e.getMessage.contains(queries.DUPLICATE_KEY_ERROR) =>
            logger.warn(
              s"Ignoring duplicate configuration submission, submissionId=$submissionId, participantId=$participantId")
            conn.rollback()
            PersistenceResponse.Duplicate
        }.get

    }
  }

  private val SQL_INSERT_PARTY_ENTRY_ACCEPT =
    SQL(
      """insert into party_entries(ledger_offset, recorded_at, submission_id, participant_id, typ, party, display_name, is_local)
        |values ({ledger_offset}, {recorded_at}, {submission_id}, {participant_id}, 'accept', {party}, {display_name}, {is_local})
        |""".stripMargin)

  private val SQL_INSERT_PARTY_ENTRY_REJECT =
    SQL(
      """insert into party_entries(ledger_offset, recorded_at, submission_id, participant_id, typ, rejection_reason)
        |values ({ledger_offset}, {recorded_at}, {submission_id}, {participant_id}, 'reject', {rejection_reason})
        |""".stripMargin)

  override def storePartyEntry(
      offset: Offset,
      partyEntry: PartyLedgerEntry,
  ): Future[PersistenceResponse] = {
    dbDispatcher.executeSql("store_party_entry") { implicit conn =>
      updateLedgerEnd(offset)

      partyEntry match {
        case PartyLedgerEntry.AllocationAccepted(
            submissionIdOpt,
            participantId,
            recordTime,
            partyDetails) =>
          Try({
            SQL_INSERT_PARTY_ENTRY_ACCEPT
              .on(
                "ledger_offset" -> offset,
                "recorded_at" -> recordTime,
                "submission_id" -> submissionIdOpt,
                "participant_id" -> participantId,
                "party" -> partyDetails.party,
                "display_name" -> partyDetails.displayName,
                "is_local" -> partyDetails.isLocal,
              )
              .execute()
            SQL_INSERT_PARTY
              .on(
                "party" -> partyDetails.party,
                "display_name" -> partyDetails.displayName,
                "ledger_offset" -> offset,
              )
              .execute()
            PersistenceResponse.Ok
          }).recover {
            case NonFatal(e) if e.getMessage.contains(queries.DUPLICATE_KEY_ERROR) =>
              logger.warn(
                s"Ignoring duplicate party submission with ID ${partyDetails.party} for submissionId $submissionIdOpt, participantId $participantId")
              conn.rollback()
              PersistenceResponse.Duplicate
          }.get
        case PartyLedgerEntry.AllocationRejected(submissionId, participantId, recordTime, reason) =>
          SQL_INSERT_PARTY_ENTRY_REJECT
            .on(
              "ledger_offset" -> offset,
              "recorded_at" -> recordTime,
              "submission_id" -> submissionId,
              "participant_id" -> participantId,
              "rejection_reason" -> reason
            )
            .execute()
          PersistenceResponse.Ok
      }
    }

  }

  private val SQL_GET_PARTY_ENTRIES = SQL(
    "select * from party_entries where ledger_offset>{startExclusive} and ledger_offset<={endInclusive} order by ledger_offset asc limit {pageSize} offset {queryOffset}")

  private val partyEntryParser: RowParser[(Offset, PartyLedgerEntry)] =
    (offset("ledger_offset") ~
      date("recorded_at") ~
      ledgerString("submission_id").? ~
      participantId("participant_id").? ~
      party("party").? ~
      str("display_name").? ~
      str("typ") ~
      str("rejection_reason").? ~
      bool("is_local").?)
      .map(flatten)
      .map {
        case (
            offset,
            recordTime,
            submissionIdOpt,
            Some(participantId),
            Some(party),
            displayNameOpt,
            `acceptType`,
            None,
            Some(isLocal)) =>
          offset ->
            PartyLedgerEntry.AllocationAccepted(
              submissionIdOpt,
              participantId,
              recordTime.toInstant,
              PartyDetails(party, displayNameOpt, isLocal))
        case (
            offset,
            recordTime,
            Some(submissionId),
            Some(participantId),
            None,
            None,
            `rejectType`,
            Some(reason),
            None) =>
          offset -> PartyLedgerEntry.AllocationRejected(
            submissionId,
            participantId,
            recordTime.toInstant,
            reason)
        case invalidRow =>
          sys.error(s"getPartyEntries: invalid party entry row: $invalidRow")
      }

  override def getPartyEntries(
      startExclusive: Offset,
      endInclusive: Offset): Source[(Offset, PartyLedgerEntry), NotUsed] = {
    PaginatingAsyncStream(PageSize) { queryOffset =>
      dbDispatcher.executeSql(
        "load_party_entries",
        Some(
          s"bounds: ]${startExclusive.toApiString}, ${endInclusive.toApiString}] queryOffset $queryOffset")) {
        implicit conn =>
          SQL_GET_PARTY_ENTRIES
            .on(
              "startExclusive" -> startExclusive,
              "endInclusive" -> endInclusive,
              "pageSize" -> PageSize,
              "queryOffset" -> queryOffset)
            .asVectorOf(partyEntryParser)
      }
    }
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
      .as(contractId("contract_id").singleOpt)

  override def lookupKey(key: Node.GlobalKey, forParty: Party): Future[Option[AbsoluteContractId]] =
    contractsReader.lookupContractKey(forParty, key)

  private def storeContract(offset: Offset, contract: ActiveContract)(
      implicit connection: Connection): Unit = storeContracts(offset, List(contract))

  private def archiveContract(offset: Offset, cid: AbsoluteContractId)(
      implicit connection: Connection): Boolean =
    SQL_ARCHIVE_CONTRACT
      .on(
        "id" -> cid.coid,
        "archive_offset" -> offset
      )
      .execute()

  private val SQL_INSERT_CONTRACT =
    """insert into contracts(id, transaction_id, create_event_id, workflow_id, package_id, name, create_offset, key)
      |values({id}, {transaction_id}, {create_event_id}, {workflow_id}, {package_id}, {name}, {create_offset}, {key})""".stripMargin

  private val SQL_INSERT_CONTRACT_WITNESS =
    "insert into contract_witnesses(contract_id, witness) values({contract_id}, {witness})"

  private val SQL_INSERT_CONTRACT_KEY_MAINTAINERS =
    "insert into contract_key_maintainers(contract_id, maintainer) values({contract_id}, {maintainer})"

  private val SQL_INSERT_CONTRACT_SIGNATORIES =
    "insert into contract_signatories(contract_id, signatory) values({contract_id}, {signatory})"

  private val SQL_INSERT_CONTRACT_OBSERVERS =
    "insert into contract_observers(contract_id, observer) values({contract_id}, {observer})"

  private def storeContractData(contracts: Seq[(AbsoluteContractId, AbsoluteContractInst)])(
      implicit connection: Connection): Unit = {
    val namedContractDataParams = contracts
      .map {
        case (cid, contract) =>
          Seq[NamedParameter](
            "id" -> cid.coid,
            "contract" -> contractSerializer
              .serializeContractInstance(contract)
              .getOrElse(sys.error(s"failed to serialize contract! cid:${cid.coid}"))
          )
      }

    if (namedContractDataParams.nonEmpty) {
      val _ = executeBatchSql(
        queries.SQL_INSERT_CONTRACT_DATA,
        namedContractDataParams
      )
    }
  }

  private def storeContracts(offset: Offset, contracts: immutable.Seq[ActiveContract])(
      implicit connection: Connection): Unit = {

    // An ACS contract contains several collections (e.g., witnesses or divulgences).
    // The contract is therefore stored in several SQL tables.

    // Part 1: insert the contract data into the 'contracts' table
    if (contracts.nonEmpty) {
      val namedContractParams = contracts
        .map(
          c =>
            Seq[NamedParameter](
              "id" -> c.id.coid,
              "transaction_id" -> c.transactionId,
              "create_event_id" -> c.eventId,
              "workflow_id" -> c.workflowId,
              "package_id" -> c.contract.template.packageId,
              "name" -> c.contract.template.qualifiedName.toString,
              "create_offset" -> offset,
              "key" -> c.key
                .map(
                  k =>
                    ValueSerializer
                      .serializeValue(k.key, s"Failed to serialize key for contract ${c.id.coid}"))
          )
        )

      executeBatchSql(
        SQL_INSERT_CONTRACT,
        namedContractParams
      )

      storeContractData(contracts.map(c => (c.id, c.contract)))

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

      // Part 3: formerly: insert divulgences into the 'contract_divulgences' table
      assert(
        contracts.forall(_.divulgences.isEmpty),
        "Encountered non-empty local divulgence. This is a bug!")
      // when storing contracts, the `divulgences` field is only used to store local divulgences.
      // since local divulgences in a committed transaction are non-existent, there is nothing to do here.

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

    // Part 5: insert signatories into the 'contract_signatories' table
    val namedSignatoriesParams = contracts
      .flatMap(
        c =>
          c.signatories.map(
            w =>
              Seq[NamedParameter](
                "contract_id" -> c.id.coid,
                "signatory" -> w
            ))
      )
      .toArray

    if (!namedSignatoriesParams.isEmpty) {
      executeBatchSql(
        SQL_INSERT_CONTRACT_SIGNATORIES,
        namedSignatoriesParams
      )
    }

    // Part 6: insert observers into the 'contract_observers' table
    val namedObserversParams = contracts
      .flatMap(
        c =>
          c.observers.map(
            w =>
              Seq[NamedParameter](
                "contract_id" -> c.id.coid,
                "observer" -> w
            ))
      )
      .toArray

    if (!namedObserversParams.isEmpty) {
      executeBatchSql(
        SQL_INSERT_CONTRACT_OBSERVERS,
        namedObserversParams
      )
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

  /**
    * Updates the active contract set from the given DAML transaction.
    * Note: This involves checking the validity of the given DAML transaction.
    * Invalid transactions trigger a rollback of the current SQL transaction.
    */
  private def updateActiveContractSet(
      offset: Offset,
      submitter: Option[Party],
      tx: Transaction,
      globalDivulgence: Relation[AbsoluteContractId, Party],
      divulgedContracts: List[(Value.AbsoluteContractId, AbsoluteContractInst)])(
      implicit connection: Connection): Option[RejectionReason] = tx match {
    case LedgerEntry.Transaction(
        _,
        transactionId,
        _,
        _,
        workflowId,
        ledgerEffectiveTime,
        _,
        transaction,
        disclosure) =>
      final class AcsStoreAcc extends ActiveLedgerState[AcsStoreAcc] {

        override def lookupContractByKey(key: GlobalKey): Option[AbsoluteContractId] =
          selectContractKey(key)

        override def lookupContractLet(cid: AbsoluteContractId): Option[LetLookup] =
          lookupContractLetSync(cid)

        override def addContract(c: ActiveContract, keyO: Option[GlobalKey]): AcsStoreAcc = {
          storeContract(offset, c)
          keyO.foreach(key => storeContractKey(key, c.id))
          this
        }

        override def removeContract(cid: AbsoluteContractId): AcsStoreAcc = {
          archiveContract(offset, cid)
          removeContractKey(cid)
          this
        }

        override def addParties(parties: Set[Party]): AcsStoreAcc = {
          val partyParams = parties.toList.map(
            p =>
              Seq[NamedParameter](
                "name" -> (p: String),
                "ledger_offset" -> offset,
                "explicit" -> false
            ))
          if (partyParams.nonEmpty) {
            executeBatchSql(queries.SQL_IMPLICITLY_INSERT_PARTIES, partyParams)
          }
          this
        }

        override def divulgeAlreadyCommittedContracts(
            transactionId: TransactionId,
            global: Relation[AbsoluteContractId, Party],
            divulgedContracts: List[(Value.AbsoluteContractId, AbsoluteContractInst)])
          : AcsStoreAcc = {
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
            executeBatchSql(queries.SQL_BATCH_INSERT_DIVULGENCES, divulgenceParams)
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
        submitter,
        transaction,
        disclosure,
        globalDivulgence,
        divulgedContracts
      )

      atr match {
        case Left(err) =>
          Some(Inconsistent(s"Reason: ${err.mkString("[", ", ", "]")}"))
        case Right(_) => None
      }
  }

  private def storeTransaction(offset: Offset, tx: LedgerEntry.Transaction, txBytes: Array[Byte])(
      implicit connection: Connection): Unit = {
    SQL_INSERT_TRANSACTION
      .on(
        "ledger_offset" -> offset,
        "transaction_id" -> tx.transactionId,
        "command_id" -> (tx.commandId: Option[String]),
        "application_id" -> (tx.applicationId: Option[String]),
        "submitter" -> (tx.submittingParty: Option[String]),
        "workflow_id" -> tx.workflowId.getOrElse(""),
        "effective_at" -> tx.ledgerEffectiveTime,
        "recorded_at" -> tx.recordedAt,
        "transaction" -> txBytes
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

  private def storeRejection(offset: Offset, rejection: LedgerEntry.Rejection)(
      implicit connection: Connection): Unit = {
    val (rejectionDescription, rejectionType) = writeRejectionReason(rejection.rejectionReason)
    SQL_INSERT_REJECTION
      .on(
        "ledger_offset" -> offset,
        "command_id" -> rejection.commandId,
        "application_id" -> rejection.applicationId,
        "submitter" -> rejection.submitter,
        "recorded_at" -> rejection.recordTime,
        "rejection_description" -> rejectionDescription,
        "rejection_type" -> rejectionType
      )
      .execute()

    ()
  }

  private def serializeTransaction(ledgerEntry: LedgerEntry): Array[Byte] = {
    ledgerEntry match {
      case LedgerEntry.Transaction(transactionId, _, _, _, _, _, _, genTransaction, _) =>
        transactionSerializer
          .serializeTransaction(genTransaction)
          .fold(
            err =>
              sys.error(
                s"Failed to serialize transaction! trId: $transactionId. Details: ${err.errorMessage}."),
            identity
          )
      case _: LedgerEntry.Rejection => Array.empty[Byte]
    }
  }

  private def splitOrThrow(id: EventId): NodeId =
    split(id).fold(sys.error(s"Illegal format for event identifier $id"))(_.nodeId)

  //TODO: test it for failures..
  override def storeLedgerEntry(
      offset: Offset,
      ledgerEntry: PersistenceEntry): Future[PersistenceResponse] = {
    import PersistenceResponse._

    val txBytes = serializeTransaction(ledgerEntry.entry)

    def insertEntry(le: PersistenceEntry)(implicit conn: Connection): PersistenceResponse =
      le match {
        case PersistenceEntry.Transaction(tx, globalDivulgence, divulgedContracts) =>
          Try {
            storeTransaction(offset, tx, txBytes)

            // Ensure divulged contracts are known about before they are referred to.
            storeContractData(divulgedContracts)

            updateActiveContractSet(
              offset,
              tx.submittingParty,
              tx,
              globalDivulgence,
              divulgedContracts)
              .flatMap { rejectionReason =>
                // we need to rollback the existing sql transaction
                conn.rollback()
                // we only need to store a rejection if the full submitter information is available.
                for {
                  cmdId <- tx.commandId
                  appId <- tx.applicationId
                  submitter <- tx.submittingParty
                } yield {
                  val rejection = LedgerEntry.Rejection(
                    tx.recordedAt,
                    cmdId,
                    appId,
                    submitter,
                    rejectionReason
                  )
                  CommandCompletionsTable.prepareInsert(offset, rejection).map(_.execute())
                  insertEntry(PersistenceEntry.Rejection(rejection))
                }
              } getOrElse {
              transactionsWriter.write(
                applicationId = tx.applicationId,
                workflowId = tx.workflowId,
                transactionId = tx.transactionId,
                commandId = tx.commandId,
                submitter = tx.submittingParty,
                roots = tx.transaction.roots.iterator.map(splitOrThrow).toSet,
                ledgerEffectiveTime = tx.ledgerEffectiveTime,
                offset = offset,
                transaction = tx.transaction.mapNodeId(splitOrThrow),
                divulgedContracts = divulgedContracts,
              )
              Ok
            }
          }.recover {
            case NonFatal(e) if e.getMessage.contains(queries.DUPLICATE_KEY_ERROR) =>
              logger.warn(
                s"Ignoring duplicate submission for submitter ${tx.submittingParty}, applicationId ${tx.applicationId}, commandId ${tx.commandId}")
              conn.rollback()
              Duplicate
          }.get

        case PersistenceEntry.Rejection(rejection) =>
          stopDeduplicatingCommandSync(domain.CommandId(rejection.commandId), rejection.submitter)
          storeRejection(offset, rejection)
          Ok
      }

    dbDispatcher
      .executeSql("store_ledger_entry", Some(ledgerEntry.getClass.getSimpleName)) { implicit conn =>
        CommandCompletionsTable.prepareInsert(offset, ledgerEntry.entry).map(_.execute())
        val resp = insertEntry(ledgerEntry)
        updateLedgerEnd(offset)
        resp
      }
  }

  override def storeInitialState(
      activeContracts: immutable.Seq[ActiveContract],
      ledgerEntries: immutable.Seq[(Offset, LedgerEntry)],
      newLedgerEnd: Offset
  ): Future[Unit] = {
    // A map to look up offset by transaction ID
    // Needed to store contracts: in the database, we store the offset at which a contract was created,
    // the Contract object stores the transaction ID at which it was created.
    val transactionIdMap = ledgerEntries.collect {
      case (i, tx: LedgerEntry.Transaction) => tx.transactionId -> i
    }.toMap

    val transactionBytes = ledgerEntries.collect {
      case (offset, entry: LedgerEntry.Transaction) => offset -> serializeTransaction(entry)
    }.toMap

    dbDispatcher
      .executeSql(
        "store_initial_state_from_scenario",
        Some(s"active contracts: ${activeContracts.size}, ledger entries: ${ledgerEntries.size}")) {
        implicit conn =>
          // First, store all ledger entries without updating the ACS
          // We can't use the storeLedgerEntry(), as that one does update the ACS
          ledgerEntries.foreach {
            case (offset, entry) =>
              entry match {
                case tx: LedgerEntry.Transaction =>
                  storeTransaction(offset, tx, transactionBytes(offset))
                  transactionsWriter.write(
                    applicationId = tx.applicationId,
                    workflowId = tx.workflowId,
                    transactionId = tx.transactionId,
                    commandId = tx.commandId,
                    submitter = tx.submittingParty,
                    roots = tx.transaction.roots.iterator.map(splitOrThrow).toSet,
                    ledgerEffectiveTime = tx.ledgerEffectiveTime,
                    offset = offset,
                    transaction = tx.transaction.mapNodeId(splitOrThrow),
                    divulgedContracts = Nil,
                  )
                case rj: LedgerEntry.Rejection => storeRejection(offset, rj)
              }
          }

          // Then, write the given ACS. We trust the caller to supply an ACS that is
          // consistent with the given list of ledger entries.
          activeContracts.foreach(c => storeContract(transactionIdMap(c.transactionId), c))

          updateLedgerEnd(newLedgerEnd)
      }
  }

  private def writeRejectionReason(rejectionReason: RejectionReason) =
    (rejectionReason.description, rejectionReason match {
      case _: Inconsistent => "Inconsistent"
      case _: OutOfQuota => "OutOfQuota"
      case _: Disputed => "Disputed"
      case _: PartyNotKnownOnLedger => "PartyNotKnownOnLedger"
      case _: SubmitterCannotActViaParticipant => "SubmitterCannotActViaParticipant"
      case _: InvalidLedgerTime => "InvalidLedgerTime"
    })

  private def readRejectionReason(rejectionType: String, description: String): RejectionReason =
    rejectionType match {
      case "Inconsistent" => Inconsistent(description)
      case "OutOfQuota" => OutOfQuota(description)
      case "Disputed" => Disputed(description)
      case "PartyNotKnownOnLedger" => PartyNotKnownOnLedger(description)
      case "SubmitterCannotActViaParticipant" => SubmitterCannotActViaParticipant(description)
      case "InvalidLedgerTime" => InvalidLedgerTime(description)
      case typ => sys.error(s"unknown rejection reason: $typ")
    }

  private val SQL_SELECT_ENTRY =
    SQL("select * from ledger_entries where ledger_offset={ledger_offset}")

  private val SQL_SELECT_DISCLOSURE =
    SQL("select * from disclosures where transaction_id={transaction_id}")

  private val EntryParser: RowParser[ParsedEntry] = (
    str("typ") ~
      ledgerString("transaction_id").? ~
      ledgerString("command_id").? ~
      ledgerString("application_id").? ~
      party("submitter").? ~
      ledgerString("workflow_id")
        .map(s => if (s.isEmpty) null.asInstanceOf[Ref.LedgerString] else s)
        .? ~
      date("effective_at").? ~
      date("recorded_at").? ~
      binaryStream("transaction").? ~
      str("rejection_type").? ~
      str("rejection_description").? ~
      offset("ledger_offset")
  ) map flatten map ParsedEntry.tupled

  private val DisclosureParser = ledgerString("event_id") ~ party("party") map flatten

  private def toLedgerEntry(
      parsedEntry: ParsedEntry,
      disclosureOpt: Option[Relation[EventId, Party]]): (Offset, LedgerEntry) =
    parsedEntry match {
      case ParsedEntry(
          "transaction",
          Some(transactionId),
          commandId,
          applicationId,
          submitter,
          workflowId,
          Some(effectiveAt),
          Some(recordedAt),
          Some(transactionStream),
          None,
          None,
          offset) =>
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
            .fold(
              err =>
                sys.error(s"failed to deserialize transaction! trId: $transactionId: error: $err"),
              identity),
          disclosureOpt.getOrElse(Map.empty)
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
      case invalidRow =>
        sys.error(
          s"invalid ledger entry for offset: ${invalidRow.offset.toApiString}. database row: $invalidRow")
    }

  override def lookupLedgerEntry(offset: Offset): Future[Option[LedgerEntry]] = {
    dbDispatcher
      .executeSql("lookup_ledger_entry_at_offset", Some(s"offset: ${offset.toApiString}")) {
        implicit conn =>
          val entry = SQL_SELECT_ENTRY
            .on("ledger_offset" -> offset)
            .as(EntryParser.singleOpt)
          entry.map(e => e -> loadDisclosureOptForEntry(e))
      }
      .map(_.map((toLedgerEntry _).tupled(_)._2))(executionContext)
  }

  private val ContractLetParser = date("effective_at").?

  private val SQL_SELECT_CONTRACT_LET =
    SQL("""
        |select cd.id, le.effective_at
        |from contract_data cd
        |left join contracts c on c.id=cd.id
        |left join ledger_entries le on c.transaction_id = le.transaction_id
        |where
        |  cd.id={contract_id} and
        |  (le.effective_at is null or c.archive_offset is null)
        | """.stripMargin)

  private def lookupContractLetSync(contractId: AbsoluteContractId)(
      implicit conn: Connection): Option[LetLookup] =
    SQL_SELECT_CONTRACT_LET
      .on("contract_id" -> contractId.coid)
      .as(ContractLetParser.singleOpt)
      .map {
        case None => LetUnknown
        case Some(let) => Let(let.toInstant)
      }

  override def lookupMaximumLedgerTime(
      contractIds: Set[AbsoluteContractId],
  ): Future[Option[Instant]] =
    contractsReader.lookupMaximumLedgerTime(contractIds)

  override def lookupActiveOrDivulgedContract(
      contractId: AbsoluteContractId,
      forParty: Party): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]] =
    contractsReader.lookupActiveContract(forParty, contractId)

  private val SQL_GET_LEDGER_ENTRIES = SQL(
    "select * from ledger_entries where ledger_offset>{startExclusive} and ledger_offset<={endInclusive} order by ledger_offset asc limit {pageSize} offset {queryOffset}")

  private val PageSize = 100

  override def getLedgerEntries(
      startExclusive: Offset,
      endInclusive: Offset): Source[(Offset, LedgerEntry), NotUsed] =
    PaginatingAsyncStream(PageSize) { queryOffset =>
      dbDispatcher.executeSql(
        s"load_ledger_entries",
        Some(
          s"bounds: ]${startExclusive.toApiString}, ${endInclusive.toApiString}] query-offset $queryOffset")) {
        implicit conn =>
          val parsedEntries = SQL_GET_LEDGER_ENTRIES
            .on(
              "startExclusive" -> startExclusive,
              "endInclusive" -> endInclusive,
              "pageSize" -> PageSize,
              "queryOffset" -> queryOffset)
            .asVectorOf(EntryParser)
          parsedEntries.map(entry => entry -> loadDisclosureOptForEntry(entry))
      }
    }.map((toLedgerEntry _).tupled)

  private def loadDisclosureOptForEntry(parsedEntry: ParsedEntry)(
      implicit conn: Connection): Option[Relation[EventId, Party]] = {
    parsedEntry.transactionId.map { transactionId =>
      SQL_SELECT_DISCLOSURE
        .on("transaction_id" -> transactionId)
        .as(DisclosureParser.*)
        .groupBy(_._1)
        .transform((_, v) => v.map(_._2).toSet)
    }
  }

  private def orEmptyStringList(xs: Iterable[String]): List[String] =
    if (xs.nonEmpty) xs.toList else List("")

  // using '&' as a "separator" for the two columns because it is not allowed in either Party or Identifier strings
  // and querying on tuples is basically impossible to do sensibly.
  private def byPartyAndTemplate(txf: TransactionFilter): List[String] =
    orEmptyStringList(
      txf.filtersByParty
        .flatMap {
          case (party, Filters(Some(InclusiveFilters(templateIds)))) =>
            templateIds.map(t => s"${t.qualifiedName.qualifiedName}&$party")
          case _ => Seq.empty
        })

  private val SQL_SELECT_MULTIPLE_PARTIES =
    SQL(
      "select party, display_name, ledger_offset, explicit from parties where party in ({parties})")

  private val SQL_SELECT_ALL_PARTIES =
    SQL("select party, display_name, ledger_offset, explicit from parties")

  private val PartyDataParser: RowParser[ParsedPartyData] =
    Macro.parser[ParsedPartyData](
      "party",
      "display_name",
      "ledger_offset",
      "explicit"
    )

  override def getParties(parties: Seq[Party]): Future[List[PartyDetails]] =
    if (parties.isEmpty)
      Future.successful(List.empty)
    else
      dbDispatcher
        .executeSql("load_parties") { implicit conn =>
          SQL_SELECT_MULTIPLE_PARTIES
            .on("parties" -> parties)
            .as(PartyDataParser.*)
        }
        .map(_.map(constructPartyDetails))(executionContext)

  override def listKnownParties(): Future[List[PartyDetails]] =
    dbDispatcher
      .executeSql("load_all_parties") { implicit conn =>
        SQL_SELECT_ALL_PARTIES
          .as(PartyDataParser.*)
      }
      .map(_.map(constructPartyDetails))(executionContext)

  private def constructPartyDetails(data: ParsedPartyData): PartyDetails =
    // TODO: isLocal should be based on equality of participantId reported in an
    //       update and the id given to participant in a command-line argument
    //       (See issue #2026)
    PartyDetails(Party.assertFromString(data.party), data.displayName, isLocal = true)

  private val SQL_INSERT_PARTY =
    SQL("""insert into parties(party, display_name, ledger_offset, explicit)
        |values ({party}, {display_name}, {ledger_offset}, 'true')""".stripMargin)

  private val SQL_SELECT_PACKAGES =
    SQL("""select package_id, source_description, known_since, size
          |from packages
          |""".stripMargin)

  private val SQL_SELECT_PACKAGE =
    SQL("""select package
          |from packages
          |where package_id = {package_id}
          |""".stripMargin)

  private val PackageDataParser: RowParser[ParsedPackageData] =
    Macro.parser[ParsedPackageData](
      "package_id",
      "source_description",
      "size",
      "known_since"
    )

  override def listLfPackages: Future[Map[PackageId, PackageDetails]] =
    dbDispatcher
      .executeSql("load_packages") { implicit conn =>
        SQL_SELECT_PACKAGES
          .as(PackageDataParser.*)
      }
      .map(
        _.map(
          d =>
            PackageId.assertFromString(d.packageId) -> PackageDetails(
              d.size,
              d.knownSince.toInstant,
              d.sourceDescription)).toMap)(executionContext)

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    dbDispatcher
      .executeSql("load_archive", Some(s"pkg id: $packageId")) { implicit conn =>
        SQL_SELECT_PACKAGE
          .on(
            "package_id" -> packageId
          )
          .as[Option[Array[Byte]]](SqlParser.byteArray("package").singleOpt)
      }
      .map(_.map(data => Archive.parseFrom(Decode.damlLfCodedInputStreamFromBytes(data))))(
        executionContext)

  private val SQL_INSERT_PACKAGE_ENTRY_ACCEPT =
    SQL("""insert into package_entries(ledger_offset, recorded_at, submission_id, typ)
      |values ({ledger_offset}, {recorded_at}, {submission_id}, 'accept')
      |""".stripMargin)

  private val SQL_INSERT_PACKAGE_ENTRY_REJECT =
    SQL(
      """insert into package_entries(ledger_offset, recorded_at, submission_id, typ, rejection_reason)
      |values ({ledger_offset}, {recorded_at}, {submission_id}, 'reject', {rejection_reason})
      |""".stripMargin)

  override def storePackageEntry(
      offset: Offset,
      packages: List[(Archive, PackageDetails)],
      optEntry: Option[PackageLedgerEntry]
  ): Future[PersistenceResponse] = {
    dbDispatcher.executeSql(
      "store_package_entry",
      Some(s"packages: ${packages.map(_._1.getHash).mkString(", ")}")) { implicit conn =>
      updateLedgerEnd(offset)

      if (packages.nonEmpty) {
        val uploadId = optEntry.map(_.submissionId).getOrElse(UUID.randomUUID().toString)
        uploadLfPackages(uploadId, packages)
      }

      optEntry.foreach {
        case PackageLedgerEntry.PackageUploadAccepted(submissionId, recordTime) =>
          SQL_INSERT_PACKAGE_ENTRY_ACCEPT
            .on(
              "ledger_offset" -> offset,
              "recorded_at" -> recordTime,
              "submission_id" -> submissionId,
            )
            .execute()
        case PackageLedgerEntry.PackageUploadRejected(submissionId, recordTime, reason) =>
          SQL_INSERT_PACKAGE_ENTRY_REJECT
            .on(
              "ledger_offset" -> offset,
              "recorded_at" -> recordTime,
              "submission_id" -> submissionId,
              "rejection_reason" -> reason
            )
            .execute()
      }
      PersistenceResponse.Ok
    }
  }

  private def uploadLfPackages(uploadId: String, packages: List[(Archive, PackageDetails)])(
      implicit conn: Connection): Unit = {
    val params = packages
      .map(
        p =>
          Seq[NamedParameter](
            "package_id" -> p._1.getHash,
            "upload_id" -> uploadId,
            "source_description" -> p._2.sourceDescription,
            "size" -> p._2.size,
            "known_since" -> p._2.knownSince,
            "package" -> p._1.toByteArray
        )
      )
    val _ = executeBatchSql(queries.SQL_INSERT_PACKAGE, params)
  }

  private val SQL_GET_PACKAGE_ENTRIES = SQL(
    "select * from package_entries where ledger_offset>{startExclusive} and ledger_offset<={endInclusive} order by ledger_offset asc limit {pageSize} offset {queryOffset}")

  private val packageEntryParser: RowParser[(Offset, PackageLedgerEntry)] =
    (offset("ledger_offset") ~
      date("recorded_at") ~
      ledgerString("submission_id").? ~
      str("typ") ~
      str("rejection_reason").?)
      .map(flatten)
      .map {
        case (offset, recordTime, Some(submissionId), `acceptType`, None) =>
          offset ->
            PackageLedgerEntry.PackageUploadAccepted(submissionId, recordTime.toInstant)
        case (offset, recordTime, Some(submissionId), `rejectType`, Some(reason)) =>
          offset ->
            PackageLedgerEntry.PackageUploadRejected(submissionId, recordTime.toInstant, reason)
        case invalidRow =>
          sys.error(s"packageEntryParser: invalid party entry row: $invalidRow")
      }

  override def getPackageEntries(
      startExclusive: Offset,
      endInclusive: Offset): Source[(Offset, PackageLedgerEntry), NotUsed] = {
    PaginatingAsyncStream(PageSize) { queryOffset =>
      dbDispatcher.executeSql(
        "load_package_entries",
        Some(
          s"bounds: ]${startExclusive.toApiString}, ${endInclusive.toApiString}] queryOffset $queryOffset")) {
        implicit conn =>
          SQL_GET_PACKAGE_ENTRIES
            .on(
              "startExclusive" -> startExclusive,
              "endInclusive" -> endInclusive,
              "pageSize" -> PageSize,
              "queryOffset" -> queryOffset)
            .asVectorOf(packageEntryParser)
      }
    }
  }

  private val SQL_SELECT_COMMAND = SQL("""
      |select deduplicate_until
      |from participant_command_submissions
      |where deduplication_key = {deduplicationKey}
    """.stripMargin)

  private val CommandDataParser: RowParser[ParsedCommandData] =
    Macro.parser[ParsedCommandData](
      "deduplicate_until"
    )

  private def deduplicationKey(
      commandId: domain.CommandId,
      submitter: Ref.Party,
  ): String = commandId.unwrap + "%" + submitter

  override def deduplicateCommand(
      commandId: domain.CommandId,
      submitter: Ref.Party,
      submittedAt: Instant,
      deduplicateUntil: Instant): Future[CommandDeduplicationResult] =
    dbDispatcher.executeSql("deduplicate_command") { implicit conn =>
      val key = deduplicationKey(commandId, submitter)
      // Insert a new deduplication entry, or update an expired entry
      val updated = SQL(queries.SQL_INSERT_COMMAND)
        .on(
          "deduplicationKey" -> key,
          "submittedAt" -> submittedAt,
          "deduplicateUntil" -> deduplicateUntil)
        .executeUpdate()

      if (updated == 1) {
        // New row inserted, this is the first time the command is submitted
        CommandDeduplicationNew
      } else {
        // Deduplication row already exists
        val result = SQL_SELECT_COMMAND
          .on("deduplicationKey" -> key)
          .as(CommandDataParser.single)

        CommandDeduplicationDuplicate(result.deduplicateUntil)
      }
    }

  private val SQL_DELETE_EXPIRED_COMMANDS = SQL("""
      |delete from participant_command_submissions
      |where deduplicate_until < {currentTime}
    """.stripMargin)

  override def removeExpiredDeduplicationData(currentTime: Instant): Future[Unit] =
    dbDispatcher.executeSql("remove_expired_deduplication_data") { implicit conn =>
      SQL_DELETE_EXPIRED_COMMANDS
        .on("currentTime" -> currentTime)
        .execute()
      ()
    }

  private val SQL_DELETE_COMMAND = SQL("""
     |delete from participant_command_submissions
     |where deduplication_key = {deduplicationKey}
    """.stripMargin)

  private[this] def stopDeduplicatingCommandSync(commandId: domain.CommandId, submitter: Party)(
      implicit conn: Connection): Unit = {
    val key = deduplicationKey(commandId, submitter)
    SQL_DELETE_COMMAND
      .on("deduplicationKey" -> key)
      .execute()
    ()
  }

  override def stopDeduplicatingCommand(
      commandId: domain.CommandId,
      submitter: Party): Future[Unit] =
    dbDispatcher.executeSql("stop_deduplicating_command") { implicit conn =>
      stopDeduplicatingCommandSync(commandId, submitter)
    }

  override def reset(): Future[Unit] =
    dbDispatcher.executeSql("truncate_all_tables") { implicit conn =>
      val _ = SQL(queries.SQL_TRUNCATE_TABLES).execute()
    }

  override val transactionsWriter: TransactionsWriter =
    new TransactionsWriter(dbType)

  override val transactionsReader: TransactionsReader =
    new TransactionsReader(dbDispatcher, executionContext, eventsPageSize)

  private val contractsReader: ContractsReader =
    ContractsReader(dbDispatcher, executionContext, dbType)

  private def executeBatchSql(query: String, params: Iterable[Seq[NamedParameter]])(
      implicit con: Connection) = {
    require(params.nonEmpty, "batch sql statement must have at least one set of name parameters")
    BatchSql(query, params.head, params.drop(1).toArray: _*).execute()
  }

  override val completions: CommandCompletionsReader[Offset] =
    CommandCompletionsReader(dbDispatcher)
}

object JdbcLedgerDao {

  private val DefaultNumberOfShortLivedConnections = 16

  private val ThreadFactory = new ThreadFactoryBuilder().setNameFormat("dao-executor-%d").build()

  def readOwner(
      serverRole: ServerRole,
      jdbcUrl: String,
      eventsPageSize: Int,
      metrics: MetricRegistry,
  )(implicit logCtx: LoggingContext): ResourceOwner[LedgerReadDao] = {
    val maxConnections = DefaultNumberOfShortLivedConnections
    owner(serverRole, jdbcUrl, maxConnections, eventsPageSize, metrics)
      .map(new MeteredLedgerReadDao(_, metrics))
  }

  def writeOwner(
      serverRole: ServerRole,
      jdbcUrl: String,
      eventsPageSize: Int,
      metrics: MetricRegistry,
  )(implicit logCtx: LoggingContext): ResourceOwner[LedgerDao] = {
    val dbType = DbType.jdbcType(jdbcUrl)
    val maxConnections =
      if (dbType.supportsParallelWrites) DefaultNumberOfShortLivedConnections else 1
    owner(serverRole, jdbcUrl, maxConnections, eventsPageSize, metrics)
      .map(new MeteredLedgerDao(_, metrics))
  }

  private def owner(
      serverRole: ServerRole,
      jdbcUrl: String,
      maxConnections: Int,
      eventsPageSize: Int,
      metrics: MetricRegistry,
  )(implicit logCtx: LoggingContext): ResourceOwner[LedgerDao] =
    for {
      dbDispatcher <- DbDispatcher.owner(serverRole, jdbcUrl, maxConnections, metrics)
      executor <- ResourceOwner.forExecutorService(() =>
        Executors.newCachedThreadPool(ThreadFactory))
    } yield
      new JdbcLedgerDao(
        maxConnections,
        dbDispatcher,
        ContractSerializer,
        TransactionSerializer,
        KeyHasher,
        DbType.jdbcType(jdbcUrl),
        ExecutionContext.fromExecutor(executor),
        eventsPageSize,
      )

  sealed trait Queries {

    // SQL statements using the proprietary Postgres on conflict .. do nothing clause
    protected[JdbcLedgerDao] def SQL_INSERT_CONTRACT_DATA: String

    protected[JdbcLedgerDao] def SQL_INSERT_PACKAGE: String

    protected[JdbcLedgerDao] def SQL_IMPLICITLY_INSERT_PARTIES: String

    protected[JdbcLedgerDao] def SQL_INSERT_COMMAND: String

    // Note: the SQL backend may receive divulgence information for the same (contract, party) tuple
    // more than once through BlindingInfo.globalDivulgence.
    // The ledger offsets for the same (contract, party) tuple should always be increasing, and the database
    // stores the offset at which the contract was first disclosed.
    // We therefore don't need to update anything if there is already some data for the given (contract, party) tuple.
    protected[JdbcLedgerDao] def SQL_BATCH_INSERT_DIVULGENCES: String

    protected[JdbcLedgerDao] def SQL_TRUNCATE_TABLES: String

    protected[JdbcLedgerDao] def DUPLICATE_KEY_ERROR
      : String // TODO: Avoid brittleness of error message checks
  }

  object PostgresQueries extends Queries {

    override protected[JdbcLedgerDao] val SQL_INSERT_CONTRACT_DATA: String =
      """insert into contract_data(id, contract) values({id}, {contract})
        |on conflict (id) do nothing""".stripMargin

    override protected[JdbcLedgerDao] val SQL_INSERT_PACKAGE: String =
      """insert into packages(package_id, upload_id, source_description, size, known_since, ledger_offset, package)
        |select {package_id}, {upload_id}, {source_description}, {size}, {known_since}, ledger_end, {package}
        |from parameters
        |on conflict (package_id) do nothing""".stripMargin

    override protected[JdbcLedgerDao] val SQL_IMPLICITLY_INSERT_PARTIES: String =
      """insert into parties(party, explicit, ledger_offset)
        |values({name}, {explicit}, {ledger_offset})
        |on conflict (party) do nothing""".stripMargin

    override protected[JdbcLedgerDao] val SQL_INSERT_COMMAND: String =
      """insert into participant_command_submissions as pcs (deduplication_key, deduplicate_until)
        |values ({deduplicationKey}, {deduplicateUntil})
        |on conflict (deduplication_key)
        |  do update
        |  set deduplicate_until={deduplicateUntil}
        |  where pcs.deduplicate_until < {submittedAt}""".stripMargin

    override protected[JdbcLedgerDao] val SQL_BATCH_INSERT_DIVULGENCES: String =
      """insert into contract_divulgences(contract_id, party, ledger_offset, transaction_id)
        |values({contract_id}, {party}, {ledger_offset}, {transaction_id})
        |on conflict on constraint contract_divulgences_idx do nothing""".stripMargin

    override protected[JdbcLedgerDao] val DUPLICATE_KEY_ERROR: String = "duplicate key"

    override protected[JdbcLedgerDao] val SQL_TRUNCATE_TABLES: String =
      """
        |truncate table configuration_entries cascade;
        |truncate table contracts cascade;
        |truncate table contract_data cascade;
        |truncate table contract_divulgences cascade;
        |truncate table contract_keys cascade;
        |truncate table contract_key_maintainers cascade;
        |truncate table contract_observers cascade;
        |truncate table contract_signatories cascade;
        |truncate table contract_witnesses cascade;
        |truncate table disclosures cascade;
        |truncate table ledger_entries cascade;
        |truncate table package_entries cascade;
        |truncate table parameters cascade;
        |truncate table participant_command_completions cascade;
        |truncate table participant_command_submissions cascade;
        |truncate table participant_events cascade;
        |truncate table participant_event_flat_transaction_witnesses cascade;
        |truncate table participant_event_witnesses_complement cascade;
        |truncate table participant_contracts cascade;
        |truncate table participant_contract_witnesses cascade;
        |truncate table parties cascade;
        |truncate table party_entries cascade;
      """.stripMargin

  }

  object H2DatabaseQueries extends Queries {

    override protected[JdbcLedgerDao] val SQL_INSERT_CONTRACT_DATA: String =
      """merge into contract_data using dual on id = {id}
        |when not matched then insert (id, contract) values ({id}, {contract})""".stripMargin

    override protected[JdbcLedgerDao] val SQL_INSERT_PACKAGE: String =
      """merge into packages using dual on package_id = {package_id}
        |when not matched then insert (package_id, upload_id, source_description, size, known_since, ledger_offset, package)
        |select {package_id}, {upload_id}, {source_description}, {size}, {known_since}, ledger_end, {package}
        |from parameters""".stripMargin

    override protected[JdbcLedgerDao] val SQL_IMPLICITLY_INSERT_PARTIES: String =
      """merge into parties using dual on party = {name}
        |when not matched then insert (party, explicit, ledger_offset) values ({name}, {explicit}, {ledger_offset})""".stripMargin

    override protected[JdbcLedgerDao] val SQL_INSERT_COMMAND: String =
      """merge into participant_command_submissions pcs
        |using dual on deduplication_key = {deduplicationKey}
        |when not matched then
        |  insert (deduplication_key, deduplicate_until)
        |  values ({deduplicationKey}, {deduplicateUntil})
        |when matched and pcs.deduplicate_until < {submittedAt} then
        |  update set deduplicate_until={deduplicateUntil}""".stripMargin

    override protected[JdbcLedgerDao] val SQL_BATCH_INSERT_DIVULGENCES: String =
      """merge into contract_divulgences using dual on contract_id = {contract_id} and party = {party}
        |when not matched then insert (contract_id, party, ledger_offset, transaction_id)
        |values ({contract_id}, {party}, {ledger_offset}, {transaction_id})""".stripMargin

    override protected[JdbcLedgerDao] val DUPLICATE_KEY_ERROR: String =
      "Unique index or primary key violation"

    override protected[JdbcLedgerDao] val SQL_TRUNCATE_TABLES: String =
      """
        |set referential_integrity false;
        |truncate table configuration_entries;
        |truncate table contracts;
        |truncate table contract_data;
        |truncate table contract_divulgences;
        |truncate table contract_keys;
        |truncate table contract_key_maintainers;
        |truncate table contract_observers;
        |truncate table contract_signatories;
        |truncate table contract_witnesses;
        |truncate table disclosures;
        |truncate table ledger_entries;
        |truncate table package_entries;
        |truncate table parameters;
        |truncate table participant_command_completions;
        |truncate table participant_command_submissions;
        |truncate table participant_events;
        |truncate table participant_event_flat_transaction_witnesses;
        |truncate table participant_event_witnesses_complement;
        |truncate table participant_contracts;
        |truncate table participant_contract_witnesses;
        |truncate table parties;
        |truncate table party_entries;
        |set referential_integrity true;
      """.stripMargin

  }
}
