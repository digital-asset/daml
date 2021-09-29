// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.sql.Connection
import java.time.Instant

import com.daml.ledger.api.domain.{LedgerId, ParticipantId, PartyDetails}
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.lf.data.Ref
import com.daml.lf.ledger.EventId
import com.daml.logging.LoggingContext
import com.daml.platform
import com.daml.platform.store.{DbType, EventSequentialId}
import com.daml.platform.store.appendonlydao.events.{ContractId, EventsTable, Key, Raw}
import com.daml.platform.store.backend.EventStorageBackend.{FilterParams, RangeParams}
import com.daml.platform.store.backend.StorageBackend.RawTransactionEvent
import com.daml.platform.store.backend.h2.H2StorageBackend
import com.daml.platform.store.backend.oracle.OracleStorageBackend
import com.daml.platform.store.backend.postgresql.{PostgresDataSourceConfig, PostgresStorageBackend}
import com.daml.platform.store.entries.{ConfigurationEntry, PackageLedgerEntry, PartyLedgerEntry}
import com.daml.platform.store.interfaces.LedgerDaoContractsReader.KeyState
import com.daml.scalautil.NeverEqualsOverride
import javax.sql.DataSource

import scala.annotation.unused
import scala.util.Try

/** Encapsulates the interface which hides database technology specific implementations.
  * Naming convention for the interface methods, which requiring Connection:
  *  - read operations are represented as nouns (plural, singular form indicates cardinality)
  *  - write operations are represented as verbs
  *
  * @tparam DB_BATCH Since parallel ingestion comes also with batching, this implementation specific type allows separation of the CPU intensive batching operation from the pure IO intensive insertBatch operation.
  */
trait StorageBackend[DB_BATCH]
    extends IngestionStorageBackend[DB_BATCH]
    with ParameterStorageBackend
    with ConfigurationStorageBackend
    with PartyStorageBackend
    with PackageStorageBackend
    with DeduplicationStorageBackend
    with CompletionStorageBackend
    with ContractStorageBackend
    with EventStorageBackend
    with DataSourceStorageBackend
    with DBLockStorageBackend
    with IntegrityStorageBackend {

  /** Truncates all storage backend tables, EXCEPT the packages table.
    * Does not touch other tables, like the Flyway history table.
    * Reason: the reset() call is used by the ledger API reset service,
    * which is mainly used for application tests in another big project,
    * and re-uploading packages after each test significantly slows down their test time.
    */
  def reset(connection: Connection): Unit

  /** Truncates ALL storage backend tables.
    * Does not touch other tables, like the Flyway history table.
    * The result is a database that looks the same as a freshly created database with Flyway migrations applied.
    */
  def resetAll(connection: Connection): Unit
}

trait IngestionStorageBackend[DB_BATCH] {

  /** The CPU intensive batching operation hides the batching logic, and the mapping to the database specific representation of the inserted data.
    * This should be pure CPU logic without IO.
    *
    * @param dbDtos is a collection of DbDto from which the batch is formed
    * @return the database-specific batch DTO, which can be inserted via insertBatch
    */
  def batch(dbDtos: Vector[DbDto]): DB_BATCH

  /** Using a JDBC connection, a batch will be inserted into the database.
    * No significant CPU load, mostly blocking JDBC communication with the database backend.
    *
    * @param connection to be used when inserting the batch
    * @param batch      to be inserted
    */
  def insertBatch(connection: Connection, batch: DB_BATCH): Unit

  /** Deletes all partially ingested data, written during a non-graceful stop of previous indexing.
    * No significant CPU load, mostly blocking JDBC communication with the database backend.
    *
    * @param ledgerEnd the current ledger end, or None if no ledger end exists
    * @param connection to be used when inserting the batch
    */
  def deletePartiallyIngestedData(ledgerEnd: Option[ParameterStorageBackend.LedgerEnd])(
      connection: Connection
  ): Unit
}

trait ParameterStorageBackend {

  /** This method is used to update the new observable ledger end.
    * No significant CPU load, mostly blocking JDBC communication with the database backend.
    *
    * @param connection to be used when updating the parameters table
    */
  def updateLedgerEnd(ledgerEnd: ParameterStorageBackend.LedgerEnd)(connection: Connection): Unit

  /** Query the current ledger end, read from the parameters table.
    * No significant CPU load, mostly blocking JDBC communication with the database backend.
    *
    * @param connection to be used to get the LedgerEnd
    * @return the current LedgerEnd, or None if no ledger end exists
    */
  def ledgerEnd(connection: Connection): Option[ParameterStorageBackend.LedgerEnd]

  /** Query the current ledger end, returning a value that points to a point before the ledger begin
    * if no ledger end exists.
    * No significant CPU load, mostly blocking JDBC communication with the database backend.
    *
    * @param connection to be used to get the LedgerEnd
    * @return the current LedgerEnd, or a LedgerEnd that points to before the ledger begin if no ledger end exists
    */
  final def ledgerEndOrBeforeBegin(connection: Connection): ParameterStorageBackend.LedgerEnd =
    ledgerEnd(connection).getOrElse(
      ParameterStorageBackend.LedgerEnd(Offset.beforeBegin, EventSequentialId.beforeBegin)
    )

  /** Part of pruning process, this needs to be in the same transaction as the other pruning related database operations
    */
  def updatePrunedUptoInclusive(prunedUpToInclusive: Offset)(connection: Connection): Unit
  def prunedUpToInclusive(connection: Connection): Option[Offset]
  def updatePrunedAllDivulgedContractsUpToInclusive(
      prunedUpToInclusive: Offset
  )(connection: Connection): Unit
  def participantAllDivulgedContractsPrunedUpToInclusive(
      connection: Connection
  ): Option[Offset]

  /** Initializes the parameters table and verifies or updates ledger identity parameters.
    * This method is idempotent:
    *  - If no identity parameters are stored, then they are set to the given value.
    *  - If identity parameters are stored, then they are compared to the given ones.
    *  - Ledger identity parameters are written at most once, and are never overwritten.
    *  No significant CPU load, mostly blocking JDBC communication with the database backend.
    *
    *  This method is NOT safe to call concurrently.
    */
  def initializeParameters(params: ParameterStorageBackend.IdentityParams)(connection: Connection)(
      implicit loggingContext: LoggingContext
  ): Unit

  /** Returns the ledger identity parameters, or None if the database hasn't been initialized yet. */
  def ledgerIdentity(connection: Connection): Option[ParameterStorageBackend.IdentityParams]
}

object ParameterStorageBackend {
  case class LedgerEnd(lastOffset: Offset, lastEventSeqId: Long)
  case class IdentityParams(ledgerId: LedgerId, participantId: ParticipantId)
}

trait ConfigurationStorageBackend {
  def ledgerConfiguration(connection: Connection): Option[(Offset, Configuration)]
  def configurationEntries(
      startExclusive: Offset,
      endInclusive: Offset,
      pageSize: Int,
      queryOffset: Long,
  )(connection: Connection): Vector[(Offset, ConfigurationEntry)]
}

trait PartyStorageBackend {
  def partyEntries(
      startExclusive: Offset,
      endInclusive: Offset,
      pageSize: Int,
      queryOffset: Long,
  )(connection: Connection): Vector[(Offset, PartyLedgerEntry)]
  def parties(parties: Seq[Ref.Party])(connection: Connection): List[PartyDetails]
  def knownParties(connection: Connection): List[PartyDetails]
}

trait PackageStorageBackend {
  def lfPackages(connection: Connection): Map[Ref.PackageId, PackageDetails]

  def lfArchive(packageId: Ref.PackageId)(connection: Connection): Option[Array[Byte]]

  def packageEntries(
      startExclusive: Offset,
      endInclusive: Offset,
      pageSize: Int,
      queryOffset: Long,
  )(connection: Connection): Vector[(Offset, PackageLedgerEntry)]
}

trait DeduplicationStorageBackend {
  def deduplicatedUntil(deduplicationKey: String)(connection: Connection): Instant
  def upsertDeduplicationEntry(
      key: String,
      submittedAt: Instant,
      deduplicateUntil: Instant,
  )(connection: Connection)(implicit loggingContext: LoggingContext): Int
  def removeExpiredDeduplicationData(currentTime: Instant)(connection: Connection): Unit
  def stopDeduplicatingCommand(deduplicationKey: String)(connection: Connection): Unit
}

trait CompletionStorageBackend {
  def commandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: Ref.ApplicationId,
      parties: Set[Ref.Party],
  )(connection: Connection): List[CompletionStreamResponse]

  /** Part of pruning process, this needs to be in the same transaction as the other pruning related database operations
    */
  def pruneCompletions(
      pruneUpToInclusive: Offset
  )(connection: Connection, loggingContext: LoggingContext): Unit
}

trait ContractStorageBackend {
  def contractKeyGlobally(key: Key)(connection: Connection): Option[ContractId]
  def maximumLedgerTime(ids: Set[ContractId])(connection: Connection): Try[Option[Instant]]
  def keyState(key: Key, validAt: Long)(connection: Connection): KeyState
  def contractState(contractId: ContractId, before: Long)(
      connection: Connection
  ): Option[StorageBackend.RawContractState]
  def activeContractWithArgument(readers: Set[Ref.Party], contractId: ContractId)(
      connection: Connection
  ): Option[StorageBackend.RawContract]
  def activeContractWithoutArgument(readers: Set[Ref.Party], contractId: ContractId)(
      connection: Connection
  ): Option[String]
  def contractKey(readers: Set[Ref.Party], key: Key)(
      connection: Connection
  ): Option[ContractId]
  def contractStateEvents(startExclusive: Long, endInclusive: Long)(
      connection: Connection
  ): Vector[StorageBackend.RawContractStateEvent]
}

trait EventStorageBackend {

  /** Part of pruning process, this needs to be in the same transaction as the other pruning related database operations
    */
  def pruneEvents(pruneUpToInclusive: Offset, pruneAllDivulgedContracts: Boolean)(
      connection: Connection,
      loggingContext: LoggingContext,
  ): Unit
  def validatePruningOffsetAgainstMigration(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      connection: Connection,
  ): Unit
  def transactionEvents(
      rangeParams: RangeParams,
      filterParams: FilterParams,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def activeContractEvents(
      rangeParams: RangeParams,
      filterParams: FilterParams,
      endInclusiveOffset: Offset,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def flatTransaction(
      transactionId: Ref.TransactionId,
      filterParams: FilterParams,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def transactionTreeEvents(
      rangeParams: RangeParams,
      filterParams: FilterParams,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]]
  def transactionTree(
      transactionId: Ref.TransactionId,
      filterParams: FilterParams,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]]

  /** Max event sequential id of observable (create, consuming and nonconsuming exercise) events. */
  def maxEventSequentialIdOfAnObservableEvent(offset: Offset)(connection: Connection): Option[Long]
  def rawEvents(startExclusive: Long, endInclusive: Long)(
      connection: Connection
  ): Vector[RawTransactionEvent]
}

object EventStorageBackend {
  case class RangeParams(
      startExclusive: Long,
      endInclusive: Long,
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )

  case class FilterParams(
      wildCardParties: Set[Ref.Party],
      partiesAndTemplates: Set[(Set[Ref.Party], Set[Ref.Identifier])],
  )
}

trait DataSourceStorageBackend {
  def createDataSource(
      jdbcUrl: String,
      dataSourceConfig: DataSourceStorageBackend.DataSourceConfig =
        DataSourceStorageBackend.DataSourceConfig(),
      connectionInitHook: Option[Connection => Unit] = None,
  )(implicit loggingContext: LoggingContext): DataSource

  def checkCompatibility(@unused connection: Connection)(implicit
      @unused loggingContext: LoggingContext
  ): Unit = ()

  def checkDatabaseAvailable(connection: Connection): Unit
}

object DataSourceStorageBackend {

  /** @param postgresConfig configurations which apply only for the PostgresSQL backend
    */
  case class DataSourceConfig(
      postgresConfig: PostgresDataSourceConfig = PostgresDataSourceConfig()
  )
}

trait DBLockStorageBackend {
  def tryAcquire(
      lockId: DBLockStorageBackend.LockId,
      lockMode: DBLockStorageBackend.LockMode,
  )(connection: Connection): Option[DBLockStorageBackend.Lock]

  def release(lock: DBLockStorageBackend.Lock)(connection: Connection): Boolean

  def lock(id: Int): DBLockStorageBackend.LockId

  def dbLockSupported: Boolean
}

object DBLockStorageBackend {
  case class Lock(lockId: LockId, lockMode: LockMode)

  trait LockId

  trait LockMode
  object LockMode {
    case object Exclusive extends LockMode
    case object Shared extends LockMode
  }
}

trait IntegrityStorageBackend {

  /** Verifies the integrity of the index database, throwing an exception if any issue is found.
    * This operation is allowed to take some time to finish.
    * It is not expected that it is used during regular index/indexer operation.
    */
  def verifyIntegrity()(connection: Connection): Unit
}

object StorageBackend {
  case class RawContractState(
      templateId: Option[String],
      flatEventWitnesses: Set[Ref.Party],
      createArgument: Option[Array[Byte]],
      createArgumentCompression: Option[Int],
      eventKind: Int,
      ledgerEffectiveTime: Option[Instant],
  )

  class RawContract(
      val templateId: String,
      val createArgument: Array[Byte],
      val createArgumentCompression: Option[Int],
  )

  case class RawContractStateEvent(
      eventKind: Int,
      contractId: ContractId,
      templateId: Option[Ref.Identifier],
      ledgerEffectiveTime: Option[Instant],
      createKeyValue: Option[Array[Byte]],
      createKeyCompression: Option[Int],
      createArgument: Option[Array[Byte]],
      createArgumentCompression: Option[Int],
      flatEventWitnesses: Set[Ref.Party],
      eventSequentialId: Long,
      offset: Offset,
  )

  case class RawTransactionEvent(
      eventKind: Int,
      transactionId: String,
      nodeIndex: Int,
      commandId: Option[String],
      workflowId: Option[String],
      eventId: EventId,
      contractId: platform.store.appendonlydao.events.ContractId,
      templateId: Option[platform.store.appendonlydao.events.Identifier],
      ledgerEffectiveTime: Option[Instant],
      createSignatories: Option[Array[String]],
      createObservers: Option[Array[String]],
      createAgreementText: Option[String],
      createKeyValue: Option[Array[Byte]],
      createKeyCompression: Option[Int],
      createArgument: Option[Array[Byte]],
      createArgumentCompression: Option[Int],
      treeEventWitnesses: Set[String],
      flatEventWitnesses: Set[String],
      submitters: Set[String],
      exerciseChoice: Option[String],
      exerciseArgument: Option[Array[Byte]],
      exerciseArgumentCompression: Option[Int],
      exerciseResult: Option[Array[Byte]],
      exerciseResultCompression: Option[Int],
      exerciseActors: Option[Array[String]],
      exerciseChildEventIds: Option[Array[String]],
      eventSequentialId: Long,
      offset: Offset,
  ) extends NeverEqualsOverride

  def of(dbType: DbType): StorageBackend[_] =
    dbType match {
      case DbType.H2Database => H2StorageBackend
      case DbType.Postgres => PostgresStorageBackend
      case DbType.Oracle => OracleStorageBackend
    }
}
