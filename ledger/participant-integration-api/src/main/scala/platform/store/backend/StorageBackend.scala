// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.sql.Connection
import javax.sql.DataSource

import com.daml.ledger.api.domain.{LedgerId, ParticipantId, PartyDetails, User, UserRight}
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.UserId
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.ledger.EventId
import com.daml.logging.LoggingContext
import com.daml.platform
import com.daml.platform.store.EventSequentialId
import com.daml.platform.store.appendonlydao.events.{ContractId, EventsTable, Key, Raw}
import com.daml.platform.store.backend.EventStorageBackend.{FilterParams, RangeParams}
import com.daml.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.daml.platform.store.entries.{ConfigurationEntry, PackageLedgerEntry, PartyLedgerEntry}
import com.daml.platform.store.interfaces.LedgerDaoContractsReader.KeyState
import com.daml.platform.store.interning.StringInterning
import com.daml.scalautil.NeverEqualsOverride

import scala.annotation.unused
import scala.util.Try

/** Encapsulates the interface which hides database technology specific implementations.
  * Naming convention for the interface methods, which requiring Connection:
  *  - read operations are represented as nouns (plural, singular form indicates cardinality)
  *  - write operations are represented as verbs
  */

trait ResetStorageBackend {

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
    * @param stringInterning will be used to switch ingested strings to the internal integers
    * @return the database-specific batch DTO, which can be inserted via insertBatch
    */
  def batch(dbDtos: Vector[DbDto], stringInterning: StringInterning): DB_BATCH

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
    ledgerEnd(connection).getOrElse(ParameterStorageBackend.LedgerEndBeforeBegin)

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
  case class LedgerEnd(lastOffset: Offset, lastEventSeqId: Long, lastStringInterningId: Int)
  case class IdentityParams(ledgerId: LedgerId, participantId: ParticipantId)

  final val LedgerEndBeforeBegin =
    ParameterStorageBackend.LedgerEnd(Offset.beforeBegin, EventSequentialId.beforeBegin, 0)
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
  def deduplicatedUntil(deduplicationKey: String)(connection: Connection): Timestamp
  def upsertDeduplicationEntry(
      key: String,
      submittedAt: Timestamp,
      deduplicateUntil: Timestamp,
  )(connection: Connection)(implicit loggingContext: LoggingContext): Int
  def removeExpiredDeduplicationData(currentTime: Timestamp)(connection: Connection): Unit
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
  def maximumLedgerTime(ids: Set[ContractId])(connection: Connection): Try[Option[Timestamp]]
  def keyState(key: Key, validAt: Long)(connection: Connection): KeyState
  def contractState(contractId: ContractId, before: Long)(
      connection: Connection
  ): Option[ContractStorageBackend.RawContractState]
  def activeContractWithArgument(readers: Set[Ref.Party], contractId: ContractId)(
      connection: Connection
  ): Option[ContractStorageBackend.RawContract]
  def activeContractWithoutArgument(readers: Set[Ref.Party], contractId: ContractId)(
      connection: Connection
  ): Option[String]
  def contractKey(readers: Set[Ref.Party], key: Key)(
      connection: Connection
  ): Option[ContractId]
  def contractStateEvents(startExclusive: Long, endInclusive: Long)(
      connection: Connection
  ): Vector[ContractStorageBackend.RawContractStateEvent]
}

object ContractStorageBackend {
  case class RawContractState(
      templateId: Option[String],
      flatEventWitnesses: Set[Ref.Party],
      createArgument: Option[Array[Byte]],
      createArgumentCompression: Option[Int],
      eventKind: Int,
      ledgerEffectiveTime: Option[Timestamp],
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
      ledgerEffectiveTime: Option[Timestamp],
      createKeyValue: Option[Array[Byte]],
      createKeyCompression: Option[Int],
      createArgument: Option[Array[Byte]],
      createArgumentCompression: Option[Int],
      flatEventWitnesses: Set[Ref.Party],
      eventSequentialId: Long,
      offset: Offset,
  )
}

trait EventStorageBackend {

  /** Part of pruning process, this needs to be in the same transaction as the other pruning related database operations
    */
  def pruneEvents(pruneUpToInclusive: Offset, pruneAllDivulgedContracts: Boolean)(
      connection: Connection,
      loggingContext: LoggingContext,
  ): Unit
  def isPruningOffsetValidAgainstMigration(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      connection: Connection,
  ): Boolean
  def transactionEvents(
      rangeParams: RangeParams,
      filterParams: FilterParams,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def activeContractEventIds(
      partyFilter: Ref.Party,
      templateIdFilter: Option[Ref.Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long]
  def activeContractEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Set[Ref.Party],
      endInclusive: Long,
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
  ): Vector[EventStorageBackend.RawTransactionEvent]
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

  case class RawTransactionEvent(
      eventKind: Int,
      transactionId: String,
      nodeIndex: Int,
      commandId: Option[String],
      workflowId: Option[String],
      eventId: EventId,
      contractId: platform.store.appendonlydao.events.ContractId,
      templateId: Option[platform.store.appendonlydao.events.Identifier],
      ledgerEffectiveTime: Option[Timestamp],
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

trait StringInterningStorageBackend {
  def loadStringInterningEntries(fromIdExclusive: Int, untilIdInclusive: Int)(
      connection: Connection
  ): Iterable[(Int, String)]
}

trait UserManagementStorageBackend {

  def createUser(user: User)(connection: Connection): Int

  def deleteUser(id: UserId)(connection: Connection): Boolean

  def getUser(id: UserId)(connection: Connection): Option[UserManagementStorageBackend.DbUser]

  def getUsers()(connection: Connection): Vector[User]

  /** @return true if the right didn't exist and we have just added it.
    */
  def addUserRight(internalId: Int, right: UserRight)(
      connection: Connection
  ): Boolean

  /** @return true if the right existed and we have just deleted it.
    */
  def deleteUserRight(internalId: Int, right: UserRight)(connection: Connection): Boolean

  def userRightExists(internalId: Int, right: UserRight)(connection: Connection): Boolean

  def getUserRights(internalId: Int)(connection: Connection): Set[UserRight]

  def countUserRights(internalId: Int)(connection: Connection): Int

}

object UserManagementStorageBackend {
  case class DbUser(internalId: Int, domainUser: User)
}
