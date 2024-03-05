// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Time.Timestamp
import com.digitalasset.canton.ledger.api.domain.ParticipantId
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.index.v2.MeteringStore.{
  ParticipantMetering,
  ReportData,
}
import com.digitalasset.canton.ledger.participant.state.index.v2.{
  IndexerPartyDetails,
  PackageDetails,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.store.EventSequentialId
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  RawActiveContract,
  RawAssignEvent,
  RawUnassignEvent,
}
import com.digitalasset.canton.platform.store.backend.MeteringParameterStorageBackend.LedgerMeteringEnd
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.PruneUptoInclusiveAndLedgerEnd
import com.digitalasset.canton.platform.store.backend.common.{
  EventReaderQueries,
  TransactionPointwiseQueries,
  TransactionStreamingQueries,
}
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.digitalasset.canton.platform.store.entries.{
  ConfigurationEntry,
  PackageLedgerEntry,
  PartyLedgerEntry,
}
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.KeyState
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.tracing.TraceContext

import java.sql.Connection
import javax.sql.DataSource
import scala.annotation.unused

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
  def deletePartiallyIngestedData(ledgerEnd: ParameterStorageBackend.LedgerEnd)(
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
    * @return the current LedgerEnd
    */
  def ledgerEnd(connection: Connection): ParameterStorageBackend.LedgerEnd

  /** Part of pruning process, this needs to be in the same transaction as the other pruning related database operations
    */
  def updatePrunedUptoInclusive(prunedUpToInclusive: Offset)(connection: Connection): Unit

  def prunedUpToInclusive(connection: Connection): Option[Offset]

  def prunedUpToInclusiveAndLedgerEnd(connection: Connection): PruneUptoInclusiveAndLedgerEnd

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
    *    No significant CPU load, mostly blocking JDBC communication with the database backend.
    *
    * This method is NOT safe to call concurrently.
    */
  def initializeParameters(
      params: ParameterStorageBackend.IdentityParams,
      loggerFactory: NamedLoggerFactory,
  )(connection: Connection): Unit

  /** Returns the ledger identity parameters, or None if the database hasn't been initialized yet. */
  def ledgerIdentity(connection: Connection): Option[ParameterStorageBackend.IdentityParams]
}

object MeteringParameterStorageBackend {
  final case class LedgerMeteringEnd(offset: Offset, timestamp: Timestamp)
}

trait MeteringParameterStorageBackend {

  /** Initialize the ledger metering end parameters if unset */
  def initializeLedgerMeteringEnd(init: LedgerMeteringEnd, loggerFactory: NamedLoggerFactory)(
      connection: Connection
  )(implicit
      traceContext: TraceContext
  ): Unit

  /** The timestamp and offset for which billable metering is available */
  def ledgerMeteringEnd(connection: Connection): Option[LedgerMeteringEnd]

  /** The timestamp and offset for which final metering is available */
  def assertLedgerMeteringEnd(connection: Connection): LedgerMeteringEnd

  /** Update the timestamp and offset for which billable metering is available */
  def updateLedgerMeteringEnd(ledgerMeteringEnd: LedgerMeteringEnd)(connection: Connection): Unit
}

object ParameterStorageBackend {
  final case class LedgerEnd(lastOffset: Offset, lastEventSeqId: Long, lastStringInterningId: Int) {
    def lastOffsetOption: Option[Offset] =
      if (lastOffset == Offset.beforeBegin) None else Some(lastOffset)
  }
  object LedgerEnd {
    val beforeBegin: ParameterStorageBackend.LedgerEnd =
      ParameterStorageBackend.LedgerEnd(Offset.beforeBegin, EventSequentialId.beforeBegin, 0)
  }
  final case class IdentityParams(participantId: ParticipantId)

  final case class PruneUptoInclusiveAndLedgerEnd(
      pruneUptoInclusive: Option[Offset],
      ledgerEnd: Offset,
  )
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
  def parties(parties: Seq[Party])(connection: Connection): List[IndexerPartyDetails]
  def knownParties(connection: Connection): List[IndexerPartyDetails]
}

trait PackageStorageBackend {
  def lfPackages(connection: Connection): Map[PackageId, PackageDetails]

  def lfArchive(packageId: PackageId)(connection: Connection): Option[Array[Byte]]

  def packageEntries(
      startExclusive: Offset,
      endInclusive: Offset,
      pageSize: Int,
      queryOffset: Long,
  )(connection: Connection): Vector[(Offset, PackageLedgerEntry)]
}

trait CompletionStorageBackend {
  def commandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Party],
      limit: Int,
  )(connection: Connection): Vector[CompletionStreamResponse]

  /** Part of pruning process, this needs to be in the same transaction as the other pruning related database operations
    */
  def pruneCompletions(
      pruneUpToInclusive: Offset
  )(connection: Connection, traceContext: TraceContext): Unit
}

trait ContractStorageBackend {
  def keyState(key: Key, validAt: Offset)(connection: Connection): KeyState
  def archivedContracts(contractIds: Seq[ContractId], before: Offset)(
      connection: Connection
  ): Map[ContractId, ContractStorageBackend.RawArchivedContract]
  def createdContracts(contractIds: Seq[ContractId], before: Offset)(
      connection: Connection
  ): Map[ContractId, ContractStorageBackend.RawCreatedContract]
  def assignedContracts(contractIds: Seq[ContractId])(
      connection: Connection
  ): Map[ContractId, ContractStorageBackend.RawCreatedContract]
}

object ContractStorageBackend {
  sealed trait RawContractState

  final case class RawCreatedContract(
      templateId: String,
      packageName: String,
      flatEventWitnesses: Set[Party],
      createArgument: Array[Byte],
      createArgumentCompression: Option[Int],
      ledgerEffectiveTime: Timestamp,
      signatories: Set[Party],
      createKey: Option[Array[Byte]],
      createKeyCompression: Option[Int],
      keyMaintainers: Option[Set[Party]],
      driverMetadata: Option[Array[Byte]],
  ) extends RawContractState

  final case class RawArchivedContract(
      flatEventWitnesses: Set[Party]
  ) extends RawContractState

  class RawContract(
      val templateId: String,
      val packageName: String,
      val createArgument: Array[Byte],
      val createArgumentCompression: Option[Int],
  )
}

trait EventStorageBackend {

  def transactionPointwiseQueries: TransactionPointwiseQueries
  def transactionStreamingQueries: TransactionStreamingQueries
  def eventReaderQueries: EventReaderQueries

  /** Part of pruning process, this needs to be in the same transaction as the other pruning related database operations
    */
  def pruneEvents(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      incompletReassignmentOffsets: Vector[Offset],
  )(implicit
      connection: Connection,
      traceContext: TraceContext,
  ): Unit

  def activeContractCreateEventBatchV2(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Set[Party],
      endInclusive: Long,
  )(connection: Connection): Vector[RawActiveContract]

  def activeContractAssignEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Set[Party],
      endInclusive: Long,
  )(connection: Connection): Vector[RawActiveContract]

  def fetchAssignEventIdsForStakeholder(
      stakeholder: Party,
      templateId: Option[Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long]

  def fetchUnassignEventIdsForStakeholder(
      stakeholder: Party,
      templateId: Option[Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long]

  def assignEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Set[Party],
  )(connection: Connection): Vector[RawAssignEvent]

  def unassignEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Set[Party],
  )(connection: Connection): Vector[RawUnassignEvent]

  def lookupAssignSequentialIdByOffset(
      offsets: Iterable[String]
  )(connection: Connection): Vector[Long]

  def lookupUnassignSequentialIdByOffset(
      offsets: Iterable[String]
  )(connection: Connection): Vector[Long]

  def lookupAssignSequentialIdByContractId(
      contractIds: Iterable[String]
  )(connection: Connection): Vector[Long]

  def lookupCreateSequentialIdByContractId(
      contractIds: Iterable[String]
  )(connection: Connection): Vector[Long]

  def maxEventSequentialId(untilInclusiveOffset: Offset)(
      connection: Connection
  ): Long
}

object EventStorageBackend {
  final case class Entry[+E](
      eventOffset: Offset,
      transactionId: String,
      nodeIndex: Int,
      eventSequentialId: Long,
      ledgerEffectiveTime: Timestamp,
      commandId: String,
      workflowId: String,
      domainId: String,
      traceContext: Option[Array[Byte]],
      recordTime: Timestamp,
      event: E,
  )

  final case class RawCreatedEvent(
      updateId: String,
      contractId: String,
      templateId: Identifier,
      packageName: PackageName,
      witnessParties: Set[String],
      signatories: Set[String],
      observers: Set[String],
      createArgument: Array[Byte],
      createArgumentCompression: Option[Int],
      createKeyMaintainers: Set[String],
      createKeyValue: Option[Array[Byte]],
      createKeyValueCompression: Option[Int],
      ledgerEffectiveTime: Timestamp,
      createKeyHash: Option[Hash],
      driverMetadata: Array[Byte],
  )

  final case class RawActiveContract(
      workflowId: Option[String],
      domainId: String,
      reassignmentCounter: Long,
      rawCreatedEvent: RawCreatedEvent,
      eventSequentialId: Long,
  )

  final case class RawUnassignEvent(
      updateId: String,
      commandId: Option[String],
      workflowId: Option[String],
      offset: String,
      sourceDomainId: String,
      targetDomainId: String,
      unassignId: String,
      submitter: Option[String],
      reassignmentCounter: Long,
      contractId: String,
      templateId: Identifier,
      witnessParties: Set[String],
      assignmentExclusivity: Option[Timestamp],
      traceContext: Option[Array[Byte]],
      recordTime: Timestamp,
  )

  final case class RawAssignEvent(
      commandId: Option[String],
      workflowId: Option[String],
      offset: String,
      sourceDomainId: String,
      targetDomainId: String,
      unassignId: String,
      submitter: Option[String],
      reassignmentCounter: Long,
      rawCreatedEvent: RawCreatedEvent,
      traceContext: Option[Array[Byte]],
      recordTime: Timestamp,
  )
}

trait DataSourceStorageBackend {
  def createDataSource(
      dataSourceConfig: DataSourceStorageBackend.DataSourceConfig,
      loggerFactory: NamedLoggerFactory,
      connectionInitHook: Option[Connection => Unit] = None,
  ): DataSource

  def checkCompatibility(@unused connection: Connection)(implicit
      @unused traceContext: TraceContext
  ): Unit = ()

  def checkDatabaseAvailable(connection: Connection): Unit
}

object DataSourceStorageBackend {

  /** @param jdbcUrl JDBC URL of the database, parameter to establish the connection between the application and the database
    * @param postgresConfig configurations which apply only for the PostgresSQL backend
    */
  final case class DataSourceConfig(
      jdbcUrl: String,
      postgresConfig: PostgresDataSourceConfig = PostgresDataSourceConfig(),
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
  final case class Lock(lockId: LockId, lockMode: LockMode)

  trait LockId

  sealed trait LockMode
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

trait MeteringStorageReadBackend {

  def reportData(
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[ApplicationId],
  )(connection: Connection): ReportData
}

trait MeteringStorageWriteBackend {

  /** This method will return the maximum offset of the lapi_transaction_metering record
    * which has an offset greater than the from offset and a timestamp prior to the
    * to timestamp, if any.
    *
    * Note that the offset returned may not have been fully ingested. This is to allow the metering to wait if there
    * are still un-fully ingested records withing the time window.
    */
  def transactionMeteringMaxOffset(from: Offset, to: Timestamp)(
      connection: Connection
  ): Option[Offset]

  /** This method will return all transaction metering records between the from offset (exclusive)
    * and the to offset (inclusive).  It is called prior to aggregation.
    */
  def selectTransactionMetering(from: Offset, to: Offset)(
      connection: Connection
  ): Map[ApplicationId, Int]

  /** This method will delete transaction metering records between the from offset (exclusive)
    * and the to offset (inclusive).  It is called following aggregation.
    */
  def deleteTransactionMetering(from: Offset, to: Offset)(connection: Connection): Unit

  def insertParticipantMetering(metering: Vector[ParticipantMetering])(connection: Connection): Unit

  /** Test Only - will be removed once reporting can be based if participant metering */
  def allParticipantMetering()(connection: Connection): Vector[ParticipantMetering]

}
