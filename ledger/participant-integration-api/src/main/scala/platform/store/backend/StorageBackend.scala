// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.MeteringStore.{ParticipantMetering, ReportData}
import com.daml.ledger.participant.state.index.v2.{IndexerPartyDetails, PackageDetails}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.ledger.EventId
import com.daml.logging.LoggingContext
import com.daml.platform.{ApplicationId, ContractId, Identifier, Key, PackageId, Party}
import com.daml.platform.store.EventSequentialId
import com.daml.platform.store.dao.events.Raw
import com.daml.platform.store.backend.MeteringParameterStorageBackend.LedgerMeteringEnd
import com.daml.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.daml.platform.store.entries.{ConfigurationEntry, PackageLedgerEntry, PartyLedgerEntry}
import com.daml.platform.store.interfaces.LedgerDaoContractsReader.KeyState
import com.daml.platform.store.interning.StringInterning
import com.daml.scalautil.NeverEqualsOverride
import java.sql.Connection
import javax.sql.DataSource

import com.daml.platform.store.backend.common.{
  TransactionPointwiseQueries,
  TransactionStreamingQueries,
}

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
  def initializeParameters(params: ParameterStorageBackend.IdentityParams)(connection: Connection)(
      implicit loggingContext: LoggingContext
  ): Unit

  /** Returns the ledger identity parameters, or None if the database hasn't been initialized yet. */
  def ledgerIdentity(connection: Connection): Option[ParameterStorageBackend.IdentityParams]
}

object MeteringParameterStorageBackend {
  case class LedgerMeteringEnd(offset: Offset, timestamp: Timestamp)
}

trait MeteringParameterStorageBackend {

  /** Initialize the ledger metering end parameters if unset */
  def initializeLedgerMeteringEnd(init: LedgerMeteringEnd)(connection: Connection)(implicit
      loggingContext: LoggingContext
  ): Unit

  /** The timestamp and offset for which billable metering is available */
  def ledgerMeteringEnd(connection: Connection): Option[LedgerMeteringEnd]

  /** The timestamp and offset for which final metering is available */
  def assertLedgerMeteringEnd(connection: Connection): LedgerMeteringEnd

  /** Update the timestamp and offset for which billable metering is available */
  def updateLedgerMeteringEnd(ledgerMeteringEnd: LedgerMeteringEnd)(connection: Connection): Unit
}

object ParameterStorageBackend {
  case class LedgerEnd(lastOffset: Offset, lastEventSeqId: Long, lastStringInterningId: Int) {
    def lastOffsetOption: Option[Offset] =
      if (lastOffset == Offset.beforeBegin) None else Some(lastOffset)
  }
  object LedgerEnd {
    val beforeBegin: ParameterStorageBackend.LedgerEnd =
      ParameterStorageBackend.LedgerEnd(Offset.beforeBegin, EventSequentialId.beforeBegin, 0)
  }
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
  )(connection: Connection, loggingContext: LoggingContext): Unit
}

trait ContractStorageBackend {
  def keyState(key: Key, validAt: Offset)(connection: Connection): KeyState
  def contractState(contractId: ContractId, before: Offset)(
      connection: Connection
  ): Option[ContractStorageBackend.RawContractState]
  def activeContractWithArgument(readers: Set[Party], contractId: ContractId)(
      connection: Connection
  ): Option[ContractStorageBackend.RawContract]
  def activeContractWithoutArgument(readers: Set[Party], contractId: ContractId)(
      connection: Connection
  ): Option[String]
  def contractStateEvents(startExclusive: Long, endInclusive: Long)(
      connection: Connection
  ): Vector[ContractStorageBackend.RawContractStateEvent]
}

object ContractStorageBackend {
  case class RawContractState(
      templateId: Option[String],
      flatEventWitnesses: Set[Party],
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
      templateId: Option[Identifier],
      ledgerEffectiveTime: Option[Timestamp],
      createKeyValue: Option[Array[Byte]],
      createKeyCompression: Option[Int],
      createArgument: Option[Array[Byte]],
      createArgumentCompression: Option[Int],
      flatEventWitnesses: Set[Party],
      eventSequentialId: Long,
      offset: Offset,
  )
}

trait EventStorageBackend {

  def transactionPointwiseQueries: TransactionPointwiseQueries
  def transactionStreamingQueries: TransactionStreamingQueries

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

  def activeContractEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Set[Party],
      endInclusive: Long,
  )(connection: Connection): Vector[EventStorageBackend.Entry[Raw.FlatEvent]]

  /** Max event sequential id of observable (create, consuming and nonconsuming exercise) events. */
  def maxEventSequentialIdOfAnObservableEvent(offset: Offset)(connection: Connection): Option[Long]

  // Used only in testing
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
      wildCardParties: Set[Party],
      partiesAndTemplates: Set[(Set[Party], Set[Identifier])],
  )

  case class RawTransactionEvent(
      eventKind: Int,
      transactionId: String,
      nodeIndex: Int,
      commandId: Option[String],
      workflowId: Option[String],
      eventId: EventId,
      contractId: ContractId,
      templateId: Option[Identifier],
      ledgerEffectiveTime: Option[Timestamp],
      createSignatories: Option[Array[String]],
      createObservers: Option[Array[String]],
      createAgreementText: Option[String],
      createKeyValue: Option[Array[Byte]],
      createKeyHash: Option[Hash],
      createKeyCompression: Option[Int],
      createArgument: Option[Array[Byte]],
      createArgumentCompression: Option[Int],
      treeEventWitnesses: Set[String],
      flatEventWitnesses: Set[String],
      submitters: Set[String],
      qualifiedChoiceName: Option[String],
      exerciseArgument: Option[Array[Byte]],
      exerciseArgumentCompression: Option[Int],
      exerciseResult: Option[Array[Byte]],
      exerciseResultCompression: Option[Int],
      exerciseActors: Option[Array[String]],
      exerciseChildEventIds: Option[Array[String]],
      eventSequentialId: Long,
      offset: Offset,
  ) extends NeverEqualsOverride

  final case class Entry[+E](
      eventOffset: Offset,
      transactionId: String,
      nodeIndex: Int,
      eventSequentialId: Long,
      ledgerEffectiveTime: Timestamp,
      commandId: String,
      workflowId: String,
      event: E,
  )
}

trait DataSourceStorageBackend {
  def createDataSource(
      dataSourceConfig: DataSourceStorageBackend.DataSourceConfig,
      connectionInitHook: Option[Connection => Unit] = None,
  )(implicit loggingContext: LoggingContext): DataSource

  def checkCompatibility(@unused connection: Connection)(implicit
      @unused loggingContext: LoggingContext
  ): Unit = ()

  def checkDatabaseAvailable(connection: Connection): Unit
}

object DataSourceStorageBackend {

  /** @param jdbcUrl JDBC URL of the database, parameter to establish the connection between the application and the database
    * @param postgresConfig configurations which apply only for the PostgresSQL backend
    */
  case class DataSourceConfig(
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

trait MeteringStorageReadBackend {

  def reportData(
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[ApplicationId],
  )(connection: Connection): ReportData
}

trait MeteringStorageWriteBackend {

  /** This method will return the maximum offset of the transaction_metering record
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
