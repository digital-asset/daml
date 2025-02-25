// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.ParticipantId
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.*
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.{
  ParticipantMetering,
  ReportData,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.indexer.parallel.PostPublishData
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
  RawActiveContract,
  RawAssignEvent,
  RawParticipantAuthorization,
  RawUnassignEvent,
  SynchronizerOffset,
  UnassignProperties,
}
import com.digitalasset.canton.platform.store.backend.MeteringParameterStorageBackend.LedgerMeteringEnd
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.PruneUptoInclusiveAndLedgerEnd
import com.digitalasset.canton.platform.store.backend.common.{
  EventReaderQueries,
  TransactionPointwiseQueries,
  TransactionStreamingQueries,
}
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.KeyState
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.PackageVersion
import com.digitalasset.daml.lf.data.Time.Timestamp

import java.sql.Connection
import javax.sql.DataSource
import scala.annotation.unused

/** Encapsulates the interface which hides database technology specific implementations. Naming
  * convention for the interface methods, which requiring Connection:
  *   - read operations are represented as nouns (plural, singular form indicates cardinality)
  *   - write operations are represented as verbs
  */

trait ResetStorageBackend {

  /** Truncates ALL storage backend tables. Does not touch other tables, like the Flyway history
    * table. The result is a database that looks the same as a freshly created database with Flyway
    * migrations applied.
    */
  def resetAll(connection: Connection): Unit
}

trait IngestionStorageBackend[DB_BATCH] {

  /** The CPU intensive batching operation hides the batching logic, and the mapping to the database
    * specific representation of the inserted data. This should be pure CPU logic without IO.
    *
    * @param dbDtos
    *   is a collection of DbDto from which the batch is formed
    * @param stringInterning
    *   will be used to switch ingested strings to the internal integers
    * @return
    *   the database-specific batch DTO, which can be inserted via insertBatch
    */
  def batch(dbDtos: Vector[DbDto], stringInterning: StringInterning): DB_BATCH

  /** Using a JDBC connection, a batch will be inserted into the database. No significant CPU load,
    * mostly blocking JDBC communication with the database backend.
    *
    * @param connection
    *   to be used when inserting the batch
    * @param batch
    *   to be inserted
    */
  def insertBatch(connection: Connection, batch: DB_BATCH): Unit

  /** Deletes all partially ingested data, written during a non-graceful stop of previous indexing.
    * No significant CPU load, mostly blocking JDBC communication with the database backend.
    *
    * @param ledgerEnd
    *   the current ledger end, or None if no ledger end exists
    * @param connection
    *   to be used when inserting the batch
    */
  def deletePartiallyIngestedData(ledgerEnd: Option[ParameterStorageBackend.LedgerEnd])(
      connection: Connection
  ): Unit
}

trait ParameterStorageBackend {

  /** This method is used to update the new observable ledger end. No significant CPU load, mostly
    * blocking JDBC communication with the database backend.
    *
    * @param connection
    *   to be used when updating the parameters table
    */
  def updateLedgerEnd(
      ledgerEnd: ParameterStorageBackend.LedgerEnd,
      lastSynchronizerIndex: Map[SynchronizerId, SynchronizerIndex] = Map.empty,
  )(connection: Connection): Unit

  /** Query the current ledger end, read from the parameters table. No significant CPU load, mostly
    * blocking JDBC communication with the database backend.
    *
    * @param connection
    *   to be used to get the LedgerEnd
    * @return
    *   the current LedgerEnd
    */
  def ledgerEnd(connection: Connection): Option[ParameterStorageBackend.LedgerEnd]

  def cleanSynchronizerIndex(synchronizerId: SynchronizerId)(
      connection: Connection
  ): Option[SynchronizerIndex]

  /** Part of pruning process, this needs to be in the same transaction as the other pruning related
    * database operations
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

  def updatePostProcessingEnd(
      postProcessingEnd: Option[Offset]
  )(connection: Connection): Unit

  def postProcessingEnd(
      connection: Connection
  ): Option[Offset]

  /** Initializes the parameters table and verifies or updates ledger identity parameters. This
    * method is idempotent:
    *   - If no identity parameters are stored, then they are set to the given value.
    *   - If identity parameters are stored, then they are compared to the given ones.
    *   - Ledger identity parameters are written at most once, and are never overwritten. No
    *     significant CPU load, mostly blocking JDBC communication with the database backend.
    *
    * This method is NOT safe to call concurrently.
    */
  def initializeParameters(
      params: ParameterStorageBackend.IdentityParams,
      loggerFactory: NamedLoggerFactory,
  )(connection: Connection): Unit

  /** Returns the ledger identity parameters, or None if the database hasn't been initialized yet.
    */
  def ledgerIdentity(connection: Connection): Option[ParameterStorageBackend.IdentityParams]
}

object MeteringParameterStorageBackend {
  final case class LedgerMeteringEnd(offset: Option[Offset], timestamp: Timestamp)
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
  final case class LedgerEnd(
      lastOffset: Offset,
      lastEventSeqId: Long,
      lastStringInterningId: Int,
      lastPublicationTime: CantonTimestamp,
  )

  object LedgerEnd {
    val beforeBegin: Option[ParameterStorageBackend.LedgerEnd] = None
  }
  final case class IdentityParams(participantId: ParticipantId)

  final case class PruneUptoInclusiveAndLedgerEnd(
      pruneUptoInclusive: Option[Offset],
      ledgerEnd: Option[Offset],
  )
}

trait PartyStorageBackend {
  def parties(parties: Seq[Party])(connection: Connection): List[IndexerPartyDetails]
  def knownParties(fromExcl: Option[Party], maxResults: Int)(
      connection: Connection
  ): List[IndexerPartyDetails]
}

trait CompletionStorageBackend {
  def commandCompletions(
      startInclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Party],
      limit: Int,
  )(connection: Connection): Vector[CompletionStreamResponse]

  def commandCompletionsForRecovery(
      startInclusive: Offset,
      endInclusive: Offset,
  )(connection: Connection): Vector[PostPublishData]

  /** Part of pruning process, this needs to be in the same transaction as the other pruning related
    * database operations
    */
  def pruneCompletions(
      pruneUpToInclusive: Offset
  )(connection: Connection, traceContext: TraceContext): Unit
}

trait ContractStorageBackend {

  /** Returns true if the batch lookup is implemented */
  def supportsBatchKeyStateLookups: Boolean

  /** Batch lookup of key states
    *
    * If the backend does not support batch lookups, the implementation will fall back to sequential
    * lookups
    */
  def keyStates(keys: Seq[Key], validAt: Offset)(connection: Connection): Map[Key, KeyState]

  /** Sequential lookup of key states */
  def keyState(key: Key, validAt: Offset)(connection: Connection): KeyState

  def archivedContracts(contractIds: Seq[ContractId], before: Offset)(
      connection: Connection
  ): Map[ContractId, ContractStorageBackend.RawArchivedContract]
  def createdContracts(contractIds: Seq[ContractId], before: Offset)(
      connection: Connection
  ): Map[ContractId, ContractStorageBackend.RawCreatedContract]
  def assignedContracts(contractIds: Seq[ContractId], before: Offset)(
      connection: Connection
  ): Map[ContractId, ContractStorageBackend.RawCreatedContract]
}

object ContractStorageBackend {
  sealed trait RawContractState

  final case class RawCreatedContract(
      templateId: String,
      packageName: String,
      packageVersion: Option[String],
      flatEventWitnesses: Set[Party],
      createArgument: Array[Byte],
      createArgumentCompression: Option[Int],
      ledgerEffectiveTime: Timestamp,
      signatories: Set[Party],
      createKey: Option[Array[Byte]],
      createKeyCompression: Option[Int],
      keyMaintainers: Option[Set[Party]],
      driverMetadata: Array[Byte],
  ) extends RawContractState

  final case class RawArchivedContract(
      flatEventWitnesses: Set[Party]
  ) extends RawContractState
}

trait EventStorageBackend {

  def transactionPointwiseQueries: TransactionPointwiseQueries
  def transactionStreamingQueries: TransactionStreamingQueries
  def eventReaderQueries: EventReaderQueries

  /** Part of pruning process, this needs to be in the same transaction as the other pruning related
    * database operations
    */
  def pruneEvents(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      incompleteReassignmentOffsets: Vector[Offset],
  )(implicit
      connection: Connection,
      traceContext: TraceContext,
  ): Unit

  def activeContractCreateEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Party]],
      endInclusive: Long,
  )(connection: Connection): Vector[RawActiveContract]

  def activeContractAssignEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Party]],
      endInclusive: Long,
  )(connection: Connection): Vector[RawActiveContract]

  def fetchAssignEventIdsForStakeholder(
      stakeholderO: Option[Party],
      templateId: Option[Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long]

  def fetchUnassignEventIdsForStakeholder(
      stakeholderO: Option[Party],
      templateId: Option[Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long]

  def assignEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Party]],
  )(connection: Connection): Vector[Entry[RawAssignEvent]]

  def unassignEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Party]],
  )(connection: Connection): Vector[Entry[RawUnassignEvent]]

  def lookupAssignSequentialIdByOffset(
      offsets: Iterable[Long]
  )(connection: Connection): Vector[Long]

  def lookupUnassignSequentialIdByOffset(
      offsets: Iterable[Long]
  )(connection: Connection): Vector[Long]

  def lookupAssignSequentialIdBy(
      unassignProperties: Iterable[UnassignProperties]
  )(connection: Connection): Map[UnassignProperties, Long]

  def lookupCreateSequentialIdByContractId(
      contractIds: Iterable[ContractId]
  )(connection: Connection): Vector[Long]

  def maxEventSequentialId(untilInclusiveOffset: Option[Offset])(
      connection: Connection
  ): Long

  def firstSynchronizerOffsetAfterOrAt(
      synchronizerId: SynchronizerId,
      afterOrAtRecordTimeInclusive: Timestamp,
  )(connection: Connection): Option[SynchronizerOffset]

  def lastSynchronizerOffsetBeforeOrAt(
      synchronizerIdO: Option[SynchronizerId],
      beforeOrAtOffsetInclusive: Offset,
  )(connection: Connection): Option[SynchronizerOffset]

  def synchronizerOffset(offset: Offset)(connection: Connection): Option[SynchronizerOffset]

  def firstSynchronizerOffsetAfterOrAtPublicationTime(
      afterOrAtPublicationTimeInclusive: Timestamp
  )(connection: Connection): Option[SynchronizerOffset]

  def lastSynchronizerOffsetBeforeOrAtPublicationTime(
      beforeOrAtPublicationTimeInclusive: Timestamp
  )(connection: Connection): Option[SynchronizerOffset]

  def archivals(fromExclusive: Option[Offset], toInclusive: Offset)(
      connection: Connection
  ): Set[ContractId]

  def fetchTopologyPartyEventIds(
      party: Option[Party],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long]

  def topologyPartyEventBatch(
      eventSequentialIds: Iterable[Long]
  )(connection: Connection): Vector[RawParticipantAuthorization]

  def topologyEventPublishedOnRecordTime(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
  )(connection: Connection): Boolean
}

object EventStorageBackend {
  final case class Entry[+E](
      offset: Long,
      updateId: String,
      eventSequentialId: Long,
      ledgerEffectiveTime: Timestamp,
      commandId: Option[String],
      workflowId: Option[String],
      synchronizerId: String,
      traceContext: Option[Array[Byte]],
      recordTime: Timestamp,
      event: E,
  )

  sealed trait RawEvent {
    def templateId: Identifier
    def witnessParties: Set[String]
  }
  // TODO(#23504) keep only RawEvent or RawAcsDeltaEvent
  sealed trait RawFlatEvent extends RawEvent
  // TODO(#23504) keep only RawEvent or RawLedgerEffectsEvent
  sealed trait RawTreeEvent extends RawEvent

  final case class RawCreatedEvent(
      updateId: String,
      offset: Long,
      nodeId: Int,
      contractId: ContractId,
      templateId: Identifier,
      packageName: PackageName,
      packageVersion: Option[PackageVersion],
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
  ) extends RawFlatEvent
      with RawTreeEvent

  final case class RawArchivedEvent(
      updateId: String,
      offset: Long,
      nodeId: Int,
      contractId: ContractId,
      templateId: Identifier,
      packageName: PackageName,
      witnessParties: Set[String],
  ) extends RawFlatEvent

  final case class RawExercisedEvent(
      updateId: String,
      offset: Long,
      nodeId: Int,
      contractId: ContractId,
      templateId: Identifier,
      packageName: PackageName,
      exerciseConsuming: Boolean,
      exerciseChoice: String,
      exerciseArgument: Array[Byte],
      exerciseArgumentCompression: Option[Int],
      exerciseResult: Option[Array[Byte]],
      exerciseResultCompression: Option[Int],
      exerciseActors: Seq[String],
      exerciseLastDescendantNodeId: Int,
      witnessParties: Set[String],
  ) extends RawTreeEvent

  final case class RawActiveContract(
      workflowId: Option[String],
      synchronizerId: String,
      reassignmentCounter: Long,
      rawCreatedEvent: RawCreatedEvent,
      eventSequentialId: Long,
  )

  final case class RawUnassignEvent(
      sourceSynchronizerId: String,
      targetSynchronizerId: String,
      unassignId: String,
      submitter: Option[String],
      reassignmentCounter: Long,
      contractId: ContractId,
      templateId: Identifier,
      packageName: PackageName,
      witnessParties: Set[String],
      assignmentExclusivity: Option[Timestamp],
  )

  final case class RawAssignEvent(
      sourceSynchronizerId: String,
      targetSynchronizerId: String,
      unassignId: String,
      submitter: Option[String],
      reassignmentCounter: Long,
      rawCreatedEvent: RawCreatedEvent,
  )

  final case class SynchronizerOffset(
      offset: Offset,
      synchronizerId: SynchronizerId,
      recordTime: Timestamp,
      publicationTime: Timestamp,
  )

  final case class RawParticipantAuthorization(
      offset: Offset,
      updateId: String,
      partyId: String,
      participantId: String,
      participant_permission: AuthorizationLevel,
      recordTime: Timestamp,
      synchronizerId: String,
      traceContext: Option[Array[Byte]],
  )

  def intToAuthorizationLevel(n: Int): AuthorizationLevel = n match {
    case 0 => Revoked
    case 1 => Submission
    case 2 => Confirmation
    case 3 => Observation
  }

  final case class UnassignProperties(
      contractId: ContractId,
      synchronizerId: String,
      sequentialId: Long,
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

  /** @param jdbcUrl
    *   JDBC URL of the database, parameter to establish the connection between the application and
    *   the database
    * @param postgresConfig
    *   configurations which apply only for the PostgresSQL backend
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
    * This operation is allowed to take some time to finish. It is not expected that it is used
    * during regular index/indexer operation.
    */
  def onlyForTestingVerifyIntegrity(failForEmptyDB: Boolean = true)(connection: Connection): Unit

  def onlyForTestingNumberOfAcceptedTransactionsFor(synchronizerId: SynchronizerId)(
      connection: Connection
  ): Int

  def onlyForTestingMoveLedgerEndBackToScratch()(connection: Connection): Unit
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

  /** This method will return the maximum offset of the lapi_transaction_metering record which has
    * an offset greater than the from offset and a timestamp prior to the to timestamp, if any.
    *
    * Note that the offset returned may not have been fully ingested. This is to allow the metering
    * to wait if there are still un-fully ingested records withing the time window.
    */
  def transactionMeteringMaxOffset(from: Option[Offset], to: Timestamp)(
      connection: Connection
  ): Option[Offset]

  /** This method will return all transaction metering records between the from offset (exclusive)
    * and the to offset (inclusive). It is called prior to aggregation.
    */
  def selectTransactionMetering(from: Option[Offset], to: Offset)(
      connection: Connection
  ): Map[ApplicationId, Int]

  /** This method will delete transaction metering records between the from offset (exclusive) and
    * the to offset (inclusive). It is called following aggregation.
    */
  def deleteTransactionMetering(from: Option[Offset], to: Offset)(
      connection: Connection
  ): Unit

  def insertParticipantMetering(metering: Vector[ParticipantMetering])(connection: Connection): Unit

  /** Test Only - will be removed once reporting can be based if participant metering */
  def allParticipantMetering()(connection: Connection): Vector[ParticipantMetering]

}
