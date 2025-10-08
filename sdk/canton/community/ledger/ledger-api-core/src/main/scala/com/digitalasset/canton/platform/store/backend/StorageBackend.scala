// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.ParticipantId
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.indexer.parallel.PostPublishData
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.*
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.PruneUptoInclusiveAndLedgerEnd
import com.digitalasset.canton.platform.store.backend.common.{
  EventPayloadSourceForUpdatesAcsDelta,
  EventPayloadSourceForUpdatesAcsDeltaLegacy,
  EventPayloadSourceForUpdatesLedgerEffects,
  EventPayloadSourceForUpdatesLedgerEffectsLegacy,
  EventReaderQueries,
  UpdatePointwiseQueries,
  UpdateStreamingQueries,
}
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.PaginationInput
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.KeyState
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{FullIdentifier, NameTypeConRef}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.google.common.annotations.VisibleForTesting

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

  /** The latest SynchronizerIndex for a synchronizerId until all events are processed fully and
    * published to the Ledger API DB. The Update which from this SynchronizerIndex originate has
    * smaller or equal offset than the current LedgerEnd: LedgerEnd and SynchronizerIndexes are
    * persisted consistently in one transaction.
    */
  def cleanSynchronizerIndex(synchronizerId: SynchronizerId)(
      connection: Connection
  ): Option[SynchronizerIndex]

  /** Part of pruning process, this needs to be in the same transaction as the other pruning related
    * database operations
    */
  def updatePrunedUptoInclusive(prunedUpToInclusive: Offset)(connection: Connection): Unit

  def prunedUpToInclusive(connection: Connection): Option[Offset]

  def prunedUpToInclusiveAndLedgerEnd(connection: Connection): PruneUptoInclusiveAndLedgerEnd

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
      userId: UserId,
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

  /** Batch lookup of key states
    *
    * If the backend does not support batch lookups, the implementation will fall back to sequential
    * lookups
    */
  def keyStatesNew(keys: Seq[Key], validAtEventSeqId: Long)(connection: Connection): Map[Key, Long]

  /** Sequential lookup of key states */
  def keyStateNew(key: Key, validAtEventSeqId: Long)(connection: Connection): Option[Long]

  def activeContractsNew(internalContractIds: Seq[Long], beforeEventSeqId: Long)(
      connection: Connection
  ): Map[Long, Boolean]

  def lastActivationsNew(synchronizerContracts: Iterable[(SynchronizerId, Long)])(
      connection: Connection
  ): Map[(SynchronizerId, Long), Long]

  /** Returns true if the batch lookup is implemented */
  def supportsBatchKeyStateLookups: Boolean

  /** Batch lookup of key states
    *
    * If the backend does not support batch lookups, the implementation will fall back to sequential
    * lookups
    */
  def keyStates(keys: Seq[Key], validAtEventSeqId: Long)(connection: Connection): Map[Key, KeyState]

  /** Sequential lookup of key states */
  def keyState(key: Key, validAtEventSeqId: Long)(connection: Connection): KeyState

  def archivedContracts(contractIds: Seq[ContractId], beforeEventSeqId: Long)(
      connection: Connection
  ): Set[ContractId]
  def createdContracts(contractIds: Seq[ContractId], beforeEventSeqId: Long)(
      connection: Connection
  ): Set[ContractId]
  def assignedContracts(contractIds: Seq[ContractId], beforeEventSeqId: Long)(
      connection: Connection
  ): Set[ContractId]

  def lastActivations(synchronizerContracts: Iterable[(SynchronizerId, ContractId)])(
      connection: Connection
  ): Map[(SynchronizerId, ContractId), Long]
}

object ContractStorageBackend {
  sealed trait RawContractState

  final case class RawCreatedContract(
      templateId: String,
      packageId: String,
      flatEventWitnesses: Set[Party],
      createArgument: Array[Byte],
      createArgumentCompression: Option[Int],
      ledgerEffectiveTime: Timestamp,
      signatories: Set[Party],
      createKey: Option[Array[Byte]],
      createKeyCompression: Option[Int],
      keyMaintainers: Option[Set[Party]],
      authenticationData: Array[Byte],
  ) extends RawContractState

  final case class RawArchivedContract(
      flatEventWitnesses: Set[Party]
  ) extends RawContractState
}

trait EventStorageBackend {

  def updatePointwiseQueries: UpdatePointwiseQueries
  def updateStreamingQueries: UpdateStreamingQueries
  def eventReaderQueries: EventReaderQueries

  /** Part of pruning process, this needs to be in the same transaction as the other pruning related
    * database operations
    */
  def pruneEventsLegacy(
      pruneUpToInclusive: Offset,
      incompleteReassignmentOffsets: Vector[Offset],
  )(implicit
      connection: Connection,
      traceContext: TraceContext,
  ): Unit

  /** Part of pruning process, this needs to be in the same transaction as the other pruning related
    * database operations
    */
  def pruneEvents(
      pruneUpToInclusive: Offset,
      incompleteReassignmentOffsets: Vector[Offset],
  )(implicit
      connection: Connection,
      traceContext: TraceContext,
  ): Unit =
    ??? // TODO(#28005): Implement

  def activeContractBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Party]],
      endInclusive: Long,
  )(connection: Connection): Vector[RawThinActiveContract]

  def activeContractCreateEventBatchLegacy(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Party]],
      endInclusive: Long,
  )(connection: Connection): Vector[RawActiveContractLegacy]

  def activeContractAssignEventBatchLegacy(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Party]],
      endInclusive: Long,
  )(connection: Connection): Vector[RawActiveContractLegacy]

  def fetchAssignEventIdsForStakeholderLegacy(
      stakeholderO: Option[Party],
      templateId: Option[NameTypeConRef],
  )(connection: Connection): PaginationInput => Vector[Long]

  def fetchUnassignEventIdsForStakeholderLegacy(
      stakeholderO: Option[Party],
      templateId: Option[NameTypeConRef],
  )(connection: Connection): PaginationInput => Vector[Long]

  def assignEventBatchLegacy(
      eventSequentialIds: SequentialIdBatch,
      allFilterParties: Option[Set[Party]],
  )(connection: Connection): Vector[Entry[RawAssignEventLegacy]]

  def unassignEventBatchLegacy(
      eventSequentialIds: SequentialIdBatch,
      allFilterParties: Option[Set[Party]],
  )(connection: Connection): Vector[Entry[RawUnassignEventLegacy]]

  def lookupAssignSequentialIdByOffset(
      offsets: Iterable[Long]
  )(connection: Connection): Vector[Long]

  def lookupAssignSequentialIdByOffsetLegacy(
      offsets: Iterable[Long]
  )(connection: Connection): Vector[Long]

  def lookupUnassignSequentialIdByOffset(
      offsets: Iterable[Long]
  )(connection: Connection): Vector[Long]

  def lookupUnassignSequentialIdByOffsetLegacy(
      offsets: Iterable[Long]
  )(connection: Connection): Vector[Long]

  def lookupAssignSequentialIdByLegacy(
      unassignProperties: Iterable[UnassignProperties]
  )(connection: Connection): Map[UnassignProperties, Long]

  def lookupCreateSequentialIdByContractIdLegacy(
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

  // Note: Added for offline party replication as CN is using it.
  def lastSynchronizerOffsetBeforeOrAtRecordTime(
      synchronizerId: SynchronizerId,
      beforeOrAtRecordTimeInclusive: Timestamp,
  )(connection: Connection): Option[SynchronizerOffset]

  /** The contracts which were archived or participant-divulged in the specified range. These are
    * the contracts in the ContractStore, which can be pruned in a single-synchronizer setup.
    */
  def prunableContracts(fromExclusive: Option[Offset], toInclusive: Offset)(
      connection: Connection
  ): Set[Long]

  def archivalsLegacy(fromExclusive: Option[Offset], toInclusive: Offset)(
      connection: Connection
  ): Set[ContractId]

  def fetchTopologyPartyEventIds(party: Option[Party])(
      connection: Connection
  ): PaginationInput => Vector[Long]

  def topologyPartyEventBatch(
      eventSequentialIds: SequentialIdBatch
  )(connection: Connection): Vector[RawParticipantAuthorization]

  def topologyEventOffsetPublishedOnRecordTime(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
  )(connection: Connection): Option[Offset]

  def fetchEventPayloadsAcsDelta(target: EventPayloadSourceForUpdatesAcsDelta)(
      eventSequentialIds: SequentialIdBatch,
      requestingParties: Option[Set[Party]],
  )(connection: Connection): Vector[RawThinAcsDeltaEvent]

  def fetchEventPayloadsAcsDeltaLegacy(target: EventPayloadSourceForUpdatesAcsDeltaLegacy)(
      eventSequentialIds: SequentialIdBatch,
      requestingParties: Option[Set[Party]],
  )(connection: Connection): Vector[Entry[RawAcsDeltaEventLegacy]]

  def fetchEventPayloadsLedgerEffects(target: EventPayloadSourceForUpdatesLedgerEffects)(
      eventSequentialIds: SequentialIdBatch,
      requestingParties: Option[Set[Ref.Party]],
  )(connection: Connection): Vector[RawThinLedgerEffectsEvent]

  def fetchEventPayloadsLedgerEffectsLegacy(
      target: EventPayloadSourceForUpdatesLedgerEffectsLegacy
  )(
      eventSequentialIds: SequentialIdBatch,
      requestingParties: Option[Set[Ref.Party]],
  )(connection: Connection): Vector[Entry[RawLedgerEffectsEventLegacy]]
}

object EventStorageBackend {
  sealed trait RawEvent extends Product with Serializable {
    def templateId: FullIdentifier
    def witnessParties: Set[String]
  }

  sealed trait RawAcsDeltaEvent extends RawEvent
  sealed trait RawLedgerEffectsEvent extends RawEvent

  sealed trait RawReassignmentEvent extends RawEvent {
    def reassignmentProperties: ReassignmentProperties
  }

  sealed trait RawTransactionEvent extends RawEvent {
    def ledgerEffectiveTime: Timestamp
    def transactionProperties: TransactionProperties
  }

  sealed trait RawThinEvent extends Product with Serializable

  sealed trait RawThinAcsDeltaEvent extends RawThinEvent
  sealed trait RawThinLedgerEffectsEvent extends RawThinEvent

  sealed trait RawThinTransactionEvent extends RawThinEvent
  sealed trait RawThinReassignmentEvent extends RawThinEvent

  final case class CommonEventProperties(
      eventSequentialId: Long,
      offset: Long,
      nodeId: Int,
      workflowId: Option[String],
      synchronizerId: String,
  )

  final case class CommonUpdateProperties(
      updateId: String,
      commandId: Option[String],
      traceContext: Array[Byte],
      recordTime: Timestamp,
  )

  final case class TransactionProperties(
      commonEventProperties: CommonEventProperties,
      commonUpdateProperties: CommonUpdateProperties,
      externalTransactionHash: Option[Array[Byte]],
  )

  final case class ReassignmentProperties(
      commonEventProperties: CommonEventProperties,
      commonUpdateProperties: CommonUpdateProperties,
      reassignmentId: String,
      submitter: Option[String],
      reassignmentCounter: Long,
  )

  final case class ThinCreatedEventProperties(
      representativePackageId: String,
      filteredAdditionalWitnessParties: Set[String],
      internalContractId: Long,
      requestingParties: Option[Set[String]],
      reassignmentCounter: Long,
      acsDelta: Boolean,
  )

  final case class FatCreatedEventProperties(
      thinCreatedEventProperties: ThinCreatedEventProperties,
      fatContract: FatContract,
  ) {
    def templateId: FullIdentifier =
      fatContract.templateId.toFullIdentifier(fatContract.packageName)

    def witnessParties: Set[String] =
      thinCreatedEventProperties.filteredAdditionalWitnessParties.iterator
        .++(fatContract.stakeholders.iterator.map(_.toString))
        .filter(party =>
          thinCreatedEventProperties.requestingParties match {
            case Some(requestingParties) => requestingParties.contains(party)
            case None => true
          }
        )
        .toSet
  }

  final case class RawThinActiveContract(
      commonEventProperties: CommonEventProperties,
      thinCreatedEventProperties: ThinCreatedEventProperties,
  ) extends RawThinEvent

  final case class RawFatActiveContract(
      commonEventProperties: CommonEventProperties,
      fatCreatedEventProperties: FatCreatedEventProperties,
  ) extends RawEvent {
    override def templateId: FullIdentifier = fatCreatedEventProperties.templateId

    override def witnessParties: Set[String] = fatCreatedEventProperties.witnessParties
  }

  final case class RawThinCreatedEvent(
      transactionProperties: TransactionProperties,
      thinCreatedEventProperties: ThinCreatedEventProperties,
  ) extends RawThinAcsDeltaEvent
      with RawThinLedgerEffectsEvent
      with RawThinTransactionEvent

  final case class RawFatCreatedEvent(
      transactionProperties: TransactionProperties,
      fatCreatedEventProperties: FatCreatedEventProperties,
  ) extends RawAcsDeltaEvent
      with RawLedgerEffectsEvent
      with RawTransactionEvent {
    override def templateId: FullIdentifier = fatCreatedEventProperties.templateId

    override def witnessParties: Set[String] = fatCreatedEventProperties.witnessParties

    override def ledgerEffectiveTime: Timestamp =
      fatCreatedEventProperties.fatContract.createdAt.time
  }

  final case class RawThinAssignEvent(
      reassignmentProperties: ReassignmentProperties,
      thinCreatedEventProperties: ThinCreatedEventProperties,
      sourceSynchronizerId: String,
  ) extends RawThinAcsDeltaEvent
      with RawThinLedgerEffectsEvent
      with RawThinReassignmentEvent

  final case class RawFatAssignEvent(
      reassignmentProperties: ReassignmentProperties,
      fatCreatedEventProperties: FatCreatedEventProperties,
  ) extends RawAcsDeltaEvent
      with RawLedgerEffectsEvent
      with RawReassignmentEvent {
    override def templateId: FullIdentifier = fatCreatedEventProperties.templateId

    override def witnessParties: Set[String] = fatCreatedEventProperties.witnessParties
  }

  final case class RawArchivedEvent(
      transactionProperties: TransactionProperties,
      contractId: ContractId,
      templateId: FullIdentifier,
      filteredStakeholderParties: Set[String],
      ledgerEffectiveTime: Timestamp,
  ) extends RawAcsDeltaEvent
      with RawTransactionEvent
      with RawThinAcsDeltaEvent
      with RawThinTransactionEvent {
    override def witnessParties: Set[String] = filteredStakeholderParties
  }

  final case class RawExercisedEvent(
      transactionProperties: TransactionProperties,
      contractId: ContractId,
      templateId: FullIdentifier,
      exerciseConsuming: Boolean,
      exerciseChoice: ChoiceName,
      exerciseChoiceInterface: Option[Ref.Identifier],
      exerciseArgument: Array[Byte],
      exerciseArgumentCompression: Option[Int],
      exerciseResult: Option[Array[Byte]],
      exerciseResultCompression: Option[Int],
      exerciseActors: Set[String],
      exerciseLastDescendantNodeId: Int,
      filteredAdditionalWitnessParties: Set[String],
      filteredStakeholderParties: Set[String],
      ledgerEffectiveTime: Timestamp,
      acsDelta: Boolean,
  ) extends RawLedgerEffectsEvent
      with RawTransactionEvent
      with RawThinLedgerEffectsEvent
      with RawThinTransactionEvent {
    override def witnessParties: Set[String] =
      filteredStakeholderParties ++ filteredAdditionalWitnessParties
  }

  final case class RawUnassignEvent(
      reassignmentProperties: ReassignmentProperties,
      contractId: ContractId,
      templateId: FullIdentifier,
      filteredStakeholderParties: Set[String],
      assignmentExclusivity: Option[Timestamp],
      targetSynchronizerId: String,
  ) extends RawAcsDeltaEvent
      with RawLedgerEffectsEvent
      with RawReassignmentEvent
      with RawThinAcsDeltaEvent
      with RawThinLedgerEffectsEvent
      with RawThinReassignmentEvent {
    override def witnessParties: Set[String] = filteredStakeholderParties
  }

  final case class Entry[+E](
      offset: Long,
      nodeId: Int,
      updateId: String,
      eventSequentialId: Long,
      ledgerEffectiveTime: Option[Timestamp],
      commandId: Option[String],
      workflowId: Option[String],
      synchronizerId: String,
      traceContext: Option[Array[Byte]],
      recordTime: Timestamp,
      externalTransactionHash: Option[Array[Byte]],
      event: E,
  ) {
    def map[T](f: E => T): Entry[T] = this.copy(event = f(event))
    def withEvent[T](t: T): Entry[T] = this.copy(event = t)
  }

  sealed trait RawEventLegacy {
    def templateId: FullIdentifier
    def witnessParties: Set[String]
  }
  sealed trait RawAcsDeltaEventLegacy extends RawEventLegacy
  sealed trait RawLedgerEffectsEventLegacy extends RawEventLegacy

  sealed trait RawReassignmentEventLegacy extends RawEventLegacy

  final case class RawCreatedEventLegacy(
      contractId: ContractId,
      templateId: FullIdentifier,
      representativePackageId: LfPackageId,
      witnessParties: Set[String],
      flatEventWitnesses: Set[String],
      signatories: Set[String],
      observers: Set[String],
      createArgument: Array[Byte],
      createArgumentCompression: Option[Int],
      createKeyMaintainers: Set[String],
      createKeyValue: Option[Array[Byte]],
      createKeyValueCompression: Option[Int],
      ledgerEffectiveTime: Timestamp,
      createKeyHash: Option[Hash],
      authenticationData: Array[Byte],
      internalContractId: Long,
  ) extends RawAcsDeltaEventLegacy
      with RawLedgerEffectsEventLegacy

  final case class RawArchivedEventLegacy(
      contractId: ContractId,
      templateId: FullIdentifier,
      witnessParties: Set[String],
  ) extends RawAcsDeltaEventLegacy

  final case class RawExercisedEventLegacy(
      contractId: ContractId,
      templateId: FullIdentifier,
      exerciseConsuming: Boolean,
      exerciseChoice: ChoiceName,
      exerciseChoiceInterface: Option[Ref.Identifier],
      exerciseArgument: Array[Byte],
      exerciseArgumentCompression: Option[Int],
      exerciseResult: Option[Array[Byte]],
      exerciseResultCompression: Option[Int],
      exerciseActors: Seq[String],
      exerciseLastDescendantNodeId: Int,
      witnessParties: Set[String],
      flatEventWitnesses: Set[String],
  ) extends RawLedgerEffectsEventLegacy

  final case class RawActiveContractLegacy(
      workflowId: Option[String],
      synchronizerId: String,
      reassignmentCounter: Long,
      rawCreatedEvent: RawCreatedEventLegacy,
      eventSequentialId: Long,
      nodeId: Int,
      offset: Long,
  )

  final case class RawUnassignEventLegacy(
      sourceSynchronizerId: String,
      targetSynchronizerId: String,
      reassignmentId: String,
      submitter: Option[String],
      reassignmentCounter: Long,
      contractId: ContractId,
      templateId: FullIdentifier,
      witnessParties: Set[String],
      assignmentExclusivity: Option[Timestamp],
  ) extends RawReassignmentEventLegacy

  final case class RawAssignEventLegacy(
      sourceSynchronizerId: String,
      targetSynchronizerId: String,
      reassignmentId: String,
      submitter: Option[String],
      reassignmentCounter: Long,
      rawCreatedEvent: RawCreatedEventLegacy,
  ) extends RawReassignmentEventLegacy {
    override def templateId: FullIdentifier = rawCreatedEvent.templateId
    override def witnessParties: Set[String] = rawCreatedEvent.witnessParties
  }

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
      authorizationEvent: AuthorizationEvent,
      recordTime: Timestamp,
      synchronizerId: String,
      traceContext: Option[Array[Byte]],
  )

  final case class UnassignProperties(
      contractId: ContractId,
      synchronizerId: String,
      sequentialId: Long,
  )

  sealed trait SequentialIdBatch
  object SequentialIdBatch {
    final case class IdRange(fromInclusive: Long, toInclusive: Long) extends SequentialIdBatch
    final case class Ids(ids: Iterable[Long]) extends SequentialIdBatch
  }
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
  @VisibleForTesting
  def verifyIntegrity(failForEmptyDB: Boolean = true)(connection: Connection): Unit

  @VisibleForTesting
  def numberOfAcceptedTransactionsFor(synchronizerId: SynchronizerId)(
      connection: Connection
  ): Int

  @VisibleForTesting
  def moveLedgerEndBackToScratch()(connection: Connection): Unit
}

trait StringInterningStorageBackend {
  def loadStringInterningEntries(fromIdExclusive: Int, untilIdInclusive: Int)(
      connection: Connection
  ): Iterable[(Int, String)]
}
