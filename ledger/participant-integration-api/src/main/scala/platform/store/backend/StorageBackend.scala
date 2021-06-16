// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.io.InputStream
import java.sql.Connection
import java.time.Instant

import com.daml.ledger.{ApplicationId, TransactionId}
import com.daml.ledger.api.domain.{LedgerId, ParticipantId, PartyDetails}
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.ledger.participant.state.v1.{Configuration, Offset}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.ledger.EventId
import com.daml.platform
import com.daml.platform.store.DbType
import com.daml.platform.store.appendonlydao.events.{ContractId, EventsTable, Key, Raw}
import com.daml.platform.store.backend.StorageBackend.RawTransactionEvent
import com.daml.platform.store.backend.postgresql.PostgresStorageBackend
import com.daml.platform.store.entries.{ConfigurationEntry, PackageLedgerEntry, PartyLedgerEntry}
import com.daml.platform.store.interfaces.LedgerDaoContractsReader.KeyState
import com.daml.scalautil.NeverEqualsOverride

import scala.util.Try

/** Encapsulates the interface which hides database technology specific implementations.
  * Naming convention for the interface methods, which requiring Connection:
  *  - read operations are represented as nouns (plural, singular form indicates cardinality)
  *  - write operations are represented as verbs
  *
  * @tparam DB_BATCH Since parallel ingestion comes also with batching, this implementation specific type allows separation of the CPU intensive batching operation from the pure IO intensive insertBatch operation.
  */
// this is the facade
trait StorageBackend[DB_BATCH]
    extends IngestionStorageBackend[DB_BATCH]
    with ParameterStorageBackend
    with ConfigurationStorageBackend
    with PartyStorageBackend
    with PackageStorageBackend
    with DeduplicationStorageBackend
    with CompletionStorageBackend
    with ContractStorageBackend
    with EventStorageBackend {
  def reset(connection: Connection): Unit
  def enforceSynchronousCommit(connection: Connection): Unit
  def duplicateKeyError: String // TODO: Avoid brittleness of error message checks
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

  /** Custom initialization code before the start of an ingestion.
    * This method is responsible for the recovery after a possibly non-graceful stop of previous indexing.
    * No significant CPU load, mostly blocking JDBC communication with the database backend.
    *
    * @param connection to be used when initializing
    * @return the LedgerEnd, which should be the basis for further indexing.
    */
  def initialize(connection: Connection): StorageBackend.LedgerEnd
}

// TODO append-only: consolidate parameters table facing functions
trait ParameterStorageBackend {

  /** This method is used to update the parameters table: setting the new observable ledger-end, and other parameters.
    * No significant CPU load, mostly blocking JDBC communication with the database backend.
    *
    * @param connection to be used when updating the parameters table
    * @param params     the parameters
    */
  def updateParams(params: StorageBackend.Params)(connection: Connection): Unit

  /** Query the ledgerEnd, read from the parameters table.
    * No significant CPU load, mostly blocking JDBC communication with the database backend.
    *
    * @param connection to be used to get the LedgerEnd
    * @return the LedgerEnd, which should be the basis for further indexing
    */
  def ledgerEnd(connection: Connection): StorageBackend.LedgerEnd
  def ledgerId(connection: Connection): Option[LedgerId]
  def updateLedgerId(ledgerId: String)(connection: Connection): Unit
  def participantId(connection: Connection): Option[ParticipantId]
  def updateParticipantId(participantId: String)(connection: Connection): Unit
  def ledgerEndOffset(connection: Connection): Offset
  def ledgerEndOffsetAndSequentialId(connection: Connection): (Offset, Long)
  def initialLedgerEnd(connection: Connection): Option[Offset]

  /** Part of pruning process, this needs to be in the same transaction as the other pruning related database operations
    */
  def updatePrunedUptoInclusive(prunedUpToInclusive: Offset)(connection: Connection): Unit
  def prunedUptoInclusive(connection: Connection): Option[Offset]
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
  def lfPackages(connection: Connection): Map[PackageId, PackageDetails]
  def lfArchive(packageId: PackageId)(connection: Connection): Option[Array[Byte]]
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
  )(connection: Connection): Int
  def removeExpiredDeduplicationData(currentTime: Instant)(connection: Connection): Unit
  def stopDeduplicatingCommand(deduplicationKey: String)(connection: Connection): Unit
}

trait CompletionStorageBackend {
  def commandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(connection: Connection): List[CompletionStreamResponse]

  /** Part of pruning process, this needs to be in the same transaction as the other pruning related database operations
    */
  def pruneCompletions(pruneUpToInclusive: Offset)(connection: Connection): Unit
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

// TODO append-only: Event related query consolidation
trait EventStorageBackend {

  /** Part of pruning process, this needs to be in the same transaction as the other pruning related database operations
    */
  def pruneEvents(pruneUpToInclusive: Offset)(connection: Connection): Unit
  def transactionsEventsSingleWildcardParty(
      startExclusive: Long,
      endInclusive: Long,
      party: Ref.Party,
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def transactionsEventsSinglePartyWithTemplates(
      startExclusive: Long,
      endInclusive: Long,
      party: Ref.Party,
      templateIds: Set[Ref.Identifier],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def transactionsEventsOnlyWildcardParties(
      startExclusive: Long,
      endInclusive: Long,
      parties: Set[Ref.Party],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def transactionsEventsSameTemplates(
      startExclusive: Long,
      endInclusive: Long,
      parties: Set[Ref.Party],
      templateIds: Set[Ref.Identifier],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def transactionsEventsMixedTemplates(
      startExclusive: Long,
      endInclusive: Long,
      partiesAndTemplateIds: Set[(Ref.Party, Ref.Identifier)],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def transactionsEventsMixedTemplatesWithWildcardParties(
      startExclusive: Long,
      endInclusive: Long,
      partiesAndTemplateIds: Set[(Ref.Party, Ref.Identifier)],
      wildcardParties: Set[Ref.Party],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def activeContractsEventsSingleWildcardParty(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      party: Ref.Party,
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def activeContractsEventsSinglePartyWithTemplates(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      party: Ref.Party,
      templateIds: Set[Ref.Identifier],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def activeContractsEventsOnlyWildcardParties(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      parties: Set[Ref.Party],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def activeContractsEventsSameTemplates(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      parties: Set[Ref.Party],
      templateIds: Set[Ref.Identifier],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def activeContractsEventsMixedTemplates(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      partiesAndTemplateIds: Set[(Ref.Party, Ref.Identifier)],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def activeContractsEventsMixedTemplatesWithWildcardParties(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      partiesAndTemplateIds: Set[(Ref.Party, Ref.Identifier)],
      wildcardParties: Set[Ref.Party],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def flatTransactionSingleParty(
      transactionId: TransactionId,
      requestingParty: Ref.Party,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def flatTransactionMultiParty(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]]
  def transactionTreeSingleParty(
      transactionId: TransactionId,
      requestingParty: Ref.Party,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]]
  def transactionTreeMultiParty(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]]
  def transactionTreeEventsSingleParty(
      startExclusive: Long,
      endInclusive: Long,
      requestingParty: Ref.Party,
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]]
  def transactionTreeEventsMultiParty(
      startExclusive: Long,
      endInclusive: Long,
      requestingParties: Set[Ref.Party],
      limit: Option[Int],
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]]
  def maxEventSeqIdForOffset(offset: Offset)(connection: Connection): Option[Long]
  def rawEvents(startExclusive: Long, endInclusive: Long)(
      connection: Connection
  ): Vector[RawTransactionEvent]
}

object StorageBackend {
  case class Params(ledgerEnd: Offset, eventSeqId: Long)

  case class LedgerEnd(lastOffset: Option[Offset], lastEventSeqId: Option[Long])

  case class RawContractState(
      templateId: Option[String],
      flatEventWitnesses: Set[Ref.Party],
      createArgument: Option[InputStream],
      createArgumentCompression: Option[Int],
      eventKind: Int,
      ledgerEffectiveTime: Option[Instant],
  )

  case class RawContract(
      templateId: String,
      createArgument: InputStream,
      createArgumentCompression: Option[Int],
  )

  case class RawContractStateEvent(
      eventKind: Int,
      contractId: ContractId,
      templateId: Option[Ref.Identifier],
      ledgerEffectiveTime: Option[Instant],
      createKeyValue: Option[InputStream],
      createKeyCompression: Option[Int],
      createArgument: Option[InputStream],
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
      createKeyValue: Option[InputStream],
      createKeyCompression: Option[Int],
      createArgument: Option[InputStream],
      createArgumentCompression: Option[Int],
      treeEventWitnesses: Set[String],
      flatEventWitnesses: Set[String],
      submitters: Set[String],
      exerciseChoice: Option[String],
      exerciseArgument: Option[InputStream],
      exerciseArgumentCompression: Option[Int],
      exerciseResult: Option[InputStream],
      exerciseResultCompression: Option[Int],
      exerciseActors: Option[Array[String]],
      exerciseChildEventIds: Option[Array[String]],
      eventSequentialId: Long,
      offset: Offset,
  ) extends NeverEqualsOverride

  def of(dbType: DbType): StorageBackend[_] =
    dbType match {
      case DbType.H2Database => throw new UnsupportedOperationException("H2 not supported yet")
      case DbType.Postgres => PostgresStorageBackend
      case DbType.Oracle => throw new UnsupportedOperationException("Oracle not supported yet")
    }
}
