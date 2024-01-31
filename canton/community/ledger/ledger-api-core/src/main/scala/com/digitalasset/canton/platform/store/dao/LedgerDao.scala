// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.v1.event_query_service.GetEventsByContractKeyResponse
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdResponse
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.update_service.{
  GetTransactionResponse,
  GetTransactionTreeResponse,
  GetUpdateTreesResponse,
  GetUpdatesResponse,
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.{BlindingInfo, CommittedTransaction}
import com.digitalasset.canton.ledger.api.domain.{LedgerId, ParticipantId}
import com.digitalasset.canton.ledger.api.health.ReportsHealth
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.index.v2.MeteringStore.ReportData
import com.digitalasset.canton.ledger.participant.state.index.v2.{
  IndexerPartyDetails,
  PackageDetails,
}
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.entries.{
  ConfigurationEntry,
  PackageLedgerEntry,
  PartyLedgerEntry,
}
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

private[platform] trait LedgerDaoTransactionsReader {
  def getFlatTransactions(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
      multiDomainEnabled: Boolean,
  )(implicit loggingContext: LoggingContextWithTrace): Source[(Offset, GetUpdatesResponse), NotUsed]

  def lookupFlatTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]]

  def getTransactionTrees(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
      multiDomainEnabled: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdateTreesResponse), NotUsed]

  def lookupTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionTreeResponse]]

  def getActiveContracts(
      activeAt: Offset,
      filter: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
      multiDomainEnabled: Boolean,
  )(implicit loggingContext: LoggingContextWithTrace): Source[GetActiveContractsResponse, NotUsed]
}

private[platform] trait LedgerDaoCommandCompletionsReader {
  def getCommandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      parties: Set[Party],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, CompletionStreamResponse), NotUsed]
}

private[platform] trait LedgerDaoEventsReader {

  def getEventsByContractId(
      contractId: ContractId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractIdResponse]

  def getEventsByContractKey(
      contractKey: com.daml.lf.value.Value,
      templateId: Ref.Identifier,
      requestingParties: Set[Party],
      endExclusiveSeqId: Option[Long],
      maxIterations: Int,
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractKeyResponse]

}
private[platform] trait LedgerReadDao extends ReportsHealth {

  def lookupParticipantId()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ParticipantId]]

  /** Looks up the current ledger end */
  def lookupLedgerEnd()(implicit loggingContext: LoggingContextWithTrace): Future[LedgerEnd]

  /** Looks up the current ledger configuration, if it has been set. */
  def lookupLedgerConfiguration()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[(Offset, Configuration)]]

  /** Returns a stream of configuration entries. */
  def getConfigurationEntries(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContextWithTrace): Source[(Offset, ConfigurationEntry), NotUsed]

  def transactionsReader: LedgerDaoTransactionsReader

  def contractsReader: LedgerDaoContractsReader

  def eventsReader: LedgerDaoEventsReader

  def completions: LedgerDaoCommandCompletionsReader

  /** Returns a list of party details for the parties specified. */
  def getParties(parties: Seq[Party])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[List[IndexerPartyDetails]]

  /** Returns a list of all known parties. */
  def listKnownParties()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[List[IndexerPartyDetails]]

  def getPartyEntries(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContextWithTrace): Source[(Offset, PartyLedgerEntry), NotUsed]

  /** Returns a list of all known Daml-LF packages */
  def listLfPackages()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Map[PackageId, PackageDetails]]

  /** Returns the given Daml-LF archive */
  def getLfArchive(packageId: PackageId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[Archive]]

  /** Returns a stream of package upload entries.
    * @return a stream of package entries tupled with their offset
    */
  def getPackageEntries(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContextWithTrace): Source[(Offset, PackageLedgerEntry), NotUsed]

  /** Prunes participant events and completions in archived history and remembers largest
    * pruning offset processed thus far.
    *
    * @param pruneUpToInclusive offset up to which to prune archived history inclusively
    * @return
    */
  def prune(pruneUpToInclusive: Offset, pruneAllDivulgedContracts: Boolean)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Unit]

  /** Return the pruned offsets from the parameters table (if defined)
    * as a tuple of (participant_all_divulged_contracts_pruned_up_to_inclusive, participant_pruned_up_to_inclusive)
    */
  def pruningOffsets(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[(Option[Offset], Option[Offset])]

  /** Returns all TransactionMetering records matching given criteria */
  def meteringReportData(
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[ApplicationId],
  )(implicit loggingContext: LoggingContextWithTrace): Future[ReportData]

}

// TODO(i12285) sandbox-classic clean-up: This interface and its implementation is only used in the JdbcLedgerDao suite
//                                It should be removed when the assertions in that suite are covered by other suites
private[platform] trait LedgerWriteDao extends ReportsHealth {

  /** Initializes the database with the given ledger identity.
    * If the database was already intialized, instead compares the given identity parameters
    * to the existing ones, and returns a Future failed with [[MismatchException]]
    * if they don't match.
    *
    * This method is idempotent.
    * This method is NOT safe to call concurrently.
    *
    * This method must succeed at least once before other LedgerWriteDao methods may be used.
    *
    * @param ledgerId the ledger id to be stored
    * @param participantId the participant id to be stored
    */
  def initialize(
      ledgerId: LedgerId,
      participantId: ParticipantId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Unit]

  def storeRejection(
      completionInfo: Option[state.CompletionInfo],
      recordTime: Timestamp,
      offset: Offset,
      reason: state.Update.CommandRejected.RejectionReasonTemplate,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PersistenceResponse]

  /** Stores a party allocation or rejection thereof.
    *
    * @param offset  Pair of previous offset and the offset to store the party entry at
    * @param partyEntry  the PartyEntry to be stored
    * @return Ok when the operation was successful otherwise a Duplicate
    */
  def storePartyEntry(offset: Offset, partyEntry: PartyLedgerEntry)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PersistenceResponse]

  /** Store a configuration change or rejection.
    */
  def storeConfigurationEntry(
      offset: Offset,
      recordedAt: Timestamp,
      submissionId: String,
      configuration: Configuration,
      rejectionReason: Option[String],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PersistenceResponse]

  /** Store a Daml-LF package upload result.
    */
  def storePackageEntry(
      offset: Offset,
      packages: List[(Archive, PackageDetails)],
      optEntry: Option[PackageLedgerEntry],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PersistenceResponse]

  /** This is a combined store transaction method to support sandbox-classic and tests
    * !!! Usage of this is discouraged, with the removal of sandbox-classic this will be removed
    */
  def storeTransaction(
      completionInfo: Option[state.CompletionInfo],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      ledgerEffectiveTime: Timestamp,
      offset: Offset,
      transaction: CommittedTransaction,
      blindingInfo: Option[BlindingInfo],
      hostedWitnesses: List[Party],
      recordTime: Timestamp,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PersistenceResponse]

}

private[platform] trait LedgerDao extends LedgerReadDao with LedgerWriteDao
