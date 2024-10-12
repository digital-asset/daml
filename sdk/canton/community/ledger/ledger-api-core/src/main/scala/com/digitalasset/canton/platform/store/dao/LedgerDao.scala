// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdResponse
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.update_service.{
  GetTransactionResponse,
  GetTransactionTreeResponse,
  GetUpdateTreesResponse,
  GetUpdatesResponse,
}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.domain.ParticipantId
import com.digitalasset.canton.ledger.api.health.ReportsHealth
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.ReportData
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.entries.PartyLedgerEntry
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.CommittedTransaction
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

private[platform] trait LedgerDaoTransactionsReader {
  def getFlatTransactions(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit loggingContext: LoggingContextWithTrace): Source[(Offset, GetUpdatesResponse), NotUsed]

  def lookupFlatTransactionById(
      updateId: UpdateId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]]

  def getTransactionTrees(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParties: Option[Set[Party]],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdateTreesResponse), NotUsed]

  def lookupTransactionTreeById(
      updateId: UpdateId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionTreeResponse]]

  def getActiveContracts(
      activeAt: Offset,
      filter: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
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

  // TODO(i16065): Re-enable getEventsByContractKey tests
//  def getEventsByContractKey(
//      contractKey: com.digitalasset.daml.lf.value.Value,
//      templateId: Ref.Identifier,
//      requestingParties: Set[Party],
//      endExclusiveSeqId: Option[Long],
//      maxIterations: Int,
//  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractKeyResponse]

}
private[platform] trait LedgerReadDao extends ReportsHealth {

  def lookupParticipantId()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ParticipantId]]

  /** Looks up the current ledger end */
  def lookupLedgerEnd()(implicit loggingContext: LoggingContextWithTrace): Future[LedgerEnd]

  def transactionsReader: LedgerDaoTransactionsReader

  def contractsReader: LedgerDaoContractsReader

  def eventsReader: LedgerDaoEventsReader

  def completions: LedgerDaoCommandCompletionsReader

  /** Returns a list of party details for the parties specified. */
  def getParties(parties: Seq[Party])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[List[IndexerPartyDetails]]

  /** Returns a list of all known parties. */
  def listKnownParties(
      fromExcl: Option[Party],
      maxResults: Int,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[List[IndexerPartyDetails]]

  def getPartyEntries(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContextWithTrace): Source[(Offset, PartyLedgerEntry), NotUsed]

  /** Prunes participant events and completions in archived history and remembers largest
    * pruning offset processed thus far.
    *
    * @param pruneUpToInclusive offset up to which to prune archived history inclusively
    * @return
    */
  def prune(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      incompletReassignmentOffsets: Vector[Offset],
  )(implicit
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
    * @param participantId the participant id to be stored
    */
  def initialize(
      participantId: ParticipantId
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

  /** This is a combined store transaction method to support sandbox-classic and tests
    * !!! Usage of this is discouraged, with the removal of sandbox-classic this will be removed
    */
  def storeTransaction(
      completionInfo: Option[state.CompletionInfo],
      workflowId: Option[WorkflowId],
      updateId: UpdateId,
      ledgerEffectiveTime: Timestamp,
      offset: Offset,
      transaction: CommittedTransaction,
      hostedWitnesses: List[Party],
      recordTime: Timestamp,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PersistenceResponse]

}

private[platform] trait LedgerDao extends LedgerReadDao with LedgerWriteDao
