// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdResponse
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.update_service.{
  GetTransactionResponse,
  GetTransactionTreeResponse,
  GetUpdateResponse,
  GetUpdateTreesResponse,
  GetUpdatesResponse,
}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.ParticipantId
import com.digitalasset.canton.ledger.api.health.ReportsHealth
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.ReportData
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.CommittedTransaction
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

private[platform] trait LedgerDaoUpdateReader {
  def getUpdates(
      startInclusive: Offset,
      endInclusive: Offset,
      internalUpdateFormat: InternalUpdateFormat,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdatesResponse), NotUsed]

  def lookupTransactionById(
      updateId: UpdateId,
      internalTransactionFormat: InternalTransactionFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]]

  def lookupTransactionByOffset(
      offset: Offset,
      internalTransactionFormat: InternalTransactionFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]]

  def lookupUpdateBy(
      lookupKey: LookupKey,
      internalUpdateFormat: InternalUpdateFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetUpdateResponse]]

  def getTransactionTrees(
      startInclusive: Offset,
      endInclusive: Offset,
      requestingParties: Option[Set[Party]],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdateTreesResponse), NotUsed]

  def lookupTransactionTreeById(
      updateId: UpdateId,
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionTreeResponse]]

  def lookupTransactionTreeByOffset(
      offset: Offset,
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionTreeResponse]]

  def getActiveContracts(
      activeAt: Option[Offset],
      filter: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit loggingContext: LoggingContextWithTrace): Source[GetActiveContractsResponse, NotUsed]
}

private[platform] trait LedgerDaoCommandCompletionsReader {
  def getCommandCompletions(
      startInclusive: Offset,
      endInclusive: Offset,
      userId: UserId,
      parties: Set[Party],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, CompletionStreamResponse), NotUsed]
}

private[platform] trait LedgerDaoEventsReader {

  def getEventsByContractId(
      contractId: ContractId,
      internalEventFormatO: Option[InternalEventFormat],
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
  def lookupLedgerEnd()(implicit loggingContext: LoggingContextWithTrace): Future[Option[LedgerEnd]]

  def updateReader: LedgerDaoUpdateReader

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

  /** Prunes participant events and completions in archived history and remembers largest pruning
    * offset processed thus far.
    *
    * @param pruneUpToInclusive
    *   offset up to which to prune archived history inclusively
    * @return
    */
  def prune(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      incompletReassignmentOffsets: Vector[Offset],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Unit]

  /** Return the pruned offsets from the parameters table (if defined) as a tuple of
    * (participant_all_divulged_contracts_pruned_up_to_inclusive,
    * participant_pruned_up_to_inclusive)
    */
  def pruningOffsets(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[(Option[Offset], Option[Offset])]

  /** Returns all TransactionMetering records matching given criteria */
  def meteringReportData(
      from: Timestamp,
      to: Option[Timestamp],
      userId: Option[UserId],
  )(implicit loggingContext: LoggingContextWithTrace): Future[ReportData]

}

// TODO(i12285) sandbox-classic clean-up: This interface and its implementation is only used in the JdbcLedgerDao suite
//                                It should be removed when the assertions in that suite are covered by other suites
private[platform] trait LedgerWriteDaoForTests extends ReportsHealth {

  /** Initializes the database with the given ledger identity. If the database was already
    * intialized, instead compares the given identity parameters to the existing ones, and returns a
    * Future failed with [[MismatchException]] if they don't match.
    *
    * This method is idempotent. This method is NOT safe to call concurrently.
    *
    * This method must succeed at least once before other LedgerWriteDao methods may be used.
    *
    * @param participantId
    *   the participant id to be stored
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

  /** Stores a party allocation or rejection thereof. */
  def storePartyAdded(
      offset: Offset,
      submissionIdOpt: Option[SubmissionId],
      recordTime: Timestamp,
      partyDetails: IndexerPartyDetails,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PersistenceResponse]

  /** This is a combined store transaction method to support only tests !!! Usage of this is
    * discouraged.
    */
  def storeTransaction(
      completionInfo: Option[state.CompletionInfo],
      workflowId: Option[WorkflowId],
      updateId: UpdateId,
      ledgerEffectiveTime: Timestamp,
      offset: Offset,
      transaction: CommittedTransaction,
      recordTime: Timestamp,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PersistenceResponse]

}
