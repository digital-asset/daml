// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdResponse
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.update_service.{GetUpdateResponse, GetUpdatesResponse}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.ParticipantId
import com.digitalasset.canton.ledger.api.health.ReportsHealth
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
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

  def lookupUpdateBy(
      lookupKey: LookupKey,
      internalUpdateFormat: InternalUpdateFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetUpdateResponse]]

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
      previousPruneUpToInclusive: Option[Offset],
      previousIncompleteReassignmentOffsets: Vector[Offset],
      pruneUpToInclusive: Offset,
      incompleteReassignmentOffsets: Vector[Offset],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Unit]

  def indexDbPrunedUpTo(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[Offset]]

  /** Return the latest pruned offset inclusive (participant_pruned_up_to_inclusive) from the
    * parameters table (if defined)
    */
  def pruningOffset(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[Offset]]
}
