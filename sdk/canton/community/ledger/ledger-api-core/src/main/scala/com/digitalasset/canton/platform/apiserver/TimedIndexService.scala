// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.daml_lf_dev.DamlLf
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
import com.daml.lf.data.Ref.{ApplicationId, Party}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.metrics.Timed
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.{ParticipantOffset, TransactionId}
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.index.v2
import com.digitalasset.canton.ledger.participant.state.index.v2.MeteringStore.ReportData
import com.digitalasset.canton.ledger.participant.state.index.v2.*
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.Metrics
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

final class TimedIndexService(delegate: IndexService, metrics: Metrics) extends IndexService {

  override def listLfPackages()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Map[Ref.PackageId, v2.PackageDetails]] =
    Timed.future(metrics.services.index.listLfPackages, delegate.listLfPackages())

  override def getLfArchive(packageId: Ref.PackageId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[DamlLf.Archive]] =
    Timed.future(metrics.services.index.getLfArchive, delegate.getLfArchive(packageId))

  override def packageEntries(
      startExclusive: Option[ParticipantOffset.Absolute]
  )(implicit loggingContext: LoggingContextWithTrace): Source[domain.PackageEntry, NotUsed] =
    Timed.source(
      metrics.services.index.packageEntries,
      delegate.packageEntries(startExclusive),
    )

  override def currentLedgerEnd(): Future[ParticipantOffset.Absolute] =
    Timed.future(metrics.services.index.currentLedgerEnd, delegate.currentLedgerEnd())

  override def getCompletions(
      begin: domain.ParticipantOffset,
      applicationId: Ref.ApplicationId,
      parties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Source[CompletionStreamResponse, NotUsed] =
    Timed.source(
      metrics.services.index.getCompletions,
      delegate.getCompletions(begin, applicationId, parties),
    )

  override def transactions(
      begin: domain.ParticipantOffset,
      endAt: Option[domain.ParticipantOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContextWithTrace): Source[GetUpdatesResponse, NotUsed] =
    Timed.source(
      metrics.services.index.transactions,
      delegate.transactions(begin, endAt, filter, verbose),
    )

  override def transactionTrees(
      begin: domain.ParticipantOffset,
      endAt: Option[domain.ParticipantOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContextWithTrace): Source[GetUpdateTreesResponse, NotUsed] =
    Timed.source(
      metrics.services.index.transactionTrees,
      delegate.transactionTrees(begin, endAt, filter, verbose),
    )

  override def getTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]] =
    Timed.future(
      metrics.services.index.getTransactionById,
      delegate.getTransactionById(transactionId, requestingParties),
    )

  override def getTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionTreeResponse]] =
    Timed.future(
      metrics.services.index.getTransactionTreeById,
      delegate.getTransactionTreeById(transactionId, requestingParties),
    )

  override def getActiveContracts(
      filter: domain.TransactionFilter,
      verbose: Boolean,
      activeAtO: Option[Offset],
  )(implicit loggingContext: LoggingContextWithTrace): Source[GetActiveContractsResponse, NotUsed] =
    Timed.source(
      metrics.services.index.getActiveContracts,
      delegate.getActiveContracts(filter, verbose, activeAtO),
    )

  override def lookupActiveContract(
      readers: Set[Ref.Party],
      contractId: Value.ContractId,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[Value.VersionedContractInstance]] =
    Timed.future(
      metrics.services.index.lookupActiveContract,
      delegate.lookupActiveContract(readers, contractId),
    )

  override def lookupContractKey(
      readers: Set[Ref.Party],
      key: GlobalKey,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[Value.ContractId]] =
    Timed.future(
      metrics.services.index.lookupContractKey,
      delegate.lookupContractKey(readers, key),
    )

  override def lookupMaximumLedgerTimeAfterInterpretation(
      ids: Set[Value.ContractId]
  )(implicit loggingContext: LoggingContextWithTrace): Future[MaximumLedgerTime] =
    Timed.future(
      metrics.services.index.lookupMaximumLedgerTime,
      delegate.lookupMaximumLedgerTimeAfterInterpretation(ids),
    )

  override def getParticipantId(): Future[Ref.ParticipantId] =
    Timed.future(metrics.services.index.getParticipantId, delegate.getParticipantId())

  override def getParties(parties: Seq[Ref.Party])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[List[IndexerPartyDetails]] =
    Timed.future(metrics.services.index.getParties, delegate.getParties(parties))

  override def listKnownParties()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[List[IndexerPartyDetails]] =
    Timed.future(metrics.services.index.listKnownParties, delegate.listKnownParties())

  override def partyEntries(
      startExclusive: Option[ParticipantOffset.Absolute]
  )(implicit loggingContext: LoggingContextWithTrace): Source[PartyEntry, NotUsed] =
    Timed.source(metrics.services.index.partyEntries, delegate.partyEntries(startExclusive))

  override def prune(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      incompletReassignmentOffsets: Vector[Offset],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Unit] =
    Timed.future(
      metrics.services.index.prune,
      delegate.prune(pruneUpToInclusive, pruneAllDivulgedContracts, incompletReassignmentOffsets),
    )

  override def getCompletions(
      startExclusive: ParticipantOffset,
      endInclusive: ParticipantOffset,
      applicationId: Ref.ApplicationId,
      parties: Set[Party],
  )(implicit loggingContext: LoggingContextWithTrace): Source[CompletionStreamResponse, NotUsed] =
    Timed.source(
      metrics.services.index.getCompletionsLimited,
      delegate.getCompletions(startExclusive, endInclusive, applicationId, parties),
    )

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()

  override def getMeteringReportData(
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[ApplicationId],
  )(implicit loggingContext: LoggingContextWithTrace): Future[ReportData] = {
    Timed.future(
      metrics.services.index.getTransactionMetering,
      delegate.getMeteringReportData(from, to, applicationId),
    )
  }

  override def lookupContractState(contractId: Value.ContractId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[ContractState] =
    Timed.future(
      metrics.services.index.lookupContractState,
      delegate.lookupContractState(contractId),
    )

  override def latestPrunedOffsets()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[(ParticipantOffset.Absolute, ParticipantOffset.Absolute)] =
    Timed.future(metrics.services.index.latestPrunedOffsets, delegate.latestPrunedOffsets())

  override def getEventsByContractId(
      contractId: ContractId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractIdResponse] =
    Timed.future(
      metrics.services.index.getEventsByContractId,
      delegate.getEventsByContractId(contractId, requestingParties),
    )

  // TODO(i16065): Re-enable getEventsByContractKey tests
//  override def getEventsByContractKey(
//      contractKey: Value,
//      templateId: Ref.Identifier,
//      requestingParties: Set[Ref.Party],
//      endExclusiveSeqId: Option[Long],
//  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractKeyResponse] =
//    Timed.future(
//      metrics.services.index.getEventsByContractKey,
//      delegate.getEventsByContractKey(
//        contractKey,
//        templateId,
//        requestingParties,
//        endExclusiveSeqId,
//      ),
//    )
}
