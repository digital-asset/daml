package com.daml.platform.sandbox.stores.ledger

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.TransactionId
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{ApplicationId, CommandId, LedgerId}
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.transaction_service.{GetFlatTransactionResponse, GetTransactionResponse, GetTransactionTreesResponse, GetTransactionsResponse}
import com.daml.ledger.participant.state.index.v2.{CommandDeduplicationDuplicate, CommandDeduplicationResult, PackageDetails}
import com.daml.ledger.participant.state.v1.{Configuration, Offset, SubmissionId, SubmissionResult, SubmittedTransaction, SubmitterInfo, TransactionMeta}
import com.daml.lf.data.Ref.{PackageId, Party}
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.language.Ast
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.platform.store.entries.{ConfigurationEntry, PackageLedgerEntry, PartyLedgerEntry}

import scala.concurrent.Future

private class NoOpLedger extends Ledger {
  override def publishTransaction(submitterInfo: SubmitterInfo, transactionMeta: TransactionMeta, transaction: SubmittedTransaction)(implicit loggingContext: LoggingContext): Future[SubmissionResult] = Future.successful(SubmissionResult.NotSupported)

  override def publishPartyAllocation(submissionId: SubmissionId, party: Party, displayName: Option[String])(implicit loggingContext: LoggingContext): Future[SubmissionResult] = Future.successful(SubmissionResult.NotSupported)

  override def uploadPackages(submissionId: SubmissionId, knownSince: Instant, sourceDescription: Option[String], payload: List[DamlLf.Archive])(implicit loggingContext: LoggingContext): Future[SubmissionResult] = Future.successful(SubmissionResult.NotSupported)

  override def publishConfiguration(maxRecordTime: Time.Timestamp, submissionId: String, config: Configuration)(implicit loggingContext: LoggingContext): Future[SubmissionResult] = Future.successful(SubmissionResult.NotSupported)

  override def ledgerId: LedgerId = LedgerId("")

  override def flatTransactions(startExclusive: Option[Offset], endInclusive: Option[Offset], filter: Map[Party, Set[Ref.Identifier]], verbose: Boolean)(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] = Source.empty

  override def transactionTrees(startExclusive: Option[Offset], endInclusive: Option[Offset], requestingParties: Set[Party], verbose: Boolean)(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionTreesResponse), NotUsed] = Source.empty

  override def ledgerEnd()(implicit loggingContext: LoggingContext): Offset = Offset.beforeBegin

  override def completions(startExclusive: Option[Offset], endInclusive: Option[Offset], applicationId: ApplicationId, parties: Set[Party])(implicit loggingContext: LoggingContext): Source[(Offset, CompletionStreamResponse), NotUsed] = Source.empty

  override def activeContracts(filter: Map[Party, Set[Ref.Identifier]], verbose: Boolean)(implicit loggingContext: LoggingContext): (Source[GetActiveContractsResponse, NotUsed], Offset) = (Source.empty, Offset.beforeBegin)

  override def lookupContract(contractId: Value.ContractId, forParties: Set[Party])(implicit loggingContext: LoggingContext): Future[Option[Value.ContractInst[Value.VersionedValue[Value.ContractId]]]] = Future.successful(None)

  override def lookupMaximumLedgerTime(contractIds: Set[Value.ContractId])(implicit loggingContext: LoggingContext): Future[Option[Instant]] = Future.successful(None)

  override def lookupKey(key: GlobalKey, forParties: Set[Party])(implicit loggingContext: LoggingContext): Future[Option[Value.ContractId]] = Future.successful(None)

  override def lookupFlatTransactionById(transactionId: TransactionId, requestingParties: Set[Party])(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]] = Future.successful(None)

  override def lookupTransactionTreeById(transactionId: TransactionId, requestingParties: Set[Party])(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]] = Future.successful(None)

  override def getParties(parties: Seq[Party])(implicit loggingContext: LoggingContext): Future[List[domain.PartyDetails]] = Future.successful(List.empty)

  override def listKnownParties()(implicit loggingContext: LoggingContext): Future[List[domain.PartyDetails]] = Future.successful(List.empty)

  override def partyEntries(startExclusive: Offset)(implicit loggingContext: LoggingContext): Source[(Offset, PartyLedgerEntry), NotUsed] = Source.empty

  override def listLfPackages()(implicit loggingContext: LoggingContext): Future[Map[PackageId, PackageDetails]] = Future.successful(Map.empty)

  override def getLfArchive(packageId: PackageId)(implicit loggingContext: LoggingContext): Future[Option[DamlLf.Archive]] = Future.successful(None)

  override def getLfPackage(packageId: PackageId)(implicit loggingContext: LoggingContext): Future[Option[Ast.Package]] = Future.successful(None)

  override def packageEntries(startExclusive: Offset)(implicit loggingContext: LoggingContext): Source[(Offset, PackageLedgerEntry), NotUsed] = Source.empty

  override def lookupLedgerConfiguration()(implicit loggingContext: LoggingContext): Future[Option[(Offset, Configuration)]] = Future.successful(None)

  override def configurationEntries(startExclusive: Offset)(implicit loggingContext: LoggingContext): Source[(Offset, ConfigurationEntry), NotUsed] = Source.empty

  /** Deduplicates commands.
   * Returns CommandDeduplicationNew if this is the first time the command is submitted
   * Returns CommandDeduplicationDuplicate if the command was submitted before
   *
   * Note: The deduplication cache is used by the submission service,
   * it does not modify any on-ledger data.
   */
  override def deduplicateCommand(commandId: CommandId, submitters: List[Party], submittedAt: Instant, deduplicateUntil: Instant)(implicit loggingContext: LoggingContext): Future[CommandDeduplicationResult] = Future.successful(CommandDeduplicationDuplicate(Instant.EPOCH))

  /** Stops deduplicating the given command.
   *
   * Note: The deduplication cache is used by the submission service,
   * it does not modify any on-ledger data.
   */
  override def stopDeduplicatingCommand(commandId: CommandId, submitters: List[Party])(implicit loggingContext: LoggingContext): Future[Unit] = Future.unit

  /** Remove all expired deduplication entries. This method has to be called
   * periodically to ensure that the deduplication cache does not grow unboundedly.
   *
   * @param currentTime The current time. This should use the same source of time as
   *                    the `deduplicateUntil` argument of [[deduplicateCommand]].
   * @return when DAO has finished removing expired entries. Clients do not
   *         need to wait for the operation to finish, it is safe to concurrently
   *         call deduplicateCommand().
   *
   *         Note: The deduplication cache is used by the submission service,
   *         it does not modify any on-ledger data.
   */
  override def removeExpiredDeduplicationData(currentTime: Instant)(implicit loggingContext: LoggingContext): Future[Unit] = Future.unit

  /** Performs participant ledger pruning up to and including the specified offset.
   */
  override def prune(pruneUpToInclusive: Offset)(implicit loggingContext: LoggingContext): Future[Unit] = Future.unit

  /** Reports the current health of the object. This should always return immediately.
   */
  override def currentHealth(): HealthStatus = HealthStatus.unhealthy

  override def close(): Unit = {
  }
}
