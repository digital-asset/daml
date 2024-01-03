// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.inspection

import cats.Eval
import cats.data.{EitherT, OptionT}
import cats.implicits.*
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.{
  ActiveContract,
  SerializableContractWithDomainId,
}
import com.digitalasset.canton.participant.admin.inspection.Error.{
  InvariantIssue,
  SerializationIssue,
}
import com.digitalasset.canton.participant.protocol.RequestJournal
import com.digitalasset.canton.participant.store.ActiveContractStore.AcsError
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.{
  LedgerSyncEvent,
  SyncDomainPersistentStateManager,
  TimestampedEvent,
  UpstreamOffsetConvert,
}
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  SignedProtocolMessage,
}
import com.digitalasset.canton.protocol.{LfCommittedTransaction, LfContractId, SerializableContract}
import com.digitalasset.canton.sequencing.PossiblyIgnoredProtocolEvent
import com.digitalasset.canton.sequencing.handlers.EnvelopeOpener
import com.digitalasset.canton.store.CursorPrehead.{
  RequestCounterCursorPrehead,
  SequencerCounterCursorPrehead,
}
import com.digitalasset.canton.store.SequencedEventStore.{
  ByTimestampRange,
  PossiblyIgnoredSequencedEvent,
}
import com.digitalasset.canton.store.{
  SequencedEventNotFoundError,
  SequencedEventRangeOverlapsWithPruning,
  SequencedEventStore,
}
import com.digitalasset.canton.topology.{DomainId, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  DomainAlias,
  LedgerTransactionId,
  LfPartyId,
  RequestCounter,
  TransferCounterO,
}

import java.io.OutputStream
import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

trait JournalGarbageCollectorControl {
  def disable(domainId: DomainId)(implicit traceContext: TraceContext): Future[Unit]
  def enable(domainId: DomainId)(implicit traceContext: TraceContext): Unit

}

object JournalGarbageCollectorControl {
  object NoOp extends JournalGarbageCollectorControl {
    override def disable(domainId: DomainId)(implicit traceContext: TraceContext): Future[Unit] =
      Future.unit
    override def enable(domainId: DomainId)(implicit traceContext: TraceContext): Unit = ()
  }
}

/** Implements inspection functions for the sync state of a participant node */
final class SyncStateInspection(
    syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
    participantNodePersistentState: Eval[ParticipantNodePersistentState],
    timeouts: ProcessingTimeout,
    journalCleaningControl: JournalGarbageCollectorControl,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  import SyncStateInspection.getOrFail

  /** For a set of contracts lookup which domain they are currently in.
    * If a contract is not found in a available ACS it will be omitted from the response.
    */
  def lookupContractDomain(
      contractIds: Set[LfContractId]
  )(implicit traceContext: TraceContext): Future[Map[LfContractId, DomainAlias]] = {
    def lookupAlias(domainId: DomainId): DomainAlias =
      // am assuming that an alias can't be missing once registered
      syncDomainPersistentStateManager
        .aliasForDomainId(domainId)
        .getOrElse(sys.error(s"missing alias for domain [$domainId]"))

    syncDomainPersistentStateManager.getAll.toList
      .map { case (id, state) => lookupAlias(id) -> state }
      .parTraverse { case (alias, state) =>
        OptionT(state.requestJournalStore.preheadClean)
          .semiflatMap(cleanRequest =>
            state.activeContractStore
              .contractSnapshot(contractIds, cleanRequest.timestamp)
              .map(_.keySet.map(_ -> alias))
          )
          .getOrElse(List.empty[(LfContractId, DomainAlias)])
      }
      .map(_.flatten.toMap)
  }

  def lookupTransactionDomain(transactionId: LedgerTransactionId)(implicit
      traceContext: TraceContext
  ): Future[Option[DomainId]] =
    participantNodePersistentState.value.multiDomainEventLog
      .lookupTransactionDomain(transactionId)
      .value

  /** returns the potentially big ACS of a given domain */
  def findAcs(
      domainAlias: DomainAlias
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsError, Map[LfContractId, (CantonTimestamp, TransferCounterO)]] =
    OptionT(
      syncDomainPersistentStateManager
        .getByAlias(domainAlias)
        .map(AcsInspection.getCurrentSnapshot)
        .sequence
    ).widen[Map[LfContractId, (CantonTimestamp, TransferCounterO)]]
      .toRight(SyncStateInspection.NoSuchDomain(domainAlias))

  /** searches the pcs and returns the contract and activeness flag */
  def findContracts(
      domain: DomainAlias,
      filterId: Option[String],
      filterPackage: Option[String],
      filterTemplate: Option[String],
      limit: Int,
  )(implicit traceContext: TraceContext): List[(Boolean, SerializableContract)] =
    getOrFail(
      timeouts.inspection.await("findContracts") {
        syncDomainPersistentStateManager
          .getByAlias(domain)
          .map(AcsInspection.findContracts(_, filterId, filterPackage, filterTemplate, limit))
          .sequence
      },
      domain,
    )

  private def disableJournalCleaningForFilter(
      domains: Map[DomainId, SyncDomainPersistentState],
      filterDomain: DomainId => Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, Error, Unit] = {
    val disabledCleaningF = Future
      .sequence(domains.collect {
        case (domainId, _) if filterDomain(domainId) =>
          journalCleaningControl.disable(domainId)
      })
      .map(_ => ())
    EitherT.right(disabledCleaningF)
  }

  // TODO(i14441): Remove deprecated ACS download / upload functionality
  @deprecated("Use exportAcsDumpActiveContracts", since = "2.8.0")
  def dumpActiveContracts(
      outputStream: OutputStream,
      filterDomain: DomainId => Boolean,
      parties: Set[LfPartyId],
      timestamp: Option[CantonTimestamp],
      protocolVersion: Option[ProtocolVersion],
      contractDomainRenames: Map[DomainId, DomainId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, Error, Unit] = {
    val allDomains = syncDomainPersistentStateManager.getAll
    // disable journal cleaning for the duration of the dump
    disableJournalCleaningForFilter(allDomains, filterDomain).flatMap { _ =>
      MonadUtil.sequentialTraverse_(allDomains) {
        case (domainId, state) if filterDomain(domainId) =>
          val domainIdForExport = contractDomainRenames.getOrElse(domainId, domainId)
          val useProtocolVersion = protocolVersion.getOrElse(state.protocolVersion)
          val ret = for {
            _ <- AcsInspection
              .forEachVisibleActiveContract(domainId, state, parties, timestamp) {
                case (contract, _) =>
                  val domainToContract =
                    SerializableContractWithDomainId(domainIdForExport, contract)
                  val encodedContract = domainToContract.encode(useProtocolVersion)
                  outputStream.write(encodedContract.getBytes)
                  Right(outputStream.flush())
              }
          } yield ()
          // re-enable journal cleaning after the dump
          ret.thereafter { _ =>
            journalCleaningControl.enable(domainId)
          }
        case _ =>
          EitherTUtil.unit
      }
    }
  }

  def allProtocolVersions: Map[DomainId, ProtocolVersion] =
    syncDomainPersistentStateManager.getAll.view.mapValues(_.protocolVersion).toMap

  def exportAcsDumpActiveContracts(
      outputStream: OutputStream,
      filterDomain: DomainId => Boolean,
      parties: Set[LfPartyId],
      timestamp: Option[CantonTimestamp],
      contractDomainRenames: Map[DomainId, (DomainId, ProtocolVersion)],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, Error, Unit] = {
    val allDomains = syncDomainPersistentStateManager.getAll
    // disable journal cleaning for the duration of the dump
    disableJournalCleaningForFilter(allDomains, filterDomain).flatMap { _ =>
      MonadUtil.sequentialTraverse_(allDomains) {
        case (domainId, state) if filterDomain(domainId) =>
          val (domainIdForExport, protocolVersion) =
            contractDomainRenames.getOrElse(domainId, (domainId, state.protocolVersion))

          val ret = for {
            _ <- AcsInspection
              .forEachVisibleActiveContract(domainId, state, parties, timestamp) {
                case (contract, transferCounter) =>
                  val activeContractE =
                    ActiveContract.create(domainIdForExport, contract, transferCounter)(
                      protocolVersion
                    )

                  activeContractE match {
                    case Left(e) =>
                      Left(InvariantIssue(domainId, contract.contractId, e.getMessage))
                    case Right(bundle) =>
                      bundle.writeDelimitedTo(outputStream) match {
                        case Left(errorMessage) =>
                          Left(SerializationIssue(domainId, contract.contractId, errorMessage))
                        case Right(_) =>
                          outputStream.flush()
                          Right(())
                      }
                  }

              }
          } yield ()
          // re-enable journal cleaning after the dump
          ret.thereafter { _ =>
            journalCleaningControl.enable(domainId)
          }
        case _ =>
          EitherTUtil.unit
      }
    }
  }

  def contractCount(domain: DomainAlias)(implicit traceContext: TraceContext): Future[Int] = {
    val state = syncDomainPersistentStateManager
      .getByAlias(domain)
      .getOrElse(throw new IllegalArgumentException(s"Unable to find contract store for $domain."))
    state.contractStore.contractCount()
  }

  def contractCountInAcs(domain: DomainAlias, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Option[Int]] = {
    getPersistentState(domain) match {
      case None => Future.successful(None)
      case Some(state) => state.activeContractStore.contractCount(timestamp).map(Some(_))
    }
  }

  def requestJournalSize(
      domain: DomainAlias,
      start: CantonTimestamp = CantonTimestamp.Epoch,
      end: Option[CantonTimestamp] = None,
  )(implicit traceContext: TraceContext): Option[Int] = {
    getPersistentState(domain).map { state =>
      timeouts.inspection.await(
        s"$functionFullName from $start to $end from the journal of domain $domain"
      )(
        state.requestJournalStore.size(start, end)
      )
    }
  }

  def partyHasActiveContracts(partyId: PartyId)(implicit
      traceContext: TraceContext
  ): Future[Boolean] =
    syncDomainPersistentStateManager.getAll.toList
      .findM { case (_, store) => AcsInspection.hasActiveContracts(store, partyId) }
      .map(_.nonEmpty)

  def findAcceptedTransactions(
      domain: Option[DomainAlias] = None,
      from: Option[CantonTimestamp] = None,
      to: Option[CantonTimestamp] = None,
      limit: Option[Int] = None,
  )(implicit
      traceContext: TraceContext
  ): Seq[(SyncStateInspection.DisplayOffset, LfCommittedTransaction)] = {
    // Need to apply limit after filtering for TransactionAccepted-Events (else might miss transactions due to
    // limit also counting towards non-TransactionAccepted-Events
    val found = findEvents(domain, from, to).collect {
      case (offset, TimestampedEvent(accepted: LedgerSyncEvent.TransactionAccepted, _, _, _)) =>
        offset -> accepted.transaction
    }
    limit.fold(found)(n => found.take(n))
  }

  /** Returns the events from the given domain; if the specified domain is empty, returns the events from the combined,
    * multi-domain event log. `from` and `to` only have an effect if the domain isn't empty.
    * @throws scala.RuntimeException (by Await.result and if lookup fails)
    */
  def findEvents(
      domain: Option[DomainAlias] = None,
      from: Option[CantonTimestamp] = None,
      to: Option[CantonTimestamp] = None,
      limit: Option[Int] = None,
  )(implicit
      traceContext: TraceContext
  ): Seq[(SyncStateInspection.DisplayOffset, TimestampedEvent)] = domain match {
    case None =>
      timeouts.inspection.await("finding events in the multi-domain event log")(
        participantNodePersistentState.value.multiDomainEventLog
          .lookupEventRange(None, limit)
          .map(_.map { case (offset, event) => (offset.toString, event) })
      )
    case Some(domainAlias) =>
      timeouts.inspection
        .await(s"$functionFullName from $from to $to in the event log")(
          getOrFail(getPersistentState(domainAlias), domainAlias).eventLog
            .lookupEventRange(None, None, from, to, limit)
        )
        .toSeq
        .map { case (offset, event) => (offset.toString, event) }
  }

  private def tryGetProtocolVersion(
      state: SyncDomainPersistentState,
      domain: DomainAlias,
  )(implicit traceContext: TraceContext): ProtocolVersion =
    timeouts.inspection
      .await(functionFullName)(state.parameterStore.lastParameters)
      .getOrElse(throw new IllegalStateException(s"No static domain parameters found for $domain"))
      .protocolVersion

  def findMessages(
      domain: DomainAlias,
      from: Option[Instant],
      to: Option[Instant],
      limit: Option[Int],
  )(implicit traceContext: TraceContext): Seq[PossiblyIgnoredProtocolEvent] = {
    val state = getPersistentState(domain).getOrElse(
      throw new NoSuchElementException(s"Unknown domain $domain")
    )
    val messagesF =
      if (from.isEmpty && to.isEmpty) state.sequencedEventStore.sequencedEvents(limit)
      else { // if a timestamp is set, need to use less efficient findRange method (it sorts results first)
        val cantonFrom =
          from.map(t => CantonTimestamp.assertFromInstant(t)).getOrElse(CantonTimestamp.MinValue)
        val cantonTo =
          to.map(t => CantonTimestamp.assertFromInstant(t)).getOrElse(CantonTimestamp.MaxValue)
        state.sequencedEventStore.findRange(ByTimestampRange(cantonFrom, cantonTo), limit).valueOr {
          // this is an inspection command, so no need to worry about pruned events
          case SequencedEventRangeOverlapsWithPruning(_criterion, _pruningStatus, foundEvents) =>
            foundEvents
        }
      }
    val closed =
      timeouts.inspection.await(s"finding messages from $from to $to on $domain")(messagesF)
    val opener =
      new EnvelopeOpener[PossiblyIgnoredSequencedEvent](
        tryGetProtocolVersion(state, domain),
        state.pureCryptoApi,
      )
    closed.map(opener.tryOpen)
  }

  def findMessage(domain: DomainAlias, criterion: SequencedEventStore.SearchCriterion)(implicit
      traceContext: TraceContext
  ): Either[SequencedEventNotFoundError, PossiblyIgnoredProtocolEvent] = {
    val state = getPersistentState(domain).getOrElse(
      throw new NoSuchElementException(s"Unknown domain $domain")
    )
    val messageF = state.sequencedEventStore.find(criterion).value
    val closed =
      timeouts.inspection.await(s"$functionFullName on $domain matching $criterion")(messageF)
    val opener = new EnvelopeOpener[PossiblyIgnoredSequencedEvent](
      tryGetProtocolVersion(state, domain),
      state.pureCryptoApi,
    )
    closed.map(opener.tryOpen)
  }

  def findComputedCommitments(
      domain: DomainAlias,
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId] = None,
  )(implicit
      traceContext: TraceContext
  ): Iterable[(CommitmentPeriod, ParticipantId, AcsCommitment.CommitmentType)] = {
    val persistentState = getPersistentState(domain)
    timeouts.inspection.await(s"$functionFullName from $start to $end on $domain")(
      getOrFail(persistentState, domain).acsCommitmentStore
        .searchComputedBetween(start, end, counterParticipant)
    )
  }

  def findReceivedCommitments(
      domain: DomainAlias,
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId] = None,
  )(implicit traceContext: TraceContext): Iterable[SignedProtocolMessage[AcsCommitment]] = {
    val persistentState = getPersistentState(domain)
    timeouts.inspection.await(s"$functionFullName from $start to $end on $domain")(
      getOrFail(persistentState, domain).acsCommitmentStore
        .searchReceivedBetween(start, end, counterParticipant)
    )
  }

  def outstandingCommitments(
      domain: DomainAlias,
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId],
  )(implicit traceContext: TraceContext): Iterable[(CommitmentPeriod, ParticipantId)] = {
    val persistentState = getPersistentState(domain)
    timeouts.inspection.await(s"$functionFullName from $start to $end on $domain")(
      getOrFail(persistentState, domain).acsCommitmentStore
        .outstanding(start, end, counterParticipant)
    )
  }

  def noOutstandingCommitmentsTs(domain: DomainAlias, beforeOrAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Option[CantonTimestamp] = {
    val persistentState = getPersistentState(domain)
    timeouts.inspection.await(s"$functionFullName on $domain for ts $beforeOrAt")(
      getOrFail(persistentState, domain).acsCommitmentStore.noOutstandingCommitments(beforeOrAt)
    )
  }

  /** Update the prehead for clean requests to the given value, bypassing all checks. Only used for testing. */
  def forceCleanPrehead(
      newHead: Option[RequestCounterCursorPrehead],
      domain: DomainAlias,
  )(implicit
      traceContext: TraceContext
  ): Either[String, Future[Unit]] = {
    getPersistentState(domain)
      .map(state => state.requestJournalStore.overridePreheadCleanForTesting(newHead))
      .toRight(s"Unknown domain $domain")
  }

  def forceCleanSequencerCounterPrehead(
      newHead: Option[SequencerCounterCursorPrehead],
      domain: DomainAlias,
  )(implicit traceContext: TraceContext): Either[String, Future[Unit]] = {
    getPersistentState(domain)
      .map(state => state.sequencerCounterTrackerStore.rewindPreheadSequencerCounter(newHead))
      .toRight(s"Unknown domain $domain")
  }

  def lookupCleanPrehead(domain: DomainAlias)(implicit
      traceContext: TraceContext
  ): Either[String, Future[Option[RequestCounterCursorPrehead]]] =
    getPersistentState(domain)
      .map(state => state.requestJournalStore.preheadClean)
      .toRight(s"Not connected to $domain")

  def requestStateInJournal(rc: RequestCounter, domain: DomainAlias)(implicit
      traceContext: TraceContext
  ): Either[String, Future[Option[RequestJournal.RequestData]]] =
    getPersistentState(domain)
      .toRight(s"Not connected to $domain")
      .map(state => state.requestJournalStore.query(rc).value)

  private[this] def getPersistentState(domain: DomainAlias): Option[SyncDomainPersistentState] =
    syncDomainPersistentStateManager.getByAlias(domain)

  def locateOffset(
      numTransactions: Long
  )(implicit traceContext: TraceContext): Future[Either[String, LedgerOffset]] = {

    if (numTransactions <= 0L)
      throw new IllegalArgumentException(
        s"Number of transactions needs to be positive and not $numTransactions"
      )

    participantNodePersistentState.value.multiDomainEventLog
      .locateOffset(numTransactions - 1L)
      .toRight(s"Participant does not contain $numTransactions transactions.")
      .map(UpstreamOffsetConvert.toLedgerOffset)
      .value
  }

  def getOffsetByTime(
      pruneUpTo: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Option[LedgerOffset]] =
    participantNodePersistentState.value.multiDomainEventLog
      .getOffsetByTimeUpTo(pruneUpTo)
      .map(UpstreamOffsetConvert.toLedgerOffset)
      .value

  def lookupPublicationTime(
      ledgerOffset: LedgerOffset
  )(implicit traceContext: TraceContext): EitherT[Future, String, CantonTimestamp] = for {
    globalOffset <- EitherT.fromEither[Future](
      UpstreamOffsetConvert.ledgerOffsetToGlobalOffset(ledgerOffset)
    )
    res <- participantNodePersistentState.value.multiDomainEventLog
      .lookupOffset(globalOffset)
      .toRight(s"offset $ledgerOffset not found")
    (_eventLogId, _localOffset, publicationTimestamp) = res
  } yield publicationTimestamp

}

object SyncStateInspection {

  private type DisplayOffset = String

  private final case class NoSuchDomain(alias: DomainAlias) extends AcsError

  private def getOrFail[T](opt: Option[T], domain: DomainAlias): T =
    opt.getOrElse(throw new IllegalArgumentException(s"no such domain [$domain]"))

}
