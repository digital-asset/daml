// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.inspection

import cats.Eval
import cats.data.{EitherT, OptionT}
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.admin.inspection.Error.SerializationIssue
import com.digitalasset.canton.participant.protocol.RequestJournal
import com.digitalasset.canton.participant.store.ActiveContractStore.AcsError
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.{
  ConnectedDomainsLookup,
  SyncDomainPersistentStateManager,
  UpstreamOffsetConvert,
}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.DomainOffset
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  SignedProtocolMessage,
}
import com.digitalasset.canton.protocol.{LfContractId, SerializableContract}
import com.digitalasset.canton.sequencing.PossiblyIgnoredProtocolEvent
import com.digitalasset.canton.sequencing.handlers.EnvelopeOpener
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.CursorPrehead.{
  RequestCounterCursorPrehead,
  SequencerCounterCursorPrehead,
}
import com.digitalasset.canton.store.SequencedEventStore.{
  ByTimestampRange,
  PossiblyIgnoredSequencedEvent,
}
import com.digitalasset.canton.store.{SequencedEventRangeOverlapsWithPruning, SequencedEventStore}
import com.digitalasset.canton.topology.{DomainId, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainAlias, LfPartyId, RequestCounter, TransferCounter}

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
    connectedDomainsLookup: ConnectedDomainsLookup,
    participantId: ParticipantId,
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

  /** returns the potentially big ACS of a given domain */
  def findAcs(
      domainAlias: DomainAlias
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsError, Map[LfContractId, (CantonTimestamp, TransferCounter)]] = {

    for {
      state <- EitherT.fromEither[Future](
        syncDomainPersistentStateManager
          .getByAlias(domainAlias)
          .toRight(SyncStateInspection.NoSuchDomain(domainAlias))
      )

      snapshotO <- EitherT.right(AcsInspection.getCurrentSnapshot(state).map(_.map(_.snapshot)))
    } yield snapshotO.fold(Map.empty[LfContractId, (CantonTimestamp, TransferCounter)])(
      _.toMap
    )
  }

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
          .traverse(AcsInspection.findContracts(_, filterId, filterPackage, filterTemplate, limit))

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

  def allProtocolVersions: Map[DomainId, ProtocolVersion] =
    syncDomainPersistentStateManager.getAll.view.mapValues(_.protocolVersion).toMap

  def exportAcsDumpActiveContracts(
      outputStream: OutputStream,
      filterDomain: DomainId => Boolean,
      parties: Set[LfPartyId],
      timestamp: Option[CantonTimestamp],
      contractDomainRenames: Map[DomainId, (DomainId, ProtocolVersion)],
      skipCleanTimestampCheck: Boolean,
      partiesOffboarding: Boolean,
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
            result <- AcsInspection
              .forEachVisibleActiveContract(
                domainId,
                state,
                parties,
                timestamp,
                skipCleanTimestampCheck = skipCleanTimestampCheck,
              ) { case (contract, transferCounter) =>
                val activeContract =
                  ActiveContract.create(domainIdForExport, contract, transferCounter)(
                    protocolVersion
                  )

                activeContract.writeDelimitedTo(outputStream) match {
                  case Left(errorMessage) =>
                    Left(SerializationIssue(domainId, contract.contractId, errorMessage))
                  case Right(_) =>
                    outputStream.flush()
                    Right(())
                }
              }

            _ <- result match {
              case Some((allStakeholders, snapshotTs)) if partiesOffboarding =>
                for {
                  syncDomain <- EitherT.fromOption[Future](
                    connectedDomainsLookup.get(domainId),
                    Error.OffboardingParty(
                      domainId,
                      s"Unable to get topology client for domain $domainId; check domain connectivity.",
                    ),
                  )

                  _ <- AcsInspection
                    .checkOffboardingSnapshot(
                      participantId,
                      offboardedParties = parties,
                      allStakeholders = allStakeholders,
                      snapshotTs = snapshotTs,
                      topologyClient = syncDomain.topologyClient,
                    )
                    .leftMap[Error](err => Error.OffboardingParty(domainId, err))
                } yield ()

              // Snapshot is empty or partiesOffboarding is false
              case _ => EitherTUtil.unit[Error]
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
  )(implicit traceContext: TraceContext): Seq[ParsingResult[PossiblyIgnoredProtocolEvent]] = {
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
    closed.map(opener.open)
  }

  def findMessage(domain: DomainAlias, criterion: SequencedEventStore.SearchCriterion)(implicit
      traceContext: TraceContext
  ): Option[ParsingResult[PossiblyIgnoredProtocolEvent]] = {
    val state = getPersistentState(domain).getOrElse(
      throw new NoSuchElementException(s"Unknown domain $domain")
    )
    val messageF = state.sequencedEventStore.find(criterion).value
    val closed =
      timeouts.inspection
        .await(s"$functionFullName on $domain matching $criterion")(messageF)
        .toOption
    val opener = new EnvelopeOpener[PossiblyIgnoredSequencedEvent](
      tryGetProtocolVersion(state, domain),
      state.pureCryptoApi,
    )
    closed.map(opener.open)
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

  def bufferedCommitments(
      domain: DomainAlias,
      endAtOrBefore: CantonTimestamp,
  )(implicit traceContext: TraceContext): Iterable[AcsCommitment] = {
    val persistentState = getPersistentState(domain)
    timeouts.inspection.await(s"$functionFullName to and including $endAtOrBefore on $domain")(
      getOrFail(persistentState, domain).acsCommitmentStore.queue.peekThrough(endAtOrBefore)
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
  @deprecated(
    "usage being removed as part of fusing MultiDomainEventLog and Ledger API Indexer",
    "3.1",
  )
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

  @deprecated(
    "usage being removed as part of fusing MultiDomainEventLog and Ledger API Indexer",
    "3.1",
  )
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

  @deprecated(
    "usage being removed as part of fusing MultiDomainEventLog and Ledger API Indexer",
    "3.1",
  )
  def locateOffset(
      numTransactions: Long
  )(implicit traceContext: TraceContext): Future[Either[String, ParticipantOffset]] = {

    if (numTransactions <= 0L)
      throw new IllegalArgumentException(
        s"Number of transactions needs to be positive and not $numTransactions"
      )

    participantNodePersistentState.value.multiDomainEventLog
      .locateOffset(numTransactions - 1L)
      .toRight(s"Participant does not contain $numTransactions transactions.")
      .map(UpstreamOffsetConvert.toParticipantOffset)
      .value
  }

  def getOffsetByTime(
      pruneUpTo: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Option[ParticipantOffset]] =
    participantNodePersistentState.value.multiDomainEventLog
      .getOffsetByTimeUpTo(pruneUpTo)
      .map(UpstreamOffsetConvert.toParticipantOffset)
      .value

  def lookupPublicationTime(
      ledgerOffset: ParticipantOffset
  )(implicit traceContext: TraceContext): EitherT[Future, String, CantonTimestamp] = for {
    globalOffset <- EitherT.fromEither[Future](
      UpstreamOffsetConvert.ledgerOffsetToGlobalOffset(ledgerOffset)
    )
    res <- participantNodePersistentState.value.multiDomainEventLog
      .lookupOffset(globalOffset)
      .toRight(s"offset $ledgerOffset not found")
    (_eventLogId, _localOffset, publicationTimestamp) = res
  } yield publicationTimestamp

  def hasInFlightSubmissions(
      domain: DomainAlias
  )(implicit traceContext: TraceContext): EitherT[Future, String, Boolean] = for {
    domainId <- EitherT.fromEither[Future](
      getPersistentState(domain).toRight(s"Unknown domain $domain").map(_.domainId.domainId)
    )
    earliestInFlightO <-
      EitherT.right[String](
        participantNodePersistentState.value.inFlightSubmissionStore.lookupEarliest(domainId)
      )
  } yield earliestInFlightO.isDefined

  def hasDirtyRequests(
      domain: DomainAlias
  )(implicit traceContext: TraceContext): EitherT[Future, String, Boolean] =
    for {
      state <- EitherT.fromEither[Future](
        getPersistentState(domain)
          .toRight(s"Unknown domain $domain")
      )
      count <- EitherT.right[String](state.requestJournalStore.totalDirtyRequests())
    } yield count > 0

  def verifyLapiStoreIntegrity()(implicit traceContext: TraceContext): Unit =
    timeouts.inspection.await(functionFullName)(
      participantNodePersistentState.value.ledgerApiStore.onlyForTestingVerifyIntegrity()
    )

  def acceptedTransactionCount(domainAlias: DomainAlias)(implicit traceContext: TraceContext): Int =
    getPersistentState(domainAlias)
      .map(domainPersistentState =>
        timeouts.inspection.await(functionFullName)(
          participantNodePersistentState.value.ledgerApiStore
            .onlyForTestingNumberOfAcceptedTransactionsFor(
              domainPersistentState.domainId.domainId
            )
        )
      )
      .getOrElse(0)

  def onlyForTestingMoveLedgerEndBackToScratch()(implicit traceContext: TraceContext): Unit =
    timeouts.inspection.await(functionFullName)(
      participantNodePersistentState.value.ledgerApiStore.onlyForTestingMoveLedgerAndBackToScratch()
    )

  def lastDomainOffset(
      domainId: DomainId
  )(implicit traceContext: TraceContext): Option[DomainOffset] =
    timeouts.inspection.await(s"$functionFullName")(
      participantNodePersistentState.value.ledgerApiStore.lastDomainOffsetBeforeOrAt(
        domainId,
        participantNodePersistentState.value.ledgerApiStore.ledgerEndCache()._1,
      )
    )
}

object SyncStateInspection {

  private type DisplayOffset = String

  private final case class NoSuchDomain(alias: DomainAlias) extends AcsError

  private def getOrFail[T](opt: Option[T], domain: DomainAlias): T =
    opt.getOrElse(throw new IllegalArgumentException(s"no such domain [$domain]"))

}
