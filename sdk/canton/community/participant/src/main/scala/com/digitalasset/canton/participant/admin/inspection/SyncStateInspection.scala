// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.inspection

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond, Offset}
import com.digitalasset.canton.ledger.participant.state.{DomainIndex, RequestIndex}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection.{
  InFlightCount,
  SyncStateInspectionError,
}
import com.digitalasset.canton.participant.protocol.RequestJournal
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.{
  ConnectedDomainsLookup,
  SyncDomainPersistentStateManager,
  UpstreamOffsetConvert,
}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.DomainOffset
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.CommitmentPeriodState.fromIntValidSentPeriodState
import com.digitalasset.canton.protocol.{LfContractId, SerializableContract}
import com.digitalasset.canton.pruning.PruningStatus
import com.digitalasset.canton.sequencing.PossiblyIgnoredProtocolEvent
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelClient
import com.digitalasset.canton.sequencing.handlers.EnvelopeOpener
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.SequencedEventStore.{
  ByTimestampRange,
  PossiblyIgnoredSequencedEvent,
}
import com.digitalasset.canton.store.{SequencedEventRangeOverlapsWithPruning, SequencedEventStore}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.OptionUtils.OptionExtension
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainAlias, LfPartyId, ReassignmentCounter, RequestCounter}
import com.google.common.annotations.VisibleForTesting

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

  /** Returns the potentially large ACS of a given domain
    * containing a map of contract IDs to tuples containing the latest activation timestamp and the contract reassignment counter
    */
  def findAcs(
      domainAlias: DomainAlias
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncStateInspectionError, Map[
    LfContractId,
    (CantonTimestamp, ReassignmentCounter),
  ]] =
    for {
      state <- EitherT.fromEither[Future](
        syncDomainPersistentStateManager
          .getByAlias(domainAlias)
          .toRight(SyncStateInspection.NoSuchDomain(domainAlias))
      )

      snapshotO <- EitherT.right(state.acsInspection.getCurrentSnapshot().map(_.map(_.snapshot)))
    } yield snapshotO.fold(Map.empty[LfContractId, (CantonTimestamp, ReassignmentCounter)])(
      _.toMap
    )

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
          .traverse(_.acsInspection.findContracts(filterId, filterPackage, filterTemplate, limit))

      },
      domain,
    )

  @VisibleForTesting
  def acsPruningStatus(
      domain: DomainAlias
  )(implicit traceContext: TraceContext): Option[PruningStatus] =
    timeouts.inspection
      .awaitUS("acsPruningStatus")(
        getOrFail(
          getPersistentState(domain),
          domain,
        ).acsInspection.activeContractStore.pruningStatus
      )
      .onShutdown(throw GrpcErrors.AbortedDueToShutdown.Error().asGrpcError)

  private def disableJournalCleaningForFilter(
      domains: Map[DomainId, SyncDomainPersistentState],
      filterDomain: DomainId => Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsInspectionError, Unit] = {
    val disabledCleaningF = Future
      .sequence(domains.collect {
        case (domainId, _) if filterDomain(domainId) =>
          journalCleaningControl.disable(domainId)
      })
      .map(_ => ())
    EitherT.right(disabledCleaningF)
  }

  def allProtocolVersions: Map[DomainId, ProtocolVersion] =
    syncDomainPersistentStateManager.getAll.view
      .mapValues(_.staticDomainParameters.protocolVersion)
      .toMap

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
  ): EitherT[FutureUnlessShutdown, AcsInspectionError, Unit] = {
    val allDomains = syncDomainPersistentStateManager.getAll

    // disable journal cleaning for the duration of the dump
    disableJournalCleaningForFilter(allDomains, filterDomain)
      .mapK(FutureUnlessShutdown.outcomeK)
      .flatMap { _ =>
        MonadUtil.sequentialTraverse_(allDomains) {
          case (domainId, state) if filterDomain(domainId) =>
            val (domainIdForExport, protocolVersion) =
              contractDomainRenames.getOrElse(
                domainId,
                (domainId, state.staticDomainParameters.protocolVersion),
              )
            val acsInspection = state.acsInspection

            val ret = for {
              result <- acsInspection
                .forEachVisibleActiveContract(
                  domainId,
                  parties,
                  timestamp,
                  skipCleanTimestampCheck = skipCleanTimestampCheck,
                ) { case (contract, reassignmentCounter) =>
                  val activeContract =
                    ActiveContract.create(domainIdForExport, contract, reassignmentCounter)(
                      protocolVersion
                    )

                  activeContract.writeDelimitedTo(outputStream) match {
                    case Left(errorMessage) =>
                      Left(
                        AcsInspectionError.SerializationIssue(
                          domainId,
                          contract.contractId,
                          errorMessage,
                        )
                      )
                    case Right(_) =>
                      outputStream.flush()
                      Either.unit
                  }
                }

              _ <- result match {
                case Some((allStakeholders, snapshotTs)) if partiesOffboarding =>
                  for {
                    syncDomain <- EitherT.fromOption[FutureUnlessShutdown](
                      connectedDomainsLookup.get(domainId),
                      AcsInspectionError.OffboardingParty(
                        domainId,
                        s"Unable to get topology client for domain $domainId; check domain connectivity.",
                      ),
                    )

                    _ <- acsInspection.checkOffboardingSnapshot(
                      participantId,
                      offboardedParties = parties,
                      allStakeholders = allStakeholders,
                      snapshotTs = snapshotTs,
                      topologyClient = syncDomain.topologyClient,
                    )
                  } yield ()

                // Snapshot is empty or partiesOffboarding is false
                case _ => EitherTUtil.unitUS[AcsInspectionError]
              }

            } yield ()
            // re-enable journal cleaning after the dump
            ret.thereafter { _ =>
              journalCleaningControl.enable(domainId)
            }
          case _ =>
            EitherTUtil.unitUS
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
  ): Future[Option[Int]] =
    getPersistentState(domain) match {
      case None => Future.successful(None)
      case Some(state) => state.activeContractStore.contractCount(timestamp).map(Some(_))
    }

  def requestJournalSize(
      domain: DomainAlias,
      start: CantonTimestamp = CantonTimestamp.Epoch,
      end: Option[CantonTimestamp] = None,
  )(implicit traceContext: TraceContext): Option[Int] =
    getPersistentState(domain).map { state =>
      timeouts.inspection.await(
        s"$functionFullName from $start to $end from the journal of domain $domain"
      )(
        state.requestJournalStore.size(start, end)
      )
    }

  def activeContractsStakeholdersFilter(
      domain: DomainId,
      timestamp: CantonTimestamp,
      parties: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[(LfContractId, ReassignmentCounter)]] =
    for {
      state <- FutureUnlessShutdown.fromTry(
        syncDomainPersistentStateManager
          .get(domain)
          .toTry(new IllegalArgumentException(s"Unable to find contract store for $domain."))
      )

      snapshot <- FutureUnlessShutdown.outcomeF(state.activeContractStore.snapshot(timestamp))

      // check that the active contract store has not been pruned up to timestamp, otherwise the snapshot is inconsistent.
      pruningStatus <- state.activeContractStore.pruningStatus
      _ <-
        if (pruningStatus.exists(_.timestamp > timestamp)) {
          FutureUnlessShutdown.failed(
            new IllegalStateException(
              s"Active contract store for domain $domain has been pruned up to ${pruningStatus
                  .map(_.lastSuccess)}, which is after the requested timestamp $timestamp"
            )
          )
        } else FutureUnlessShutdown.unit

      contracts <- FutureUnlessShutdown.outcomeF(
        state.contractStore
          .lookupManyExistingUncached(snapshot.keys.toSeq)
          .valueOr { missingContractId =>
            ErrorUtil.invalidState(
              s"Contract id $missingContractId is in the active contract store but its contents is not in the contract store"
            )
          }
      )

      contractsWithTransferCounter = contracts.map(c => c -> snapshot(c.contractId)._2)

      filteredByParty = contractsWithTransferCounter.collect {
        case (contract, transferCounter)
            if parties.intersect(contract.contract.metadata.stakeholders).nonEmpty =>
          (contract.contractId, transferCounter)
      }
    } yield filteredByParty.toSet

  private def tryGetProtocolVersion(
      state: SyncDomainPersistentState,
      domain: DomainAlias,
  )(implicit traceContext: TraceContext): ProtocolVersion =
    timeouts.inspection
      .await(functionFullName)(state.parameterStore.lastParameters)
      .getOrElse(throw new IllegalStateException(s"No static domain parameters found for $domain"))
      .protocolVersion

  def getProtocolVersion(
      domain: DomainAlias
  )(implicit traceContext: TraceContext): Future[ProtocolVersion] =
    for {
      param <- syncDomainPersistentStateManager
        .getByAlias(domain)
        .traverse(_.parameterStore.lastParameters)
    } yield {
      getOrFail(param, domain)
        .map(_.protocolVersion)
        .getOrElse(
          throw new IllegalStateException(s"No static domain parameters found for $domain")
        )
    }

  def findMessages(
      domain: DomainAlias,
      from: Option[Instant],
      to: Option[Instant],
      limit: Option[Int],
  )(implicit traceContext: TraceContext): Seq[ParsingResult[PossiblyIgnoredProtocolEvent]] = {
    val state = getOrFail(getPersistentState(domain), domain)
    val messagesF =
      if (from.isEmpty && to.isEmpty)
        FutureUnlessShutdown.outcomeF(state.sequencedEventStore.sequencedEvents(limit))
      else { // if a timestamp is set, need to use less efficient findRange method (it sorts results first)
        val cantonFrom =
          from.map(t => CantonTimestamp.assertFromInstant(t)).getOrElse(CantonTimestamp.MinValue)
        val cantonTo =
          to.map(t => CantonTimestamp.assertFromInstant(t)).getOrElse(CantonTimestamp.MaxValue)
        state.sequencedEventStore
          .findRange(ByTimestampRange(cantonFrom, cantonTo), limit)
          .valueOr {
            // this is an inspection command, so no need to worry about pruned events
            case SequencedEventRangeOverlapsWithPruning(_criterion, _pruningStatus, foundEvents) =>
              foundEvents
          }
      }
    val closed =
      timeouts.inspection
        .awaitUS(s"finding messages from $from to $to on $domain")(messagesF)
        .onShutdown(throw GrpcErrors.AbortedDueToShutdown.Error().asGrpcError)
    val opener =
      new EnvelopeOpener[PossiblyIgnoredSequencedEvent](
        tryGetProtocolVersion(state, domain),
        state.pureCryptoApi,
      )
    closed.map(opener.open)
  }

  @VisibleForTesting
  def findMessage(domain: DomainAlias, criterion: SequencedEventStore.SearchCriterion)(implicit
      traceContext: TraceContext
  ): Option[ParsingResult[PossiblyIgnoredProtocolEvent]] = {
    val state = getOrFail(getPersistentState(domain), domain)
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
  ): Iterable[(CommitmentPeriod, ParticipantId, AcsCommitment.CommitmentType)] =
    timeouts.inspection.await(s"$functionFullName from $start to $end on $domain")(
      getOrFail(getPersistentState(domain), domain).acsCommitmentStore
        .searchComputedBetween(start, end, counterParticipant.toList)
    )

  def findLastComputedAndSent(
      domain: DomainAlias
  )(implicit traceContext: TraceContext): Option[CantonTimestampSecond] =
    timeouts.inspection.await(s"$functionFullName on $domain")(
      getOrFail(getPersistentState(domain), domain).acsCommitmentStore.lastComputedAndSent
    )

  def findReceivedCommitments(
      domain: DomainAlias,
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId] = None,
  )(implicit traceContext: TraceContext): Iterable[SignedProtocolMessage[AcsCommitment]] =
    timeouts.inspection.await(s"$functionFullName from $start to $end on $domain")(
      getOrFail(getPersistentState(domain), domain).acsCommitmentStore
        .searchReceivedBetween(start, end, counterParticipant.toList)
    )

  def crossDomainSentCommitmentMessages(
      domainPeriods: Seq[DomainSearchCommitmentPeriod],
      counterParticipants: Seq[ParticipantId],
      states: Seq[CommitmentPeriodState],
      verbose: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Iterable[SentAcsCommitment]] =
    timeouts.inspection.await(functionFullName) {
      val searchResult = domainPeriods.map { dp =>
        for {
          domain <- syncDomainPersistentStateManager
            .aliasForDomainId(dp.indexedDomain.domainId)
            .toRight(s"No domain alias found for ${dp.indexedDomain.domainId}")

          persistentState <- getPersistentStateE(domain)

          result = for {
            computed <- persistentState.acsCommitmentStore
              .searchComputedBetween(dp.fromExclusive, dp.toInclusive, counterParticipants)
            received <-
              if (verbose)
                persistentState.acsCommitmentStore
                  .searchReceivedBetween(dp.fromExclusive, dp.toInclusive, counterParticipants)
                  .map(iter => iter.map(rec => rec.message))
              else Future.successful(Seq.empty)
            outstanding <- persistentState.acsCommitmentStore
              .outstanding(
                dp.fromExclusive,
                dp.toInclusive,
                counterParticipants,
                includeMatchedPeriods = true,
              )
              .map { collection =>
                collection
                  .collect { case (period, participant, state) =>
                    val converted = fromIntValidSentPeriodState(state.toInt)
                    (period, participant, converted)
                  }
                  .flatMap {
                    case (period, participant, Some(state)) => Some((period, participant, state))
                    case _ => None
                  }
              }
          } yield SentAcsCommitment
            .compare(dp.indexedDomain.domainId, computed, received, outstanding, verbose)
            .filter(cmt => states.isEmpty || states.contains(cmt.state))
        } yield result
      }

      val (lefts, rights) = searchResult.partitionMap(identity)

      NonEmpty.from(lefts) match {
        case Some(leftsNe) => Future.successful(Left(leftsNe.head1))
        case None =>
          Future.sequence(rights).map(seqSentAcsCommitments => Right(seqSentAcsCommitments.flatten))
      }
    }

  def crossDomainReceivedCommitmentMessages(
      domainPeriods: Seq[DomainSearchCommitmentPeriod],
      counterParticipants: Seq[ParticipantId],
      states: Seq[CommitmentPeriodState],
      verbose: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Iterable[ReceivedAcsCommitment]] =
    timeouts.inspection
      .awaitUS(functionFullName) {
        val searchResult = domainPeriods.map { dp =>
          for {
            domain <- syncDomainPersistentStateManager
              .aliasForDomainId(dp.indexedDomain.domainId)
              .toRight(s"No domain alias found for ${dp.indexedDomain.domainId}")
            persistentState <- getPersistentStateE(domain)

            result = for {
              computed <-
                if (verbose)
                  FutureUnlessShutdown.outcomeF(
                    persistentState.acsCommitmentStore
                      .searchComputedBetween(dp.fromExclusive, dp.toInclusive, counterParticipants)
                  )
                else FutureUnlessShutdown.pure(Seq.empty)
              received <- FutureUnlessShutdown.outcomeF(
                persistentState.acsCommitmentStore
                  .searchReceivedBetween(dp.fromExclusive, dp.toInclusive, counterParticipants)
                  .map(iter => iter.map(rec => rec.message))
              )
              outstanding <- FutureUnlessShutdown.outcomeF(
                persistentState.acsCommitmentStore
                  .outstanding(
                    dp.fromExclusive,
                    dp.toInclusive,
                    counterParticipants,
                    includeMatchedPeriods = true,
                  )
              )
              buffered <- persistentState.acsCommitmentStore.queue
                .peekThrough(dp.toInclusive) // peekThrough takes an upper bound parameter
                .map(iter =>
                  iter.filter(cmt =>
                    cmt.period.fromExclusive >= dp.fromExclusive && cmt.domainId == dp.indexedDomain.domainId && (counterParticipants.isEmpty ||
                      counterParticipants
                        .contains(cmt.sender))
                  )
                )
            } yield ReceivedAcsCommitment
              .compare(
                dp.indexedDomain.domainId,
                received,
                computed,
                buffered,
                outstanding,
                verbose,
              )
              .filter(cmt => states.isEmpty || states.contains(cmt.state))
          } yield result
        }

        val (lefts, rights) = searchResult.partitionMap(identity)

        NonEmpty.from(lefts) match {
          case Some(leftsNe) => FutureUnlessShutdown.pure(Left(leftsNe.head1))
          case None =>
            rights.sequence.map(seqRecAcsCommitments =>
              Right(seqRecAcsCommitments.flatten.toSet)
            ) // toSet is done to avoid duplicates
        }
      }
      .onShutdown(throw GrpcErrors.AbortedDueToShutdown.Error().asGrpcError)

  def outstandingCommitments(
      domain: DomainAlias,
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): Iterable[(CommitmentPeriod, ParticipantId, CommitmentPeriodState)] = {
    val persistentState = getPersistentState(domain)
    timeouts.inspection.await(s"$functionFullName from $start to $end on $domain")(
      getOrFail(persistentState, domain).acsCommitmentStore
        .outstanding(start, end, counterParticipant.toList)
    )
  }

  def bufferedCommitments(
      domain: DomainAlias,
      endAtOrBefore: CantonTimestamp,
  )(implicit traceContext: TraceContext): Iterable[AcsCommitment] = {
    val persistentState = getPersistentState(domain)
    timeouts.inspection
      .awaitUS(s"$functionFullName to and including $endAtOrBefore on $domain")(
        getOrFail(persistentState, domain).acsCommitmentStore.queue.peekThrough(endAtOrBefore)
      )
      .onShutdown(throw GrpcErrors.AbortedDueToShutdown.Error().asGrpcError)
  }

  def noOutstandingCommitmentsTs(domain: DomainAlias, beforeOrAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Option[CantonTimestamp] =
    timeouts.inspection.await(s"$functionFullName on $domain for ts $beforeOrAt")(
      getOrFail(getPersistentState(domain), domain).acsCommitmentStore
        .noOutstandingCommitments(beforeOrAt)
    )

  def lookupRequestIndex(domain: DomainAlias)(implicit
      traceContext: TraceContext
  ): Either[String, Future[Option[RequestIndex]]] =
    lookupDomainLedgerEnd(domain)
      .map(_.map(_.requestIndex))

  def lookupDomainLedgerEnd(domain: DomainAlias)(implicit
      traceContext: TraceContext
  ): Either[String, Future[DomainIndex]] =
    getPersistentStateE(domain)
      .map(state =>
        participantNodePersistentState.value.ledgerApiStore
          .domainIndex(state.indexedDomain.domainId)
      )

  def requestStateInJournal(rc: RequestCounter, domain: DomainAlias)(implicit
      traceContext: TraceContext
  ): Either[String, Future[Option[RequestJournal.RequestData]]] =
    getPersistentStateE(domain)
      .map(state => state.requestJournalStore.query(rc).value)

  private[this] def getPersistentState(domain: DomainAlias): Option[SyncDomainPersistentState] =
    syncDomainPersistentStateManager.getByAlias(domain)

  private[this] def getPersistentStateE(
      domain: DomainAlias
  ): Either[String, SyncDomainPersistentState] =
    syncDomainPersistentStateManager.getByAlias(domain).toRight(s"no such domain [$domain]")

  def getOffsetByTime(
      pruneUpTo: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Option[Long]] =
    participantNodePersistentState.value.ledgerApiStore
      .lastDomainOffsetBeforeOrAtPublicationTime(pruneUpTo)
      .map(
        _.map(_.offset.toLong)
      )

  def lookupPublicationTime(
      ledgerOffset: Long
  )(implicit traceContext: TraceContext): EitherT[Future, String, CantonTimestamp] = for {
    offset <- EitherT.fromEither[Future](
      UpstreamOffsetConvert.toLedgerSyncOffset(ledgerOffset)
    )
    domainOffset <- EitherT(
      participantNodePersistentState.value.ledgerApiStore
        .domainOffset(offset)
        .map(_.toRight(s"offset $ledgerOffset not found"))
    )
  } yield CantonTimestamp(domainOffset.publicationTime)

  def countInFlight(
      domain: DomainAlias
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, InFlightCount] =
    for {
      state <- EitherT.fromEither[Future](
        getPersistentState(domain)
          .toRight(s"Unknown domain $domain")
      )
      domainId = state.indexedDomain.domainId
      unsequencedSubmissions <- EitherT(
        participantNodePersistentState.value.inFlightSubmissionStore
          .lookupUnsequencedUptoUnordered(domainId, CantonTimestamp.now())
          .map(Right(_))
          .onShutdown(Left("Aborted due to shutdown"))
      )
      pendingSubmissions = NonNegativeInt.tryCreate(unsequencedSubmissions.size)
      pendingTransactions <- EitherT.right[String](state.requestJournalStore.totalDirtyRequests())
    } yield {
      InFlightCount(pendingSubmissions, pendingTransactions)
    }

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
              domainPersistentState.indexedDomain.domainId
            )
        )
      )
      .getOrElse(0)

  def onlyForTestingMoveLedgerEndBackToScratch()(implicit traceContext: TraceContext): Unit =
    timeouts.inspection.await(functionFullName)(
      participantNodePersistentState.value.ledgerApiStore.onlyForTestingMoveLedgerEndBackToScratch()
    )

  def lastDomainOffset(
      domainId: DomainId
  )(implicit traceContext: TraceContext): Option[DomainOffset] =
    timeouts.inspection.await(s"$functionFullName")(
      participantNodePersistentState.value.ledgerApiStore.lastDomainOffsetBeforeOrAt(
        domainId,
        Offset.fromAbsoluteOffsetO(
          participantNodePersistentState.value.ledgerApiStore.ledgerEndCache()._1
        ),
      )
    )

  def prunedUptoOffset(implicit traceContext: TraceContext): Option[GlobalOffset] =
    timeouts.inspection.await(functionFullName)(
      participantNodePersistentState.value.pruningStore.pruningStatus().map(_.completedO)
    )

  def getSequencerChannelClient(domainId: DomainId): Option[SequencerChannelClient] = for {
    syncDomain <- connectedDomainsLookup.get(domainId)
    sequencerChannelClient <- syncDomain.domainHandle.sequencerChannelClientO
  } yield sequencerChannelClient

  def getAcsInspection(domainId: DomainId): Option[AcsInspection] =
    connectedDomainsLookup.get(domainId).map(_.domainHandle.domainPersistentState.acsInspection)
}

object SyncStateInspection {

  sealed trait SyncStateInspectionError extends Product with Serializable
  private final case class NoSuchDomain(alias: DomainAlias) extends SyncStateInspectionError

  private def getOrFail[T](opt: Option[T], domain: DomainAlias): T =
    opt.getOrElse(throw new IllegalArgumentException(s"no such domain [$domain]"))

  final case class InFlightCount(
      pendingSubmissions: NonNegativeInt,
      pendingTransactions: NonNegativeInt,
  ) {
    def exists: Boolean = pendingSubmissions.unwrap > 0 || pendingTransactions.unwrap > 0
  }

}
