// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.inspection

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong}
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond, Offset}
import com.digitalasset.canton.ledger.participant.state.{DomainIndex, RequestIndex}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcUSExtended
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection.{
  InFlightCount,
  SyncStateInspectionError,
}
import com.digitalasset.canton.participant.protocol.RequestJournal
import com.digitalasset.canton.participant.pruning.SortedReconciliationIntervalsProviderFactory
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.ActiveContractStore.ActivenessChangeDetail
import com.digitalasset.canton.participant.sync.{
  ConnectedDomainsLookup,
  SyncDomainPersistentStateManager,
}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.DomainOffset
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.CommitmentPeriodState.{
  Matched,
  fromIntValidSentPeriodState,
}
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId, SerializableContract}
import com.digitalasset.canton.pruning.{
  ConfigForDomainThresholds,
  ConfigForSlowCounterParticipants,
  CounterParticipantIntervalsBehind,
  PruningStatus,
}
import com.digitalasset.canton.sequencing.PossiblyIgnoredProtocolEvent
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelClient
import com.digitalasset.canton.sequencing.handlers.EnvelopeOpener
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.SequencedEventStore.{
  ByTimestampRange,
  PossiblyIgnoredSequencedEvent,
}
import com.digitalasset.canton.store.{SequencedEventRangeOverlapsWithPruning, SequencedEventStore}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.OptionUtils.OptionExtension
import com.digitalasset.canton.util.ReassignmentTag.Source
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  DomainAlias,
  LfPartyId,
  ReassignmentCounter,
  RequestCounter,
  SequencerCounter,
}
import com.google.common.annotations.VisibleForTesting

import java.io.OutputStream
import java.time.Instant
import scala.collection.immutable.SortedMap
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
    syncCrypto: SyncCryptoApiProvider,
    participantId: ParticipantId,
    futureSupervisor: FutureSupervisor,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  import SyncStateInspection.getOrFail

  private lazy val sortedReconciliationIntervalsProviderFactory =
    new SortedReconciliationIntervalsProviderFactory(
      syncDomainPersistentStateManager,
      futureSupervisor,
      loggerFactory,
    )

  /** Look up all unpruned state changes of a set of contracts on all domains.
    * If a contract is not found in an available ACS it will be omitted from the response.
    */
  def lookupContractDomains(
      contractIds: Set[LfContractId]
  )(implicit
      traceContext: TraceContext
  ): Future[
    Map[DomainId, SortedMap[LfContractId, Seq[(CantonTimestamp, ActivenessChangeDetail)]]]
  ] =
    syncDomainPersistentStateManager.getAll.toList
      .map { case (id, state) =>
        state.activeContractStore.activenessOf(contractIds.toSeq).map(s => id -> s)
      }
      .sequence
      .map(_.toMap)

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
          .failOnShutdownToAbortException("findContracts")
      },
      domain,
    )

  def findContractPayloads(
      domain: DomainId,
      contractIds: Seq[LfContractId],
      limit: Int,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, SerializableContract]] = {
    val domainAlias = syncDomainPersistentStateManager
      .aliasForDomainId(domain)
      .getOrElse(throw new IllegalArgumentException(s"no such domain [$domain]"))

    NonEmpty.from(contractIds) match {
      case None =>
        FutureUnlessShutdown.pure(Map.empty[LfContractId, SerializableContract])
      case Some(neCids) =>
        val domainAcsInspection =
          getOrFail(
            syncDomainPersistentStateManager
              .get(domain),
            domainAlias,
          ).acsInspection

        domainAcsInspection.findContractPayloads(
          neCids,
          limit,
        )
    }
  }

  def lookupReassignmentIds(
      targetDomain: DomainId,
      sourceDomainId: DomainId,
      contractIds: Seq[LfContractId],
      minUnassignmentTs: Option[CantonTimestamp] = None,
      minCompletionTs: Option[CantonTimestamp] = None,
  )(implicit traceContext: TraceContext): Map[LfContractId, Seq[ReassignmentId]] =
    timeouts.inspection
      .awaitUS(functionFullName) {
        syncDomainPersistentStateManager
          .get(targetDomain)
          .traverse(
            _.reassignmentStore.findContractReassignmentId(
              contractIds,
              Some(Source(sourceDomainId)),
              minUnassignmentTs,
              minCompletionTs,
            )
          )
      }
      .asGrpcResponse
      .getOrElse(throw new IllegalArgumentException(s"no such domain [$targetDomain]"))

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
      .asGrpcResponse

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

  def contractCount(implicit traceContext: TraceContext): Future[Int] =
    participantNodePersistentState.value.contractStore.contractCount()

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
        participantNodePersistentState.value.contractStore
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
        .asGrpcResponse
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
    timeouts.inspection
      .awaitUS(s"$functionFullName from $start to $end on $domain")(
        getOrFail(getPersistentState(domain), domain).acsCommitmentStore
          .searchComputedBetween(start, end, counterParticipant.toList)
      )
      .asGrpcResponse

  def findLastComputedAndSent(
      domain: DomainAlias
  )(implicit traceContext: TraceContext): Option[CantonTimestampSecond] =
    timeouts.inspection
      .awaitUS(s"$functionFullName on $domain")(
        getOrFail(getPersistentState(domain), domain).acsCommitmentStore.lastComputedAndSent
      )
      .asGrpcResponse

  def findReceivedCommitments(
      domain: DomainAlias,
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId] = None,
  )(implicit traceContext: TraceContext): Iterable[SignedProtocolMessage[AcsCommitment]] =
    timeouts.inspection
      .awaitUS(s"$functionFullName from $start to $end on $domain")(
        getOrFail(getPersistentState(domain), domain).acsCommitmentStore
          .searchReceivedBetween(start, end, counterParticipant.toList)
      )
      .asGrpcResponse

  def crossDomainSentCommitmentMessages(
      domainPeriods: Seq[DomainSearchCommitmentPeriod],
      counterParticipants: Seq[ParticipantId],
      states: Seq[CommitmentPeriodState],
      verbose: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Iterable[SentAcsCommitment]] =
    timeouts.inspection
      .awaitUS(functionFullName) {
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
                else FutureUnlessShutdown.pure(Seq.empty)
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
          case Some(leftsNe) => FutureUnlessShutdown.pure(Left(leftsNe.head1))
          case None =>
            FutureUnlessShutdown
              .sequence(rights)
              .map(seqSentAcsCommitments => Right(seqSentAcsCommitments.flatten))
        }
      }
      .asGrpcResponse

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
                  persistentState.acsCommitmentStore
                    .searchComputedBetween(dp.fromExclusive, dp.toInclusive, counterParticipants)
                else FutureUnlessShutdown.pure(Seq.empty)
              received <- persistentState.acsCommitmentStore
                .searchReceivedBetween(dp.fromExclusive, dp.toInclusive, counterParticipants)
                .map(iter => iter.map(rec => rec.message))
              outstanding <- persistentState.acsCommitmentStore
                .outstanding(
                  dp.fromExclusive,
                  dp.toInclusive,
                  counterParticipants,
                  includeMatchedPeriods = true,
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
      .asGrpcResponse

  def outstandingCommitments(
      domain: DomainAlias,
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): Iterable[(CommitmentPeriod, ParticipantId, CommitmentPeriodState)] = {
    val persistentState = getPersistentState(domain)
    timeouts.inspection.awaitUS(s"$functionFullName from $start to $end on $domain")(
      getOrFail(persistentState, domain).acsCommitmentStore
        .outstanding(start, end, counterParticipant.toList)
    )
  }.asGrpcResponse

  def bufferedCommitments(
      domain: DomainAlias,
      endAtOrBefore: CantonTimestamp,
  )(implicit traceContext: TraceContext): Iterable[AcsCommitment] = {
    val persistentState = getPersistentState(domain)
    timeouts.inspection
      .awaitUS(s"$functionFullName to and including $endAtOrBefore on $domain")(
        getOrFail(persistentState, domain).acsCommitmentStore.queue.peekThrough(endAtOrBefore)
      )
      .asGrpcResponse
  }

  def noOutstandingCommitmentsTs(domain: DomainAlias, beforeOrAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Option[CantonTimestamp] = {
    val persistentState = getPersistentState(domain)

    timeouts.inspection.awaitUS(s"$functionFullName on $domain for ts $beforeOrAt")(
      for {
        result <- getOrFail(persistentState, domain).acsCommitmentStore.noOutstandingCommitments(
          beforeOrAt
        )
      } yield result
    )
  }.asGrpcResponse

  def lookupCleanRequestIndex(domain: DomainAlias)(implicit
      traceContext: TraceContext
  ): Either[String, Future[Option[RequestIndex]]] =
    lookupCleanDomainIndex(domain)
      .map(_.map(_.flatMap(_.requestIndex)))

  def lookupCleanDomainIndex(domain: DomainAlias)(implicit
      traceContext: TraceContext
  ): Either[String, Future[Option[DomainIndex]]] =
    getPersistentStateE(domain)
      .map(state =>
        participantNodePersistentState.value.ledgerApiStore
          .cleanDomainIndex(state.indexedDomain.domainId)
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
        _.map(_.offset.unwrap)
      )

  def lookupPublicationTime(
      ledgerOffset: Long
  )(implicit traceContext: TraceContext): EitherT[Future, String, CantonTimestamp] = for {
    offset <- EitherT.fromEither[Future](
      Offset.fromLong(ledgerOffset)
    )
    domainOffset <- EitherT(
      participantNodePersistentState.value.ledgerApiStore
        .domainOffset(offset)
        .map(_.toRight(s"offset $ledgerOffset not found"))
    )
  } yield CantonTimestamp(domainOffset.publicationTime)

  def getConfigsForSlowCounterParticipants()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    (Seq[ConfigForSlowCounterParticipants], Seq[ConfigForDomainThresholds])
  ] =
    participantNodePersistentState.value.acsCounterParticipantConfigStore
      .fetchAllSlowCounterParticipantConfig()

  def getIntervalsBehindForParticipants(domains: Seq[DomainId], participants: Seq[ParticipantId])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Iterable[CounterParticipantIntervalsBehind]] = {
    val result = for {
      (domainId, syncDomain) <-
        syncDomainPersistentStateManager.getAll
          .filter { case (domain, _) => domains.contains(domain) || domains.isEmpty }
    } yield for {
      lastSent <- syncDomain.acsCommitmentStore.lastComputedAndSent(traceContext)

      lastSentFinal = lastSent.fold(CantonTimestamp.MinValue)(_.forgetRefinement)
      outstanding <- syncDomain.acsCommitmentStore
        .outstanding(
          CantonTimestamp.MinValue,
          lastSentFinal,
          participants,
          includeMatchedPeriods = true,
        )

      upToDate <- syncDomain.acsCommitmentStore.searchReceivedBetween(
        lastSentFinal,
        lastSentFinal,
      )

      filteredAllParticipants <- findAllKnownParticipants(domains, participants)
      allParticipantIds = filteredAllParticipants.values.flatten.toSet

      matchedFilteredByYoungest = outstanding
        .filter { case (_, _, state) => state == Matched }
        .groupBy { case (_, participantId, _) => participantId }
        .view
        .mapValues(_.maxByOption { case (period, _, _) => period.fromExclusive })
        .values
        .flatten
        .toSeq

      sortedReconciliationProvider <- EitherTUtil.toFutureUnlessShutdown(
        sortedReconciliationIntervalsProviderFactory
          .get(domainId, lastSentFinal)
          .leftMap(string =>
            new IllegalStateException(
              s"failed to retrieve reconciliationIntervalProvider: $string"
            )
          )
          .mapK(FutureUnlessShutdown.outcomeK)
      )
      oldestOutstandingTimeOption = outstanding
        .filter { case (_, _, state) => state != CommitmentPeriodState.Matched }
        .minByOption { case (period, _, _) =>
          period.toInclusive
        }
      allCoveredTimePeriods <-
        sortedReconciliationProvider
          .computeReconciliationIntervalsCovering(
            oldestOutstandingTimeOption.fold(lastSentFinal) { case (period, _, _) =>
              period.toInclusive.forgetRefinement
            },
            lastSentFinal,
          )
    } yield {
      val newestMatchPerParticipant = matchedFilteredByYoungest
        .filter { case (_, participant, _) =>
          participants.contains(participant) || participants.isEmpty
        }
        .map { case (period, participant, _) =>
          CounterParticipantIntervalsBehind(
            domainId,
            participant,
            NonNegativeLong
              .tryCreate(allCoveredTimePeriods.count(_.toInclusive > period.toInclusive).toLong),
            NonNegativeFiniteDuration
              .tryOfSeconds((lastSentFinal - period.toInclusive.forgetRefinement).getSeconds),
            lastSentFinal,
          )
        }
      // anything that is up to date, or we don't have any commitments would not have an entry in matchedFilteredByYoungest.
      val participantsWithoutMatches = allParticipantIds
        .filter(participantId =>
          !matchedFilteredByYoungest.exists { case (_, outstandingParticipant, _) =>
            outstandingParticipant == participantId
          }
        )
        .map { participantId =>
          val isUpToDate =
            upToDate.exists(signMessage => signMessage.message.sender == participantId)
          CounterParticipantIntervalsBehind(
            domainId,
            participantId,
            if (isUpToDate) NonNegativeLong.zero else NonNegativeLong.maxValue,
            NonNegativeFiniteDuration.tryOfSeconds(Long.MaxValue),
            lastSentFinal,
          )

        }
      newestMatchPerParticipant ++ participantsWithoutMatches
    }
    FutureUnlessShutdown.sequence(result).map(_.flatten)
  }

  def addOrUpdateConfigsForSlowCounterParticipants(
      configs: Seq[ConfigForSlowCounterParticipants],
      thresholds: Seq[ConfigForDomainThresholds],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    participantNodePersistentState.value.acsCounterParticipantConfigStore
      .createOrUpdateCounterParticipantConfigs(configs, thresholds)

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
    participantNodePersistentState.value.ledgerApiStore
      .ledgerEndCache()
      .map(_.lastOffset)
      .flatMap(ledgerEnd =>
        timeouts.inspection.await(s"$functionFullName")(
          participantNodePersistentState.value.ledgerApiStore.lastDomainOffsetBeforeOrAt(
            domainId,
            ledgerEnd,
          )
        )
      )

  def prunedUptoOffset(implicit traceContext: TraceContext): Option[Offset] =
    timeouts.inspection.await(functionFullName)(
      participantNodePersistentState.value.pruningStore.pruningStatus().map(_.completedO)
    )

  def getSequencerChannelClient(domainId: DomainId): Option[SequencerChannelClient] = for {
    syncDomain <- connectedDomainsLookup.get(domainId)
    sequencerChannelClient <- syncDomain.domainHandle.sequencerChannelClientO
  } yield sequencerChannelClient

  def getAcsInspection(domainId: DomainId): Option[AcsInspection] =
    connectedDomainsLookup.get(domainId).map(_.domainHandle.domainPersistentState.acsInspection)

  def findAllKnownParticipants(
      domainFilter: Seq[DomainId] = Seq.empty,
      participantFilter: Seq[ParticipantId] = Seq.empty,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[DomainId, Set[ParticipantId]]] = {
    val result = for {
      (domainId, _) <-
        syncDomainPersistentStateManager.getAll.filter { case (domainId, _) =>
          domainFilter.contains(domainId) || domainFilter.isEmpty
        }
    } yield for {
      _ <- FutureUnlessShutdown.unit
      domainTopoClient = syncCrypto.ips.tryForDomain(domainId)
      ipsSnapshot <- domainTopoClient.awaitSnapshotUS(domainTopoClient.approximateTimestamp)
      allMembers <- ipsSnapshot.allMembers()
      allParticipants = allMembers
        .filter(_.code == ParticipantId.Code)
        .map(member => ParticipantId.apply(member.uid))
        .excl(participantId)
        .filter(participantFilter.contains(_) || participantFilter.isEmpty)
    } yield (domainId, allParticipants)

    FutureUnlessShutdown.sequence(result).map(_.toMap)
  }

  def cleanSequencedEventStoreAboveCleanDomainIndex(domainAlias: DomainAlias)(implicit
      traceContext: TraceContext
  ): Unit =
    getOrFail(
      getPersistentState(domainAlias).map { domainState =>
        timeouts.inspection.await(functionFullName)(
          participantNodePersistentState.value.ledgerApiStore
            .cleanDomainIndex(domainState.indexedDomain.domainId)
            .flatMap { domainIndexO =>
              val nextSequencerCounter = domainIndexO
                .flatMap(_.sequencerIndex)
                .map(
                  _.counter.increment
                    .getOrElse(
                      throw new IllegalStateException("sequencer counter cannot be increased")
                    )
                )
                .getOrElse(SequencerCounter.Genesis)
              logger.info(
                s"Deleting events from SequencedEventStore for domain $domainAlias fromInclusive $nextSequencerCounter"
              )
              domainState.sequencedEventStore.delete(nextSequencerCounter)
            }
        )
      },
      domainAlias,
    )
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
