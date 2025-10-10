// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.inspection

import cats.Eval
import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong}
import com.digitalasset.canton.crypto.SyncCryptoApiParticipantProvider
import com.digitalasset.canton.data.{
  BufferedAcsCommitment,
  CantonTimestamp,
  CantonTimestampSecond,
  Offset,
}
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.pretty.PrettyPrinting
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcUSExtended
import com.digitalasset.canton.participant.admin.data.ActiveContractOld
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection.{
  InFlightCount,
  SyncStateInspectionError,
}
import com.digitalasset.canton.participant.protocol.RequestJournal
import com.digitalasset.canton.participant.pruning.SortedReconciliationIntervalsProviderFactory
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.ActiveContractStore.ActivenessChangeDetail
import com.digitalasset.canton.participant.sync.{
  ConnectedSynchronizersLookup,
  SyncPersistentStateManager,
}
import com.digitalasset.canton.participant.util.{TimeOfChange, TimeOfRequest}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SynchronizerOffset
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.CommitmentPeriodState.{
  Matched,
  fromIntValidSentPeriodState,
}
import com.digitalasset.canton.protocol.{
  ContractInstance,
  LfContractId,
  ReassignmentId,
  SerializableContract,
}
import com.digitalasset.canton.pruning.{
  ConfigForSlowCounterParticipants,
  ConfigForSynchronizerThresholds,
  CounterParticipantIntervalsBehind,
  PruningStatus,
}
import com.digitalasset.canton.sequencing.PossiblyIgnoredProtocolEvent
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelClient
import com.digitalasset.canton.store.SequencedEventStore.{
  ByTimestamp,
  ByTimestampRange,
  PossiblyIgnoredSequencedEvent,
}
import com.digitalasset.canton.store.{SequencedEventRangeOverlapsWithPruning, SequencedEventStore}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.OptionUtils.OptionExtension
import com.digitalasset.canton.util.ReassignmentTag.Source
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  LfPartyId,
  ReassignmentCounter,
  RequestCounter,
  SequencerCounter,
  SynchronizerAlias,
}
import com.google.common.annotations.VisibleForTesting

import java.io.OutputStream
import java.time.Instant
import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}

trait JournalGarbageCollectorControl {
  def disable(synchronizerId: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext
  ): Future[Unit]
  def enable(synchronizerId: PhysicalSynchronizerId)(implicit traceContext: TraceContext): Unit
}

object JournalGarbageCollectorControl {
  object NoOp extends JournalGarbageCollectorControl {
    override def disable(synchronizerId: PhysicalSynchronizerId)(implicit
        traceContext: TraceContext
    ): Future[Unit] =
      Future.unit
    override def enable(synchronizerId: PhysicalSynchronizerId)(implicit
        traceContext: TraceContext
    ): Unit =
      ()
  }
}

/** Implements inspection functions for the sync state of a participant node */
final class SyncStateInspection(
    val syncPersistentStateManager: SyncPersistentStateManager,
    participantNodePersistentState: Eval[ParticipantNodePersistentState],
    synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    timeouts: ProcessingTimeout,
    journalCleaningControl: JournalGarbageCollectorControl,
    connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    syncCrypto: SyncCryptoApiParticipantProvider,
    participantId: ParticipantId,
    futureSupervisor: FutureSupervisor,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  import SyncStateInspection.getOrFail

  private lazy val sortedReconciliationIntervalsProviderFactory =
    new SortedReconciliationIntervalsProviderFactory(
      syncPersistentStateManager,
      futureSupervisor,
      loggerFactory,
    )

  /** Look up all unpruned state changes of a set of contracts on all synchronizers. If a contract
    * is not found in an available ACS it will be omitted from the response.
    */
  def lookupContractSynchronizers(
      contractIds: Set[LfContractId]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Map[SynchronizerId, SortedMap[LfContractId, Seq[(CantonTimestamp, ActivenessChangeDetail)]]]
  ] =
    syncPersistentStateManager.getAllLatest.toList
      .map { case (id, state) =>
        state.activeContractStore.activenessOf(contractIds.toSeq).map(s => id -> s)
      }
      .sequence
      .map(_.toMap)

  /** Returns the potentially large ACS of a given synchronizer containing a map of contract IDs to
    * tuples containing the latest activation timestamp and the contract reassignment counter
    */
  def findAcs(
      synchronizerAlias: SynchronizerAlias
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncStateInspectionError, Map[
    LfContractId,
    (TimeOfChange, ReassignmentCounter),
  ]] =
    for {
      acsInspection <- EitherT.fromEither[FutureUnlessShutdown](
        syncPersistentStateManager
          .acsInspection(synchronizerAlias)
          .toRight(SyncStateInspection.NoSuchSynchronizer(synchronizerAlias))
      )

      snapshotO <- EitherT.right(acsInspection.getCurrentSnapshot().map(_.map(_.snapshot)))
    } yield snapshotO.fold(Map.empty[LfContractId, (TimeOfChange, ReassignmentCounter)])(
      _.toMap
    )

  /** searches the pcs and returns the contract and activeness flag */
  def findContracts(
      synchronizerAlias: SynchronizerAlias,
      exactId: Option[String],
      filterPackage: Option[String],
      filterTemplate: Option[String],
      limit: Int,
  )(implicit traceContext: TraceContext): List[(Boolean, ContractInstance)] =
    getOrFail(
      timeouts.inspection.await("findContracts") {
        syncPersistentStateManager
          .acsInspection(synchronizerAlias)
          .traverse(_.findContracts(exactId, filterPackage, filterTemplate, limit))
          .failOnShutdownToAbortException("findContracts")
      },
      synchronizerAlias,
    )

  def findContractPayloads(
      synchronizerId: SynchronizerId,
      contractIds: Seq[LfContractId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, ContractInstance]] = {
    val synchronizerAlias = syncPersistentStateManager
      .aliasForSynchronizerId(synchronizerId)
      .getOrElse(throw new IllegalArgumentException(s"no such synchronizer [$synchronizerId]"))

    NonEmpty.from(contractIds) match {
      case None =>
        FutureUnlessShutdown.pure(Map.empty[LfContractId, ContractInstance])
      case Some(neCids) =>
        val synchronizerAcsInspection =
          getOrFail(
            syncPersistentStateManager.acsInspection(synchronizerAlias),
            synchronizerAlias,
          )

        synchronizerAcsInspection.findContractPayloads(neCids)
    }
  }

  def lookupReassignmentIds(
      targetSynchronizerId: SynchronizerId,
      sourceSynchronizerId: SynchronizerId,
      contractIds: Seq[LfContractId],
      minUnassignmentTs: Option[CantonTimestamp] = None,
      minCompletionTs: Option[CantonTimestamp] = None,
  )(implicit traceContext: TraceContext): Map[LfContractId, Seq[ReassignmentId]] =
    timeouts.inspection
      .awaitUS(functionFullName) {
        syncPersistentStateManager
          .reassignmentStore(targetSynchronizerId)
          .traverse(
            _.findContractReassignmentId(
              contractIds,
              Some(Source(sourceSynchronizerId)),
              minUnassignmentTs,
              minCompletionTs,
            )
          )
      }
      .asGrpcResponse
      .getOrElse(
        throw new IllegalArgumentException(s"no such synchronizer [$targetSynchronizerId]")
      )

  @VisibleForTesting
  def acsPruningStatus(
      synchronizerId: SynchronizerId
  )(implicit traceContext: TraceContext): Option[PruningStatus] =
    timeouts.inspection
      .awaitUS("acsPruningStatus")(
        getOrFail(
          getAcsInspection(synchronizerId),
          synchronizerId,
        ).activeContractStore.pruningStatus
      )
      .asGrpcResponse

  private def disableJournalCleaningForFilter(
      synchronizers: Map[PhysicalSynchronizerId, SyncPersistentState],
      filterSynchronizerId: SynchronizerId => Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsInspectionError, Unit] = {
    val disabledCleaningF = Future
      .sequence(synchronizers.collect {
        case (synchronizerId, _) if filterSynchronizerId(synchronizerId.logical) =>
          journalCleaningControl.disable(synchronizerId)
      })
      .map(_ => ())
    EitherT.right(disabledCleaningF)
  }

  def exportAcsDumpActiveContracts(
      outputStream: OutputStream,
      filterSynchronizerId: SynchronizerId => Boolean,
      parties: Set[LfPartyId],
      timestamp: Option[CantonTimestamp],
      skipCleanTimestampCheck: Boolean,
      partiesOffboarding: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, AcsInspectionError, Unit] = {
    // To disable/re-enable background pruning
    val allSynchronizers: Map[PhysicalSynchronizerId, SyncPersistentState] =
      syncPersistentStateManager.getAll

    // For the ACS export
    val latestSynchronizers = syncPersistentStateManager.getAllLatest

    def writeACSToStream(synchronizerId: SynchronizerId, state: SyncPersistentState) = {
      val pv = state.staticSynchronizerParameters.protocolVersion

      val acsInspection = state.acsInspection
      val timeOfSnapshotO = timestamp.map(TimeOfChange.apply)
      for {
        result <- acsInspection
          .forEachVisibleActiveContract(
            synchronizerId.logical,
            parties,
            timeOfSnapshotO,
            skipCleanTocCheck = skipCleanTimestampCheck,
          ) { case (contractInst, reassignmentCounter) =>
            (for {
              contract <- SerializableContract.fromLfFatContractInst(contractInst.inst)
              activeContract = ActiveContractOld.create(
                synchronizerId,
                contract,
                reassignmentCounter,
              )(pv)

              _ <- activeContract.writeDelimitedTo(outputStream)
            } yield ()) match {
              case Left(errorMessage) =>
                Left(
                  AcsInspectionError.SerializationIssue(
                    synchronizerId.logical,
                    contractInst.contractId,
                    errorMessage,
                  )
                )
              case Right(_) =>
                outputStream.flush()
                Either.unit
            }
          }

        _ <- result match {
          case Some((allStakeholders, snapshotToc)) if partiesOffboarding =>
            for {
              connectedSynchronizer <- EitherT.fromOption[FutureUnlessShutdown](
                connectedSynchronizersLookup.get(synchronizerId),
                AcsInspectionError.OffboardingParty(
                  synchronizerId.logical,
                  s"Unable to get topology client for synchronizer $synchronizerId; check synchronizer connectivity.",
                ),
              )

              _ <- acsInspection.checkOffboardingSnapshot(
                participantId,
                offboardedParties = parties,
                allStakeholders = allStakeholders,
                snapshotToc = snapshotToc,
                topologyClient = connectedSynchronizer.topologyClient,
              )
            } yield ()

          // Snapshot is empty or partiesOffboarding is false
          case _ => EitherTUtil.unitUS[AcsInspectionError]
        }
      } yield ()
    }

    // disable journal cleaning for the duration of the dump
    val res: EitherT[FutureUnlessShutdown, AcsInspectionError, Unit] =
      disableJournalCleaningForFilter(allSynchronizers, filterSynchronizerId)
        .mapK(FutureUnlessShutdown.outcomeK)
        .flatMap { _ =>
          MonadUtil.sequentialTraverse_(latestSynchronizers) {
            case (synchronizerId, state) if filterSynchronizerId(synchronizerId) =>
              writeACSToStream(synchronizerId, state)
            case _ =>
              EitherTUtil.unitUS
          }
        }

    // re-enable journal cleaning after the dump
    res.thereafter { _ =>
      allSynchronizers.keys.foreach(journalCleaningControl.enable)
    }
  }

  def contractCount(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] =
    participantNodePersistentState.value.contractStore.contractCount()

  def contractCountInAcs(synchronizerAlias: SynchronizerAlias, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Int]] = syncPersistentStateManager
    .activeContractStore(synchronizerAlias)
    .traverse(_.contractCount(timestamp))

  def requestJournalSize(
      psid: PhysicalSynchronizerId,
      start: CantonTimestamp = CantonTimestamp.Epoch,
      end: Option[CantonTimestamp] = None,
  )(implicit traceContext: TraceContext): Option[UnlessShutdown[Int]] =
    getPersistentState(psid).toOption.map { state =>
      timeouts.inspection.awaitUS(
        s"$functionFullName from $start to $end from the journal of synchronizer $psid"
      )(
        state.requestJournalStore.size(start, end)
      )
    }

  def activeContractsStakeholdersFilter(
      psid: PhysicalSynchronizerId,
      toc: TimeOfChange,
      parties: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[(ContractInstance, ReassignmentCounter)]] =
    for {
      state <- FutureUnlessShutdown.fromTry(
        syncPersistentStateManager
          .get(psid)
          .toTry(
            new IllegalArgumentException(s"Unable to find contract store for $psid.")
          )
      )

      snapshot <- state.activeContractStore.snapshot(toc)

      // check that the active contract store has not been pruned up to timestamp, otherwise the snapshot is inconsistent.
      pruningStatus <- state.activeContractStore.pruningStatus
      _ <-
        if (pruningStatus.exists(_.timestamp > toc.timestamp)) {
          FutureUnlessShutdown.failed(
            new IllegalStateException(
              s"Active contract store for synchronizer $psid has been pruned up to ${pruningStatus
                  .map(_.lastSuccess)}, which is after the requested time of change $toc"
            )
          )
        } else FutureUnlessShutdown.unit

      contracts <-
        participantNodePersistentState.value.contractStore
          .lookupManyExistingUncached(snapshot.keys.toSeq)
          .valueOr { missingContractId =>
            ErrorUtil.invalidState(
              s"Contract id $missingContractId is in the active contract store but its contents is not in the contract store"
            )
          }

      contractsWithReassignmentCounter = contracts.map(c => c -> snapshot(c.contractId)._2)

      filteredByParty = contractsWithReassignmentCounter.collect {
        case (contract, reassignmentCounter) if parties.intersect(contract.stakeholders).nonEmpty =>
          (contract, reassignmentCounter)
      }
    } yield filteredByParty.toSet

  def latestKnownProtocolVersion(
      synchronizerAlias: SynchronizerAlias
  ): Option[ProtocolVersion] =
    for {
      id <- syncPersistentStateManager.synchronizerIdForAlias(synchronizerAlias)
      pv <- syncPersistentStateManager.latestKnownProtocolVersion(id)
    } yield pv

  def findMessages(
      psid: PhysicalSynchronizerId,
      from: Option[Instant],
      to: Option[Instant],
      limit: Option[Int],
      warnOnDiscardedEnvelopes: Boolean,
  )(implicit traceContext: TraceContext): Seq[PossiblyIgnoredProtocolEvent] = {
    val state = getOrFail(syncPersistentStateManager.get(psid), psid)
    val messagesF =
      if (from.isEmpty && to.isEmpty)
        state.sequencedEventStore.sequencedEvents(limit)
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
        .awaitUS(s"finding messages from $from to $to on $psid")(messagesF)
        .asGrpcResponse
    closed.map { closedEvent =>
      val openWithErrors = PossiblyIgnoredSequencedEvent.openEnvelopes(closedEvent)(
        psid.protocolVersion,
        state.pureCryptoApi,
      )
      if (warnOnDiscardedEnvelopes && openWithErrors.openingErrors.nonEmpty) {
        logger.warn(s"Discarding envelopes with errors: ${openWithErrors.openingErrors}")
      }
      openWithErrors.event
    }
  }

  @VisibleForTesting
  def findMessage(
      psid: PhysicalSynchronizerId,
      criterion: SequencedEventStore.SearchCriterion,
  )(implicit
      traceContext: TraceContext
  ): Option[PossiblyIgnoredProtocolEvent] = {
    val state = getOrFail(syncPersistentStateManager.get(psid), psid)
    val messageF = state.sequencedEventStore.find(criterion).value
    val closed =
      timeouts.inspection
        .awaitUS(s"$functionFullName on $psid matching $criterion")(messageF)
    closed match {
      case UnlessShutdown.Outcome(result) =>
        result.toOption.map(
          PossiblyIgnoredSequencedEvent
            .openEnvelopes(_)(
              psid.protocolVersion,
              state.pureCryptoApi,
            )
            .event
        )
      case UnlessShutdown.AbortedDueToShutdown => None
    }
  }

  def findComputedCommitments(
      synchronizerAlias: SynchronizerAlias,
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId] = None,
  )(implicit
      traceContext: TraceContext
  ): Iterable[(CommitmentPeriod, ParticipantId, AcsCommitment.HashedCommitmentType)] =
    timeouts.inspection
      .awaitUS(s"$functionFullName from $start to $end on $synchronizerAlias")(
        getOrFail(getAcsCommitmentStore(synchronizerAlias), synchronizerAlias)
          .searchComputedBetween(start, end, counterParticipant.map(NonEmpty.mk(Seq, _)))
      )
      .asGrpcResponse

  def findLastComputedAndSent(
      synchronizerAlias: SynchronizerAlias
  )(implicit traceContext: TraceContext): Option[CantonTimestampSecond] =
    timeouts.inspection
      .awaitUS(s"$functionFullName on $synchronizerAlias")(
        getOrFail(
          getAcsCommitmentStore(synchronizerAlias),
          synchronizerAlias,
        ).lastComputedAndSent
      )
      .asGrpcResponse

  def findReceivedCommitments(
      synchronizerAlias: SynchronizerAlias,
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId] = None,
  )(implicit traceContext: TraceContext): Iterable[SignedProtocolMessage[AcsCommitment]] =
    timeouts.inspection
      .awaitUS(s"$functionFullName from $start to $end on $synchronizerAlias")(
        getOrFail(getAcsCommitmentStore(synchronizerAlias), synchronizerAlias)
          .searchReceivedBetween(start, end, counterParticipant.map(NonEmpty.mk(Seq, _)))
      )
      .asGrpcResponse

  def crossSynchronizerSentCommitmentMessages(
      synchronizerPeriods: Seq[SynchronizerSearchCommitmentPeriod],
      counterParticipantsFilter: Option[NonEmpty[Seq[ParticipantId]]],
      states: Seq[CommitmentPeriodState],
      verbose: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Iterable[SentAcsCommitment]] =
    timeouts.inspection
      .awaitUS(functionFullName) {
        val searchResult = synchronizerPeriods.map { dp =>
          for {
            acsCommitmentStore <- getAcsCommitmentStore(dp.indexedSynchronizer.synchronizerId)
              .toRight(
                s"No ACS commitment store found for ${dp.indexedSynchronizer.synchronizerId}"
              )

            result = for {
              computed <- acsCommitmentStore
                .searchComputedBetween(dp.fromExclusive, dp.toInclusive, counterParticipantsFilter)
              received <-
                if (verbose)
                  acsCommitmentStore
                    .searchReceivedBetween(
                      dp.fromExclusive,
                      dp.toInclusive,
                      counterParticipantsFilter,
                    )
                    .map(iter => iter.map(rec => rec.message))
                else FutureUnlessShutdown.pure(Seq.empty)
              outstanding <- acsCommitmentStore
                .outstanding(
                  dp.fromExclusive,
                  dp.toInclusive,
                  counterParticipantsFilter,
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
              .compare(
                dp.indexedSynchronizer.synchronizerId,
                computed,
                received,
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
            FutureUnlessShutdown
              .sequence(rights)
              .map(seqSentAcsCommitments => Right(seqSentAcsCommitments.flatten))
        }
      }
      .asGrpcResponse

  def crossSynchronizerReceivedCommitmentMessages(
      synchronizerPeriods: Seq[SynchronizerSearchCommitmentPeriod],
      counterParticipantsFilter: Option[NonEmpty[Seq[ParticipantId]]],
      states: Seq[CommitmentPeriodState],
      verbose: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Iterable[ReceivedAcsCommitment]] =
    timeouts.inspection
      .awaitUS(functionFullName) {
        val searchResult = synchronizerPeriods.map { dp =>
          for {
            acsCommitmentStore <- getAcsCommitmentStore(dp.indexedSynchronizer.synchronizerId)
              .toRight(s"No acs commitment store for ${dp.indexedSynchronizer.synchronizerId}")

            result = for {
              computed <-
                if (verbose)
                  acsCommitmentStore
                    .searchComputedBetween(
                      dp.fromExclusive,
                      dp.toInclusive,
                      counterParticipantsFilter,
                    )
                else FutureUnlessShutdown.pure(Seq.empty)
              received <- acsCommitmentStore
                .searchReceivedBetween(dp.fromExclusive, dp.toInclusive, counterParticipantsFilter)
                .map(iter => iter.map(rec => rec.message))
              outstanding <- acsCommitmentStore.outstanding(
                dp.fromExclusive,
                dp.toInclusive,
                counterParticipantsFilter,
                includeMatchedPeriods = true,
              )

              buffered <- acsCommitmentStore.queue
                .peekThrough(dp.toInclusive) // peekThrough takes an upper bound parameter
                .map(iter =>
                  iter.filter(cmt =>
                    cmt.period.fromExclusive >= dp.fromExclusive && cmt.synchronizerId == dp.indexedSynchronizer.synchronizerId && (counterParticipantsFilter
                      .fold(true)(_.contains(cmt.sender)))
                  )
                )
            } yield ReceivedAcsCommitment
              .compare(
                dp.indexedSynchronizer.synchronizerId,
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
      synchronizerAlias: SynchronizerAlias,
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): Iterable[(CommitmentPeriod, ParticipantId, CommitmentPeriodState)] =
    timeouts.inspection
      .awaitUS(s"$functionFullName from $start to $end on $synchronizerAlias")(
        getOrFail(getAcsCommitmentStore(synchronizerAlias), synchronizerAlias)
          .outstanding(start, end, counterParticipant.map(NonEmpty.mk(Seq, _)))
      )
      .asGrpcResponse

  def bufferedCommitments(
      synchronizerAlias: SynchronizerAlias,
      endAtOrBefore: CantonTimestamp,
  )(implicit traceContext: TraceContext): Iterable[BufferedAcsCommitment] =
    timeouts.inspection
      .awaitUS(s"$functionFullName to and including $endAtOrBefore on $synchronizerAlias")(
        getOrFail(getAcsCommitmentStore(synchronizerAlias), synchronizerAlias).queue
          .peekThrough(endAtOrBefore)
      )
      .asGrpcResponse

  def noOutstandingCommitmentsTs(synchronizerAlias: SynchronizerAlias, beforeOrAt: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): Option[CantonTimestamp] =
    timeouts.inspection
      .awaitUS(s"$functionFullName on $synchronizerAlias for ts $beforeOrAt")(
        getOrFail(getAcsCommitmentStore(synchronizerAlias), synchronizerAlias)
          .noOutstandingCommitments(beforeOrAt)
      )
      .asGrpcResponse

  /** Returns the last clean time of request of the specified synchronizer from the indexer's point
    * of view.
    */
  @VisibleForTesting
  def lookupCleanTimeOfRequest(psid: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext
  ): Either[String, FutureUnlessShutdown[Option[TimeOfRequest]]] =
    getPersistentState(psid)
      .map(state =>
        (for {
          cleanTs <- OptionT(
            participantNodePersistentState.value.ledgerApiStore
              .cleanSynchronizerIndex(state.synchronizerIdx.synchronizerId)
              .map(_.map(_.recordTime))
          )
          cleanTimeOfRequest <- OptionT(
            state.requestJournalStore.lastRequestTimeWithRequestTimestampBeforeOrAt(cleanTs)
          )
        } yield cleanTimeOfRequest).value
      )

  def lookupCleanSynchronizerIndex(synchronizerAlias: SynchronizerAlias)(implicit
      traceContext: TraceContext
  ): Either[String, FutureUnlessShutdown[Option[SynchronizerIndex]]] = syncPersistentStateManager
    .getLatest(synchronizerAlias)
    .toRight(s"Unable to find persistent state for $synchronizerAlias")
    .map { state =>
      participantNodePersistentState.value.ledgerApiStore
        .cleanSynchronizerIndex(state.synchronizerIdx.synchronizerId)
    }

  def lookupCleanSequencerCounter(psid: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext
  ): Either[String, FutureUnlessShutdown[Option[SequencerCounter]]] =
    getPersistentState(psid)
      .map(state =>
        participantNodePersistentState.value.ledgerApiStore
          .cleanSynchronizerIndex(state.synchronizerIdx.synchronizerId)
          .flatMap(
            _.flatMap(_.sequencerIndex)
              .traverse(sequencerIndex =>
                state.sequencedEventStore
                  .find(ByTimestamp(sequencerIndex.sequencerTimestamp))
                  .value
                  .map(
                    _.getOrElse(
                      ErrorUtil.invalidState(
                        s"SequencerIndex with timestamp ${sequencerIndex.sequencerTimestamp} is not found in sequenced event store"
                      )
                    ).counter
                  )
              )
          )
      )

  def requestStateInJournal(rc: RequestCounter, psid: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext
  ): Either[String, FutureUnlessShutdown[Option[RequestJournal.RequestData]]] =
    getPersistentState(psid).map(_.requestJournalStore.query(rc).value)

  private[this] def getPersistentState(
      psid: PhysicalSynchronizerId
  ): Either[String, SyncPersistentState] =
    syncPersistentStateManager.get(psid).toRight(s"no such synchronizer [$psid]")

  def getOffsetByTime(
      pruneUpTo: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[Long]] =
    participantNodePersistentState.value.ledgerApiStore
      .lastSynchronizerOffsetBeforeOrAtPublicationTime(pruneUpTo)
      .map(
        _.map(_.offset.unwrap)
      )

  def lookupPublicationTime(
      ledgerOffset: Long
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, CantonTimestamp] =
    for {
      offset <- EitherT.fromEither[FutureUnlessShutdown](
        Offset.fromLong(ledgerOffset)
      )
      synchronizerOffset <- EitherT(
        participantNodePersistentState.value.ledgerApiStore
          .synchronizerOffset(offset)
          .map(_.toRight(s"offset $ledgerOffset not found"))
      )
    } yield CantonTimestamp(synchronizerOffset.publicationTime)

  def getConfigsForSlowCounterParticipants()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    (Seq[ConfigForSlowCounterParticipants], Seq[ConfigForSynchronizerThresholds])
  ] =
    participantNodePersistentState.value.acsCounterParticipantConfigStore
      .fetchAllSlowCounterParticipantConfig()

  def getIntervalsBehindForParticipants(
      synchronizersFilter: Option[NonEmpty[Seq[SynchronizerId]]],
      participantsFilter: Option[NonEmpty[Seq[ParticipantId]]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Iterable[CounterParticipantIntervalsBehind]] = {

    /*
      We restrict here the query to the persistent states to the latest per logical id.
      This works because we use a persistent state for:

      - ACS commitment store, which is logical
      - Querying the reconciliation intervals over time. Such a list is only growing with each new physical instance.
     */
    val filteredStates = synchronizersFilter match {
      case Some(synchronizers) =>
        syncPersistentStateManager.getAllLatest.values.filter { state =>
          synchronizers.contains(state.lsid)
        }

      case None => syncPersistentStateManager.getAllLatest.values
    }

    for {
      allKnownParticipants <- findAllKnownParticipants(synchronizersFilter, participantsFilter).map(
        _.view.mapValues(_.excl(participantId)).toMap
      )

      result <- MonadUtil.sequentialTraverse(filteredStates.toSeq)(
        getIntervalsBehindForParticipants(allKnownParticipants, participantsFilter, _)
      )
    } yield result.flatten
  }

  /** @param filteredKnownParticipants
    *   All known participants except the local one
    * @param participantsFilter
    *   Optional filter on the participants
    * @return
    */
  private def getIntervalsBehindForParticipants(
      filteredKnownParticipants: Map[PhysicalSynchronizerId, Set[ParticipantId]],
      participantsFilter: Option[NonEmpty[Seq[ParticipantId]]],
      syncPersistentState: SyncPersistentState,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[CounterParticipantIntervalsBehind]] = {
    val synchronizerId = syncPersistentState.lsid

    for {
      lastSent <- syncPersistentState.acsCommitmentStore.lastComputedAndSent(traceContext)

      lastSentFinal = lastSent.fold(CantonTimestamp.MinValue)(_.forgetRefinement)
      outstanding <- syncPersistentState.acsCommitmentStore
        .outstanding(
          CantonTimestamp.MinValue,
          lastSentFinal,
          participantsFilter,
          includeMatchedPeriods = true,
        )

      upToDate <- syncPersistentState.acsCommitmentStore.searchReceivedBetween(
        lastSentFinal,
        lastSentFinal,
      )

      allParticipantIds = filteredKnownParticipants.values.flatten.toSet

      matchedFilteredByYoungest = outstanding
        .filter { case (_, _, state) => state == Matched }
        .groupBy { case (_, participantId, _) => participantId }
        .view
        .mapValues(_.maxByOption { case (period, _, _) => period.fromExclusive })
        .values
        .flatten
        .toSeq

      synchronizerPredecessor <- synchronizerConnectionConfigStore
        .get(syncPersistentState.psid)
        .map(_.predecessor)
        .bimap(
          err =>
            FutureUnlessShutdown.failed(
              new IllegalStateException(
                s"Failed to retrieve configuration for ${syncPersistentState.psid}: $err"
              )
            ),
          FutureUnlessShutdown.pure,
        )
        .merge

      sortedReconciliationProvider <- EitherTUtil.toFutureUnlessShutdown(
        sortedReconciliationIntervalsProviderFactory
          .get(syncPersistentState.psid, lastSentFinal, synchronizerPredecessor)
          .leftMap(string =>
            new IllegalStateException(
              s"failed to retrieve reconciliationIntervalProvider: $string"
            )
          )
      )

      oldestOutstandingTimeOption = outstanding
        .filter { case (_, _, state) => state != CommitmentPeriodState.Matched }
        .minByOption { case (period, _, _) => period.toInclusive }

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
          participantsFilter.fold(true)(_.contains(participant))
        }
        .map { case (period, participant, _) =>
          CounterParticipantIntervalsBehind(
            synchronizerId,
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
            synchronizerId,
            participantId,
            if (isUpToDate) NonNegativeLong.zero else NonNegativeLong.maxValue,
            NonNegativeFiniteDuration.tryOfSeconds(Long.MaxValue),
            lastSentFinal,
          )

        }
      newestMatchPerParticipant ++ participantsWithoutMatches
    }
  }

  def addOrUpdateConfigsForSlowCounterParticipants(
      configs: Seq[ConfigForSlowCounterParticipants],
      thresholds: Seq[ConfigForSynchronizerThresholds],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    participantNodePersistentState.value.acsCounterParticipantConfigStore
      .createOrUpdateCounterParticipantConfigs(configs, thresholds)

  def countInFlight(
      psid: PhysicalSynchronizerId
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, InFlightCount] =
    for {
      state <- EitherT.fromEither[FutureUnlessShutdown](getPersistentState(psid))
      unsequencedSubmissions <- EitherT.right(
        participantNodePersistentState.value.inFlightSubmissionStore
          .lookupUnsequencedUptoUnordered(psid.logical, CantonTimestamp.now())
      )
      pendingSubmissions = NonNegativeInt.tryCreate(unsequencedSubmissions.size)
      pendingTransactions <- EitherT.right[String](state.requestJournalStore.totalDirtyRequests())
    } yield {
      InFlightCount(pendingSubmissions, pendingTransactions)
    }

  @VisibleForTesting
  def verifyLapiStoreIntegrity()(implicit traceContext: TraceContext): Unit =
    timeouts.inspection
      .awaitUS(functionFullName)(
        participantNodePersistentState.value.ledgerApiStore.verifyIntegrity()
      )
      .onShutdown(throw new RuntimeException("verifyLapiStoreIntegrity"))

  @VisibleForTesting
  def acceptedTransactionCount(
      synchronizerAlias: SynchronizerAlias
  )(implicit traceContext: TraceContext): Int =
    syncPersistentStateManager
      .getLatest(synchronizerAlias)
      .map(synchronizerPersistentState =>
        timeouts.inspection
          .awaitUS(functionFullName)(
            participantNodePersistentState.value.ledgerApiStore
              .numberOfAcceptedTransactionsFor(
                synchronizerPersistentState.synchronizerIdx.synchronizerId
              )
          )
          .onShutdown(0)
      )
      .getOrElse(0)

  @VisibleForTesting
  def moveLedgerEndBackToScratch()(implicit traceContext: TraceContext): Unit =
    timeouts.inspection
      .awaitUS(functionFullName)(
        participantNodePersistentState.value.ledgerApiStore
          .moveLedgerEndBackToScratch()
      )
      .onShutdown(throw new RuntimeException("onlyForTestingMoveLedgerEndBackToScratch"))

  def lastSynchronizerOffset(
      synchronizerId: SynchronizerId
  )(implicit traceContext: TraceContext): Option[SynchronizerOffset] =
    participantNodePersistentState.value.ledgerApiStore
      .ledgerEndCache()
      .map(_.lastOffset)
      .flatMap(ledgerEnd =>
        timeouts.inspection
          .awaitUS(s"$functionFullName")(
            participantNodePersistentState.value.ledgerApiStore.lastSynchronizerOffsetBeforeOrAt(
              synchronizerId,
              ledgerEnd,
            )
          )
          .onShutdown(None)
      )

  def prunedUptoOffset(implicit traceContext: TraceContext): Option[Offset] =
    timeouts.inspection
      .awaitUS(functionFullName)(
        participantNodePersistentState.value.pruningStore.pruningStatus().map(_.completedO)
      )
      .onShutdown(None)

  def getSequencerChannelClient(
      synchronizerId: PhysicalSynchronizerId
  ): Option[SequencerChannelClient] =
    for {
      connectedSynchronizer <- connectedSynchronizersLookup.get(synchronizerId)
      sequencerChannelClient <- connectedSynchronizer.synchronizerHandle.sequencerChannelClientO
    } yield sequencerChannelClient

  def getAcsInspection(synchronizerId: SynchronizerId): Option[AcsInspection] =
    syncPersistentStateManager.acsInspection(synchronizerId)

  def getAcsCommitmentStore(synchronizerAlias: SynchronizerAlias): Option[AcsCommitmentStore] =
    syncPersistentStateManager.acsCommitmentStore(synchronizerAlias)

  def getAcsCommitmentStore(synchronizerId: SynchronizerId): Option[AcsCommitmentStore] =
    syncPersistentStateManager.acsCommitmentStore(synchronizerId)

  /** Return the list of all known participants. If several physical synchronizer are known for a
    * given [[com.digitalasset.canton.topology.SynchronizerId]], only the latest one is considered
    */
  def findAllKnownParticipants(
      synchronizerFilter: Option[NonEmpty[Seq[SynchronizerId]]],
      participantFilter: Option[NonEmpty[Seq[ParticipantId]]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PhysicalSynchronizerId, Set[ParticipantId]]] = {
    val filteredSynchronizerIds = syncPersistentStateManager.getAllLatest.toSeq
      .mapFilter { case (synchronizerId, state) =>
        Option.when(synchronizerFilter.fold(true)(_.contains(synchronizerId)))(
          state.psid
        )
      }

    MonadUtil
      .sequentialTraverse(filteredSynchronizerIds) { synchronizerId =>
        val synchronizerTopoClient = syncCrypto.ips.tryForSynchronizer(synchronizerId)
        val ipsSnapshot = synchronizerTopoClient.currentSnapshotApproximation

        ipsSnapshot
          .allMembers()
          .map(
            _.collect {
              case id: ParticipantId if participantFilter.fold(true)(_.contains(id)) => id
            }
          )
          .map(synchronizerId -> _)
      }
      .map(_.toMap)
  }

  @VisibleForTesting
  def cleanSequencedEventStoreAboveCleanSynchronizerIndex(psid: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext
  ): Unit = {

    val persistentState = getOrFail(getPersistentState(psid).toOption, psid)

    val sequencerCounterF: FutureUnlessShutdown[Option[SequencerCounter]] =
      participantNodePersistentState.value.ledgerApiStore
        .cleanSynchronizerIndex(persistentState.synchronizerIdx.synchronizerId)
        .flatMap(
          _.flatMap(_.sequencerIndex)
            .traverse(sequencerIndex =>
              persistentState.sequencedEventStore
                .find(ByTimestamp(sequencerIndex.sequencerTimestamp))
                .value
                .map(
                  _.getOrElse(
                    ErrorUtil.invalidState(
                      s"SequencerIndex with timestamp ${sequencerIndex.sequencerTimestamp} is not found in sequenced event store"
                    )
                  ).counter
                )
            )
        )

    val resultF = sequencerCounterF
      .flatMap { sequencerCounterO =>
        val nextSequencerCounter = sequencerCounterO
          .map(
            _.increment.getOrElse(
              throw new IllegalStateException("sequencer counter cannot be increased")
            )
          )
          .getOrElse(SequencerCounter.Genesis)
        logger.info(
          s"Deleting events from SequencedEventStore for synchronizer $psid fromInclusive $nextSequencerCounter"
        )
        persistentState.sequencedEventStore.delete(nextSequencerCounter)
      }
      .failOnShutdownToAbortException("cleanSequencedEventStoreAboveCleanSynchronizerIndex")

    timeouts.inspection.await(functionFullName)(resultF)
  }
}

object SyncStateInspection {

  sealed trait SyncStateInspectionError extends Product with Serializable
  private final case class NoSuchSynchronizer(alias: SynchronizerAlias)
      extends SyncStateInspectionError

  private def getOrFail[T, Sync <: PrettyPrinting](opt: Option[T], synchronizer: Sync): T =
    opt.getOrElse(throw new IllegalArgumentException(s"no such synchronizer [$synchronizer]"))

  final case class InFlightCount(
      pendingSubmissions: NonNegativeInt,
      pendingTransactions: NonNegativeInt,
  ) {
    def exists: Boolean = pendingSubmissions.unwrap > 0 || pendingTransactions.unwrap > 0

    def +(other: InFlightCount): InFlightCount = InFlightCount(
      pendingSubmissions = pendingSubmissions + other.pendingSubmissions,
      pendingTransactions = pendingTransactions + other.pendingTransactions,
    )
  }

  object InFlightCount {
    val zero: InFlightCount = InFlightCount(NonNegativeInt.zero, NonNegativeInt.zero)
  }

}
