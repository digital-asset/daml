// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.crypto.SyncCryptoApiParticipantProvider
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond, Offset}
import com.digitalasset.canton.ledger.participant.state.{RequestIndex, SynchronizerIndex}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
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
  ConnectedSynchronizersLookup,
  SyncPersistentStateManager,
}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SynchronizerOffset
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.CommitmentPeriodState.{
  Matched,
  fromIntValidSentPeriodState,
}
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId, SerializableContract}
import com.digitalasset.canton.pruning.{
  ConfigForSlowCounterParticipants,
  ConfigForSynchronizerThresholds,
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
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
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
  def disable(synchronizerId: SynchronizerId)(implicit traceContext: TraceContext): Future[Unit]
  def enable(synchronizerId: SynchronizerId)(implicit traceContext: TraceContext): Unit
}

object JournalGarbageCollectorControl {
  object NoOp extends JournalGarbageCollectorControl {
    override def disable(synchronizerId: SynchronizerId)(implicit
        traceContext: TraceContext
    ): Future[Unit] =
      Future.unit
    override def enable(synchronizerId: SynchronizerId)(implicit traceContext: TraceContext): Unit =
      ()
  }
}

/** Implements inspection functions for the sync state of a participant node */
final class SyncStateInspection(
    syncPersistentStateManager: SyncPersistentStateManager,
    participantNodePersistentState: Eval[ParticipantNodePersistentState],
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

  /** Look up all unpruned state changes of a set of contracts on all synchronizers.
    * If a contract is not found in an available ACS it will be omitted from the response.
    */
  def lookupContractSynchronizers(
      contractIds: Set[LfContractId]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Map[SynchronizerId, SortedMap[LfContractId, Seq[(CantonTimestamp, ActivenessChangeDetail)]]]
  ] =
    syncPersistentStateManager.getAll.toList
      .map { case (id, state) =>
        state.activeContractStore.activenessOf(contractIds.toSeq).map(s => id -> s)
      }
      .sequence
      .map(_.toMap)

  /** Returns the potentially large ACS of a given synchronizer
    * containing a map of contract IDs to tuples containing the latest activation timestamp and the contract reassignment counter
    */
  def findAcs(
      synchronizerAlias: SynchronizerAlias
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncStateInspectionError, Map[
    LfContractId,
    (CantonTimestamp, ReassignmentCounter),
  ]] =
    for {
      state <- EitherT.fromEither[FutureUnlessShutdown](
        syncPersistentStateManager
          .getByAlias(synchronizerAlias)
          .toRight(SyncStateInspection.NoSuchSynchronizer(synchronizerAlias))
      )

      snapshotO <- EitherT.right(state.acsInspection.getCurrentSnapshot().map(_.map(_.snapshot)))
    } yield snapshotO.fold(Map.empty[LfContractId, (CantonTimestamp, ReassignmentCounter)])(
      _.toMap
    )

  /** searches the pcs and returns the contract and activeness flag */
  def findContracts(
      synchronizerAlias: SynchronizerAlias,
      filterId: Option[String],
      filterPackage: Option[String],
      filterTemplate: Option[String],
      limit: Int,
  )(implicit traceContext: TraceContext): List[(Boolean, SerializableContract)] =
    getOrFail(
      timeouts.inspection.await("findContracts") {
        syncPersistentStateManager
          .getByAlias(synchronizerAlias)
          .traverse(_.acsInspection.findContracts(filterId, filterPackage, filterTemplate, limit))
          .failOnShutdownToAbortException("findContracts")
      },
      synchronizerAlias,
    )

  def findContractPayloads(
      synchronizerId: SynchronizerId,
      contractIds: Seq[LfContractId],
      limit: Int,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, SerializableContract]] = {
    val synchronizerAlias = syncPersistentStateManager
      .aliasForSynchronizerId(synchronizerId)
      .getOrElse(throw new IllegalArgumentException(s"no such synchronizer [$synchronizerId]"))

    NonEmpty.from(contractIds) match {
      case None =>
        FutureUnlessShutdown.pure(Map.empty[LfContractId, SerializableContract])
      case Some(neCids) =>
        val synchronizerAcsInspection =
          getOrFail(
            syncPersistentStateManager
              .get(synchronizerId),
            synchronizerAlias,
          ).acsInspection

        synchronizerAcsInspection.findContractPayloads(
          neCids,
          limit,
        )
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
          .get(targetSynchronizerId)
          .traverse(
            _.reassignmentStore.findContractReassignmentId(
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
      synchronizerAlias: SynchronizerAlias
  )(implicit traceContext: TraceContext): Option[PruningStatus] =
    timeouts.inspection
      .awaitUS("acsPruningStatus")(
        getOrFail(
          getPersistentState(synchronizerAlias),
          synchronizerAlias,
        ).acsInspection.activeContractStore.pruningStatus
      )
      .asGrpcResponse

  private def disableJournalCleaningForFilter(
      synchronizers: Map[SynchronizerId, SyncPersistentState],
      filterSynchronizerId: SynchronizerId => Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsInspectionError, Unit] = {
    val disabledCleaningF = Future
      .sequence(synchronizers.collect {
        case (synchronizerId, _) if filterSynchronizerId(synchronizerId) =>
          journalCleaningControl.disable(synchronizerId)
      })
      .map(_ => ())
    EitherT.right(disabledCleaningF)
  }

  def allProtocolVersions: Map[SynchronizerId, ProtocolVersion] =
    syncPersistentStateManager.getAll.view
      .mapValues(_.staticSynchronizerParameters.protocolVersion)
      .toMap

  def exportAcsDumpActiveContracts(
      outputStream: OutputStream,
      filterSynchronizerId: SynchronizerId => Boolean,
      parties: Set[LfPartyId],
      timestamp: Option[CantonTimestamp],
      contractSynchronizerRenames: Map[SynchronizerId, (SynchronizerId, ProtocolVersion)],
      skipCleanTimestampCheck: Boolean,
      partiesOffboarding: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, AcsInspectionError, Unit] = {
    val allSynchronizers = syncPersistentStateManager.getAll

    // disable journal cleaning for the duration of the dump
    disableJournalCleaningForFilter(allSynchronizers, filterSynchronizerId)
      .mapK(FutureUnlessShutdown.outcomeK)
      .flatMap { _ =>
        MonadUtil.sequentialTraverse_(allSynchronizers) {
          case (synchronizerId, state) if filterSynchronizerId(synchronizerId) =>
            val (synchronizerIdForExport, protocolVersion) =
              contractSynchronizerRenames.getOrElse(
                synchronizerId,
                (synchronizerId, state.staticSynchronizerParameters.protocolVersion),
              )
            val acsInspection = state.acsInspection

            val ret = for {
              result <- acsInspection
                .forEachVisibleActiveContract(
                  synchronizerId,
                  parties,
                  timestamp,
                  skipCleanTimestampCheck = skipCleanTimestampCheck,
                ) { case (contract, reassignmentCounter) =>
                  val activeContract =
                    ActiveContract.create(synchronizerIdForExport, contract, reassignmentCounter)(
                      protocolVersion
                    )

                  activeContract.writeDelimitedTo(outputStream) match {
                    case Left(errorMessage) =>
                      Left(
                        AcsInspectionError.SerializationIssue(
                          synchronizerId,
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
                    connectedSynchronizer <- EitherT.fromOption[FutureUnlessShutdown](
                      connectedSynchronizersLookup.get(synchronizerId),
                      AcsInspectionError.OffboardingParty(
                        synchronizerId,
                        s"Unable to get topology client for synchronizer $synchronizerId; check synchronizer connectivity.",
                      ),
                    )

                    _ <- acsInspection.checkOffboardingSnapshot(
                      participantId,
                      offboardedParties = parties,
                      allStakeholders = allStakeholders,
                      snapshotTs = snapshotTs,
                      topologyClient = connectedSynchronizer.topologyClient,
                    )
                  } yield ()

                // Snapshot is empty or partiesOffboarding is false
                case _ => EitherTUtil.unitUS[AcsInspectionError]
              }

            } yield ()
            // re-enable journal cleaning after the dump
            ret.thereafter { _ =>
              journalCleaningControl.enable(synchronizerId)
            }
          case _ =>
            EitherTUtil.unitUS
        }
      }
  }

  def contractCount(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] =
    participantNodePersistentState.value.contractStore.contractCount()

  def contractCountInAcs(synchronizerAlias: SynchronizerAlias, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Int]] =
    getPersistentState(synchronizerAlias) match {
      case None => FutureUnlessShutdown.pure(None)
      case Some(state) => state.activeContractStore.contractCount(timestamp).map(Some(_))
    }

  def requestJournalSize(
      synchronizerAlias: SynchronizerAlias,
      start: CantonTimestamp = CantonTimestamp.Epoch,
      end: Option[CantonTimestamp] = None,
  )(implicit traceContext: TraceContext): Option[UnlessShutdown[Int]] =
    getPersistentState(synchronizerAlias).map { state =>
      timeouts.inspection.awaitUS(
        s"$functionFullName from $start to $end from the journal of synchronizer $synchronizerAlias"
      )(
        state.requestJournalStore.size(start, end)
      )
    }

  def activeContractsStakeholdersFilter(
      synchronizerId: SynchronizerId,
      timestamp: CantonTimestamp,
      parties: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[(LfContractId, ReassignmentCounter)]] =
    for {
      state <- FutureUnlessShutdown.fromTry(
        syncPersistentStateManager
          .get(synchronizerId)
          .toTry(
            new IllegalArgumentException(s"Unable to find contract store for $synchronizerId.")
          )
      )

      snapshot <- state.activeContractStore.snapshot(timestamp)

      // check that the active contract store has not been pruned up to timestamp, otherwise the snapshot is inconsistent.
      pruningStatus <- state.activeContractStore.pruningStatus
      _ <-
        if (pruningStatus.exists(_.timestamp > timestamp)) {
          FutureUnlessShutdown.failed(
            new IllegalStateException(
              s"Active contract store for synchronizer $synchronizerId has been pruned up to ${pruningStatus
                  .map(_.lastSuccess)}, which is after the requested timestamp $timestamp"
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
        case (contract, reassignmentCounter)
            if parties.intersect(contract.metadata.stakeholders).nonEmpty =>
          (contract.contractId, reassignmentCounter)
      }
    } yield filteredByParty.toSet

  private def tryGetProtocolVersion(
      state: SyncPersistentState,
      synchronizerAlias: SynchronizerAlias,
  )(implicit traceContext: TraceContext): ProtocolVersion =
    timeouts.inspection
      .awaitUS(functionFullName)(state.parameterStore.lastParameters) match {
      case UnlessShutdown.Outcome(Some(ssp)) => ssp.protocolVersion
      case _ =>
        throw new IllegalStateException(
          s"No static synchronizer parameters found for $synchronizerAlias"
        )
    }

  def getProtocolVersion(
      synchronizerAlias: SynchronizerAlias
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ProtocolVersion] =
    for {
      param <- syncPersistentStateManager
        .getByAlias(synchronizerAlias)
        .traverse(_.parameterStore.lastParameters)
    } yield {
      getOrFail(param, synchronizerAlias)
        .map(_.protocolVersion)
        .getOrElse(
          throw new IllegalStateException(
            s"No static synchronizer parameters found for $synchronizerAlias"
          )
        )
    }

  def findMessages(
      synchronizerAlias: SynchronizerAlias,
      from: Option[Instant],
      to: Option[Instant],
      limit: Option[Int],
  )(implicit traceContext: TraceContext): Seq[ParsingResult[PossiblyIgnoredProtocolEvent]] = {
    val state = getOrFail(getPersistentState(synchronizerAlias), synchronizerAlias)
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
        .awaitUS(s"finding messages from $from to $to on $synchronizerAlias")(messagesF)
        .asGrpcResponse
    val opener =
      new EnvelopeOpener[PossiblyIgnoredSequencedEvent](
        tryGetProtocolVersion(state, synchronizerAlias),
        state.pureCryptoApi,
      )
    closed.map(opener.open)
  }

  @VisibleForTesting
  def findMessage(
      synchronizerAlias: SynchronizerAlias,
      criterion: SequencedEventStore.SearchCriterion,
  )(implicit
      traceContext: TraceContext
  ): Option[ParsingResult[PossiblyIgnoredProtocolEvent]] = {
    val state = getOrFail(getPersistentState(synchronizerAlias), synchronizerAlias)
    val messageF = state.sequencedEventStore.find(criterion).value
    val closed =
      timeouts.inspection
        .awaitUS(s"$functionFullName on $synchronizerAlias matching $criterion")(messageF)
    val opener = new EnvelopeOpener[PossiblyIgnoredSequencedEvent](
      tryGetProtocolVersion(state, synchronizerAlias),
      state.pureCryptoApi,
    )
    closed match {
      case UnlessShutdown.Outcome(result) => result.toOption.map(opener.open)
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
  ): Iterable[(CommitmentPeriod, ParticipantId, AcsCommitment.CommitmentType)] =
    timeouts.inspection
      .awaitUS(s"$functionFullName from $start to $end on $synchronizerAlias")(
        getOrFail(getPersistentState(synchronizerAlias), synchronizerAlias).acsCommitmentStore
          .searchComputedBetween(start, end, counterParticipant.toList)
      )
      .asGrpcResponse

  def findLastComputedAndSent(
      synchronizerAlias: SynchronizerAlias
  )(implicit traceContext: TraceContext): Option[CantonTimestampSecond] =
    timeouts.inspection
      .awaitUS(s"$functionFullName on $synchronizerAlias")(
        getOrFail(
          getPersistentState(synchronizerAlias),
          synchronizerAlias,
        ).acsCommitmentStore.lastComputedAndSent
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
        getOrFail(getPersistentState(synchronizerAlias), synchronizerAlias).acsCommitmentStore
          .searchReceivedBetween(start, end, counterParticipant.toList)
      )
      .asGrpcResponse

  def crossSynchronizerSentCommitmentMessages(
      synchronizerPeriods: Seq[SynchronizerSearchCommitmentPeriod],
      counterParticipants: Seq[ParticipantId],
      states: Seq[CommitmentPeriodState],
      verbose: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Iterable[SentAcsCommitment]] =
    timeouts.inspection
      .awaitUS(functionFullName) {
        val searchResult = synchronizerPeriods.map { dp =>
          for {
            synchronizerAlias <- syncPersistentStateManager
              .aliasForSynchronizerId(dp.indexedSynchronizer.synchronizerId)
              .toRight(s"No synchronizer alias found for ${dp.indexedSynchronizer.synchronizerId}")

            persistentState <- getPersistentStateE(synchronizerAlias)

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
      counterParticipants: Seq[ParticipantId],
      states: Seq[CommitmentPeriodState],
      verbose: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Iterable[ReceivedAcsCommitment]] =
    timeouts.inspection
      .awaitUS(functionFullName) {
        val searchResult = synchronizerPeriods.map { dp =>
          for {
            synchronizerAlias <- syncPersistentStateManager
              .aliasForSynchronizerId(dp.indexedSynchronizer.synchronizerId)
              .toRight(s"No synchronizer alias found for ${dp.indexedSynchronizer.synchronizerId}")
            persistentState <- getPersistentStateE(synchronizerAlias)

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
                    cmt.period.fromExclusive >= dp.fromExclusive && cmt.synchronizerId == dp.indexedSynchronizer.synchronizerId && (counterParticipants.isEmpty ||
                      counterParticipants
                        .contains(cmt.sender))
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
  ): Iterable[(CommitmentPeriod, ParticipantId, CommitmentPeriodState)] = {
    val persistentState = getPersistentState(synchronizerAlias)
    timeouts.inspection.awaitUS(s"$functionFullName from $start to $end on $synchronizerAlias")(
      getOrFail(persistentState, synchronizerAlias).acsCommitmentStore
        .outstanding(start, end, counterParticipant.toList)
    )
  }.asGrpcResponse

  def bufferedCommitments(
      synchronizerAlias: SynchronizerAlias,
      endAtOrBefore: CantonTimestamp,
  )(implicit traceContext: TraceContext): Iterable[AcsCommitment] = {
    val persistentState = getPersistentState(synchronizerAlias)
    timeouts.inspection
      .awaitUS(s"$functionFullName to and including $endAtOrBefore on $synchronizerAlias")(
        getOrFail(persistentState, synchronizerAlias).acsCommitmentStore.queue
          .peekThrough(endAtOrBefore)
      )
      .asGrpcResponse
  }

  def noOutstandingCommitmentsTs(synchronizerAlias: SynchronizerAlias, beforeOrAt: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): Option[CantonTimestamp] = {
    val persistentState = getPersistentState(synchronizerAlias)

    timeouts.inspection.awaitUS(s"$functionFullName on $synchronizerAlias for ts $beforeOrAt")(
      for {
        result <- getOrFail(persistentState, synchronizerAlias).acsCommitmentStore
          .noOutstandingCommitments(beforeOrAt)
      } yield result
    )
  }.asGrpcResponse

  def lookupCleanRequestIndex(synchronizerAlias: SynchronizerAlias)(implicit
      traceContext: TraceContext
  ): Either[String, FutureUnlessShutdown[Option[RequestIndex]]] =
    lookupCleanSynchronizerIndex(synchronizerAlias)
      .map(_.map(_.flatMap(_.requestIndex)))

  def lookupCleanSynchronizerIndex(synchronizerAlias: SynchronizerAlias)(implicit
      traceContext: TraceContext
  ): Either[String, FutureUnlessShutdown[Option[SynchronizerIndex]]] =
    getPersistentStateE(synchronizerAlias)
      .map(state =>
        participantNodePersistentState.value.ledgerApiStore
          .cleanSynchronizerIndex(state.indexedSynchronizer.synchronizerId)
      )

  def requestStateInJournal(rc: RequestCounter, synchronizerAlias: SynchronizerAlias)(implicit
      traceContext: TraceContext
  ): Either[String, FutureUnlessShutdown[Option[RequestJournal.RequestData]]] =
    getPersistentStateE(synchronizerAlias)
      .map(state => state.requestJournalStore.query(rc).value)

  private[this] def getPersistentState(
      synchronizerAlias: SynchronizerAlias
  ): Option[SyncPersistentState] =
    syncPersistentStateManager.getByAlias(synchronizerAlias)

  private[this] def getPersistentStateE(
      synchronizerAlias: SynchronizerAlias
  ): Either[String, SyncPersistentState] =
    syncPersistentStateManager
      .getByAlias(synchronizerAlias)
      .toRight(s"no such synchronizer [$synchronizerAlias]")

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
      synchronizers: Seq[SynchronizerId],
      participants: Seq[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Iterable[CounterParticipantIntervalsBehind]] = {
    val result = for {
      (synchronizerId, syncPersistentState) <-
        syncPersistentStateManager.getAll
          .filter { case (synchronizer, _) =>
            synchronizers.contains(synchronizer) || synchronizers.isEmpty
          }
    } yield for {
      lastSent <- syncPersistentState.acsCommitmentStore.lastComputedAndSent(traceContext)

      lastSentFinal = lastSent.fold(CantonTimestamp.MinValue)(_.forgetRefinement)
      outstanding <- syncPersistentState.acsCommitmentStore
        .outstanding(
          CantonTimestamp.MinValue,
          lastSentFinal,
          participants,
          includeMatchedPeriods = true,
        )

      upToDate <- syncPersistentState.acsCommitmentStore.searchReceivedBetween(
        lastSentFinal,
        lastSentFinal,
      )

      filteredAllParticipants <- findAllKnownParticipants(synchronizers, participants)
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
          .get(synchronizerId, lastSentFinal)
          .leftMap(string =>
            new IllegalStateException(
              s"failed to retrieve reconciliationIntervalProvider: $string"
            )
          )
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
    FutureUnlessShutdown.sequence(result).map(_.flatten)
  }

  def addOrUpdateConfigsForSlowCounterParticipants(
      configs: Seq[ConfigForSlowCounterParticipants],
      thresholds: Seq[ConfigForSynchronizerThresholds],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    participantNodePersistentState.value.acsCounterParticipantConfigStore
      .createOrUpdateCounterParticipantConfigs(configs, thresholds)

  def countInFlight(
      synchronizerAlias: SynchronizerAlias
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, InFlightCount] =
    for {
      state <- EitherT.fromEither[FutureUnlessShutdown](
        getPersistentState(synchronizerAlias)
          .toRight(s"Unknown synchronizer $synchronizerAlias")
      )
      synchronizerId = state.indexedSynchronizer.synchronizerId
      unsequencedSubmissions <- EitherT.right(
        participantNodePersistentState.value.inFlightSubmissionStore
          .lookupUnsequencedUptoUnordered(synchronizerId, CantonTimestamp.now())
      )
      pendingSubmissions = NonNegativeInt.tryCreate(unsequencedSubmissions.size)
      pendingTransactions <- EitherT.right[String](state.requestJournalStore.totalDirtyRequests())
    } yield {
      InFlightCount(pendingSubmissions, pendingTransactions)
    }

  def verifyLapiStoreIntegrity()(implicit traceContext: TraceContext): Unit =
    timeouts.inspection
      .awaitUS(functionFullName)(
        participantNodePersistentState.value.ledgerApiStore.onlyForTestingVerifyIntegrity()
      )
      .onShutdown(throw new RuntimeException("verifyLapiStoreIntegrity"))

  def acceptedTransactionCount(
      synchronizerAlias: SynchronizerAlias
  )(implicit traceContext: TraceContext): Int =
    getPersistentState(synchronizerAlias)
      .map(synchronizerPersistentState =>
        timeouts.inspection
          .awaitUS(functionFullName)(
            participantNodePersistentState.value.ledgerApiStore
              .onlyForTestingNumberOfAcceptedTransactionsFor(
                synchronizerPersistentState.indexedSynchronizer.synchronizerId
              )
          )
          .onShutdown(0)
      )
      .getOrElse(0)

  def onlyForTestingMoveLedgerEndBackToScratch()(implicit traceContext: TraceContext): Unit =
    timeouts.inspection
      .awaitUS(functionFullName)(
        participantNodePersistentState.value.ledgerApiStore
          .onlyForTestingMoveLedgerEndBackToScratch()
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

  def getSequencerChannelClient(synchronizerId: SynchronizerId): Option[SequencerChannelClient] =
    for {
      connectedSynchronizer <- connectedSynchronizersLookup.get(synchronizerId)
      sequencerChannelClient <- connectedSynchronizer.synchronizerHandle.sequencerChannelClientO
    } yield sequencerChannelClient

  def getAcsInspection(synchronizerId: SynchronizerId): Option[AcsInspection] =
    connectedSynchronizersLookup
      .get(synchronizerId)
      .map(_.synchronizerHandle.syncPersistentState.acsInspection)

  def findAllKnownParticipants(
      synchronizerFilter: Seq[SynchronizerId] = Seq.empty,
      participantFilter: Seq[ParticipantId] = Seq.empty,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SynchronizerId, Set[ParticipantId]]] = {
    val result = for {
      (synchronizerId, _) <-
        syncPersistentStateManager.getAll.filter { case (synchronizerId, _) =>
          synchronizerFilter.contains(synchronizerId) || synchronizerFilter.isEmpty
        }
    } yield for {
      _ <- FutureUnlessShutdown.unit
      synchronizerTopoClient = syncCrypto.ips.tryForSynchronizer(synchronizerId)
      ipsSnapshot <- synchronizerTopoClient.awaitSnapshot(
        synchronizerTopoClient.approximateTimestamp
      )
      allMembers <- ipsSnapshot.allMembers()
      allParticipants = allMembers
        .filter(_.code == ParticipantId.Code)
        .map(member => ParticipantId.apply(member.uid))
        .excl(participantId)
        .filter(participantFilter.contains(_) || participantFilter.isEmpty)
    } yield (synchronizerId, allParticipants)

    FutureUnlessShutdown.sequence(result).map(_.toMap)
  }

  def cleanSequencedEventStoreAboveCleanSynchronizerIndex(synchronizerAlias: SynchronizerAlias)(
      implicit traceContext: TraceContext
  ): Unit =
    getOrFail(
      getPersistentState(synchronizerAlias).map { synchronizerState =>
        timeouts.inspection.await(functionFullName)(
          participantNodePersistentState.value.ledgerApiStore
            .cleanSynchronizerIndex(synchronizerState.indexedSynchronizer.synchronizerId)
            .flatMap { synchronizerIndexO =>
              val nextSequencerCounter = synchronizerIndexO
                .flatMap(_.sequencerIndex)
                .map(
                  _.counter.increment
                    .getOrElse(
                      throw new IllegalStateException("sequencer counter cannot be increased")
                    )
                )
                .getOrElse(SequencerCounter.Genesis)
              logger.info(
                s"Deleting events from SequencedEventStore for synchronizer $synchronizerAlias fromInclusive $nextSequencerCounter"
              )
              synchronizerState.sequencedEventStore.delete(nextSequencerCounter)
            }
            .failOnShutdownToAbortException("cleanSequencedEventStoreAboveCleanSynchronizerIndex")
        )
      },
      synchronizerAlias,
    )
}

object SyncStateInspection {

  sealed trait SyncStateInspectionError extends Product with Serializable
  private final case class NoSuchSynchronizer(alias: SynchronizerAlias)
      extends SyncStateInspectionError

  private def getOrFail[T](opt: Option[T], synchronizerAlias: SynchronizerAlias): T =
    opt.getOrElse(throw new IllegalArgumentException(s"no such synchronizer [$synchronizerAlias]"))

  final case class InFlightCount(
      pendingSubmissions: NonNegativeInt,
      pendingTransactions: NonNegativeInt,
  ) {
    def exists: Boolean = pendingSubmissions.unwrap > 0 || pendingTransactions.unwrap > 0
  }

}
