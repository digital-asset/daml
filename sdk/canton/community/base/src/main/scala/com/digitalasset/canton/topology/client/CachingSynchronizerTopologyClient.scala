// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.caching.ScaffeineCache.TracedAsyncLoadingCache
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{
  BatchingConfig,
  CachingConfigs,
  ProcessingTimeout,
  TopologyConfig,
}
import com.digitalasset.canton.crypto.SigningKeysWithThreshold
import com.digitalasset.canton.data.{
  CantonTimestamp,
  SynchronizerPredecessor,
  SynchronizerSuccessor,
}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle, PromiseUnlessShutdown}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{
  DynamicSequencingParametersWithValidity,
  DynamicSynchronizerParametersWithValidity,
  StaticSynchronizerParameters,
}
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.CachingSynchronizerTopologyClient.SnapshotEntry
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.processing.*
import com.digitalasset.canton.topology.store.{
  PackageDependencyResolver,
  TopologyStore,
  TopologyStoreId,
  UnknownOrUnvettedPackages,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.daml.lf.data.Ref.PackageId

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable.SortedMap
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala

final class CachingSynchronizerTopologyClient(
    delegate: StoreBasedSynchronizerTopologyClient,
    cachingConfigs: CachingConfigs,
    batchingConfig: BatchingConfig,
    val timeouts: ProcessingTimeout,
    override protected val futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends SynchronizerTopologyClientWithInit
    with NamedLogging {

  override def staticSynchronizerParameters: StaticSynchronizerParameters =
    delegate.staticSynchronizerParameters

  override def updateHead(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      approximateTimestamp: ApproximateTime,
  )(implicit traceContext: TraceContext): Unit =
    delegate.updateHead(
      sequencedTimestamp,
      effectiveTimestamp,
      approximateTimestamp,
    )

  private val maxTimestampCache: TracedAsyncLoadingCache[
    FutureUnlessShutdown,
    SequencedTime,
    Option[(SequencedTime, EffectiveTime)],
  ] = ScaffeineCache.buildTracedAsync[
    FutureUnlessShutdown,
    SequencedTime,
    Option[(SequencedTime, EffectiveTime)],
  ](
    cache = cachingConfigs.synchronizerClientMaxTimestamp.buildScaffeine(loggerFactory),
    loader = traceContext => delegate.awaitMaxTimestamp(_)(traceContext),
  )(logger, "maxTimestampCache")

  /** Map of snapshot intervals for which snapshots are cached. The keys of the map represent the
    * smallest timestamp for which the respective snapshot is valid. Invariants:
    *   - The intervals must not be overlapping.
    *   - There may be gaps in the intervals, which get re-populated on-demand.
    *   - The interval serving head and approximate snapshots (i.e. >=
    *     latestTopologyChangeTimestamp) is managed explicitly (see [[snapshot]])
    *
    * An entry with a given `intervalStartInclusive` refers to the topology snapshot at the same
    * timestamp `intervalStartInclusive`. This is the snapshot, that covers all committed topology
    * transactions with `validFrom < intervalStartInclusive` and
    * `validUntil.forall(intervalStartInclusive <= _)`, following the topology snapshot and
    * effective time semantics: there are no topology changes strictly between
    * `intervalStartInclusive` and `intervalEndExclusive`. In other words, there is no non-proposal
    * or non-rejected topology transaction in the store with `intervalEndExclusive.forall(end =>
    * intervalStartInclusive < validFrom && validFrom < end)`.
    *
    * Technical note: using SortedMap for log(n) lookups, inserts and deletes, assuming rare
    * updates, and similarly we expect low contention on updateAndGet / getAndUpdate for the
    * AtomicReference.
    */
  private val snapshots =
    new AtomicReference[SortedMap[CantonTimestamp, SnapshotEntry]](SortedMap.empty)

  /** Keys that we tried to evict but could not, for later retry of eviction.
    */
  private val evictLater = ConcurrentHashMap.newKeySet[CantonTimestamp]()

  private def findSnapshotEntry(timestamp: CantonTimestamp): Option[SnapshotEntry] =
    snapshots
      .get()
      .maxBefore(
        timestamp.immediateSuccessor
      ) // immediateSuccessor because intervals are inclusive at start
      .collect {
        case (_, entry) if entry.intervalEndExclusive > timestamp => entry
      }

  /** Attempts eviction of interval keys from `snapshots`, preventing eviction of intervals covering
    * current snapshot approximation. Returns not evicted keys for later retry.
    */
  private def tryEvict(keys: Seq[CantonTimestamp]): Seq[CantonTimestamp] = {
    val updatedSnapshots = snapshots.updateAndGet { current =>
      // We never evict intervals that cover current and future approximate timestamps
      // as these are expected to be reused heavily.
      val approximateTime = approximateTimestamp
      val keysToRemove = keys.filter { key =>
        val entryO = findSnapshotEntry(key)
        entryO.forall(_.intervalEndExclusive <= approximateTime)
      }
      keysToRemove.foreach(key => evictLater.remove(key).discard)
      current.removedAll(keysToRemove)
    }
    keys.filterNot(updatedSnapshots.contains)
  }

  /** Cache of snapshots. We want to avoid loading redundant data from the database. Now, we know
    * that if there was no topology transaction between tx and ty, then snapshot(ty) ==
    * snapshot(tx). Therefore, we remember the list of timestamps when updates happened (in
    * `snapshots`) and use that map in order to figure out which snapshot we can use instead of
    * loading the same data again and again. So we use `snapshots` to figure out the update
    * timestamp and then we use the `pointwise` cache to load the corresponding snapshot. Upon
    * eviction of an entry in the pointwise cache, the entry in `snapshots` is removed as well,
    * except for any interval past the current snapshot approximation.
    */
  private val pointwise =
    cachingConfigs.topologySnapshot
      .buildScaffeine(loggerFactory)
      .evictionListener[Traced[CantonTimestamp], CachingTopologySnapshot] {
        case (tracedKey, _value, _cause) =>
          val key = tracedKey.unwrap
          tryEvict(key :: evictLater.asScala.toList).foreach(key => evictLater.add(key).discard)
      }
      .build[Traced[CantonTimestamp], CachingTopologySnapshot] {
        (tracedTs: Traced[CantonTimestamp]) =>
          tracedTs.withTraceContext(implicit tc =>
            ts =>
              new CachingTopologySnapshot(
                delegate.trySnapshot(ts),
                cachingConfigs,
                batchingConfig,
                loggerFactory,
                futureSupervisor,
              )
          )
      }

  override def headSnapshot(implicit traceContext: TraceContext): TopologySnapshot =
    new ForwardingTopologySnapshotClient(
      topologyKnownUntilTimestamp,
      pointwise.get(Traced(latestTopologyChangeTimestamp)),
      loggerFactory,
    )

  private def findAndCacheSnapshotEntry(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[SnapshotEntry]] =
    findSnapshotEntry(timestamp).fold(
      // we don't have a reference to the corresponding cached snapshot anymore.
      // therefore, we recreate the snapshot interval for the given timestamp
      delegate
        .findTopologyIntervalForTimestamp(timestamp)
        .map(_.map { case (validFrom, validUntil) =>
          // topology snapshots are exclusive on effective time, the below "emulates" inclusivity for the given effective time,
          // as we want to make topology changes observable as part of the topology snapshot for the given time
          val validFromAsInclusive = validFrom.immediateSuccessor.value
          val validUntilAsExclusive = validUntil
            .map(_.immediateSuccessor.value)
            .getOrElse(
              ErrorUtil.invalidState(
                s"Topology interval starting from $timestamp before latestTopologyChangeTimestamp = $latestTopologyChangeTimestamp is expected to have an upper bound."
              )
            )
          val newEntry = SnapshotEntry(validFromAsInclusive, validUntilAsExclusive)
          logger.debug(
            s"Caching new snapshot interval for timestamp $timestamp, validFrom=$validFromAsInclusive, validUntil=$validUntilAsExclusive"
          )
          // If we lost the contention for the same key in the meantime, we take the existing one:
          val newSnapshots = snapshots.updateAndGet { current =>
            current.updated(newEntry.intervalStartInclusive, newEntry)
          }
          newSnapshots.getOrElse(
            newEntry.intervalStartInclusive,
            ErrorUtil.invalidState(
              s"Could not find the interval $newEntry that we just inserted."
            ),
          )
        })
    ) { snapshotEntry =>
      logger.debug(
        s"Re-using existing snapshot interval for timestamp $timestamp, validFrom=${snapshotEntry.intervalStartInclusive}, validUntil=${snapshotEntry.intervalEndExclusive}"
      )
      FutureUnlessShutdown.pure(Some(snapshotEntry))
    }

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshotLoader] =
    waitForTimestampWithLogging(timestamp).flatMap { _ =>
      val latestTopologyChangeTimestampValue = latestTopologyChangeTimestamp
      if (timestamp >= latestTopologyChangeTimestampValue) {
        // Returning effectively the head snapshot
        FutureUnlessShutdown.pure(
          new ForwardingTopologySnapshotClient(
            timestamp,
            pointwise.get(Traced(latestTopologyChangeTimestampValue)),
            loggerFactory,
          )
        )
      } else {
        // An interval covering `timestamp` will be found or recreated in the `snapshots` map
        findAndCacheSnapshotEntry(timestamp).map {
          case Some(snapshotEntry) =>
            new ForwardingTopologySnapshotClient(
              timestamp,
              pointwise.get(Traced(snapshotEntry.intervalStartInclusive)),
              loggerFactory,
            )
          case None =>
            // only exceptionally this will be called, mostly during the node initialization
            logger.info(
              s"Using pointwise cached snapshot for timestamp $timestamp"
            )
            pointwise.get(Traced(timestamp))
        }
      }
    }

  override def hypotheticalSnapshot(timestamp: CantonTimestamp, desiredTimestamp: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshotLoader] = snapshot(timestamp).map(
    new ForwardingTopologySnapshotClient(
      desiredTimestamp,
      _,
      loggerFactory,
    )
  )

  override def synchronizerId: SynchronizerId = delegate.synchronizerId
  override def psid: PhysicalSynchronizerId = delegate.psid

  override def snapshotAvailable(timestamp: CantonTimestamp): Boolean =
    delegate.snapshotAvailable(timestamp)

  override def awaitTimestamp(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Option[FutureUnlessShutdown[Unit]] =
    delegate.awaitTimestamp(timestamp)

  override def awaitSequencedTimestamp(timestampInclusive: SequencedTime)(implicit
      traceContext: TraceContext
  ): Option[FutureUnlessShutdown[Unit]] = delegate.awaitSequencedTimestamp(timestampInclusive)

  override def approximateTimestamp: CantonTimestamp = delegate.approximateTimestamp

  override def currentSnapshotApproximation(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshotLoader] = snapshot(approximateTimestamp)

  override def topologyKnownUntilTimestamp: CantonTimestamp = delegate.topologyKnownUntilTimestamp

  override def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Boolean
  ] = // we use our implementation such that we can benefit from cached data
    delegate.scheduleAwait(
      currentSnapshotApproximation.flatMap(snapshot =>
        FutureUnlessShutdown.outcomeF(condition(snapshot))
      ),
      timeout,
    )

  override def awaitUS(
      condition: TopologySnapshot => FutureUnlessShutdown[Boolean],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Boolean
  ] = // we use our implementation such that we can benefit from cached data
    delegate.scheduleAwait(currentSnapshotApproximation.flatMap(condition), timeout)

  override private[topology] def scheduleAwait(
      condition: => FutureUnlessShutdown[Boolean],
      timeout: Duration,
  ) =
    delegate.scheduleAwait(condition, timeout)

  override def numPendingChanges: Int = delegate.numPendingChanges

  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    delegate.observed(sequencedTimestamp, effectiveTimestamp, sequencerCounter, transactions)

  override def setSynchronizerTimeTracker(tracker: SynchronizerTimeTracker): Unit = {
    delegate.setSynchronizerTimeTracker(tracker)
    super.setSynchronizerTimeTracker(tracker)
  }

  override def awaitMaxTimestamp(sequencedTime: SequencedTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] =
    maxTimestampCache.get(sequencedTime)

  override def close(): Unit = {
    pointwise.invalidateAll()
    pointwise.cleanUp()
    maxTimestampCache.invalidateAll()
    maxTimestampCache.cleanUp()
    LifeCycle.close(delegate)(logger)
  }

  override def latestTopologyChangeTimestamp: CantonTimestamp =
    delegate.latestTopologyChangeTimestamp

  override def initialize(
      sequencerSnapshotTimestamp: Option[EffectiveTime],
      synchronizerUpgradeTime: Option[SequencedTime],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    delegate.initialize(sequencerSnapshotTimestamp, synchronizerUpgradeTime)
}

object CachingSynchronizerTopologyClient {

  def create(
      clock: Clock,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      store: TopologyStore[TopologyStoreId.SynchronizerStore],
      synchronizerPredecessor: Option[SynchronizerPredecessor],
      packageDependencyResolver: PackageDependencyResolver,
      cachingConfigs: CachingConfigs,
      batchingConfig: BatchingConfig,
      topologyConfig: TopologyConfig,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(
      sequencerSnapshotTimestamp: Option[EffectiveTime] = None
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[SynchronizerTopologyClientWithInit] = {
    val dbClient =
      new StoreBasedSynchronizerTopologyClient(
        clock,
        staticSynchronizerParameters,
        store,
        packageDependencyResolver,
        topologyConfig,
        timeouts,
        futureSupervisor,
        loggerFactory,
      )
    val caching =
      new CachingSynchronizerTopologyClient(
        dbClient,
        cachingConfigs,
        batchingConfig,
        timeouts,
        futureSupervisor,
        loggerFactory,
      )
    val synchronizerUpgradeTime =
      synchronizerPredecessor.map(predecessor => SequencedTime(predecessor.upgradeTime))
    caching
      .initialize(sequencerSnapshotTimestamp, synchronizerUpgradeTime)
      .map(_ => caching)
  }

  final case class SnapshotEntry(
      intervalStartInclusive: CantonTimestamp,
      intervalEndExclusive: CantonTimestamp,
  )
}

/** A simple wrapper class in order to "override" the timestamp we are returning here when caching.
  */
private class ForwardingTopologySnapshotClient(
    override val timestamp: CantonTimestamp,
    parent: TopologySnapshotLoader,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends TopologySnapshotLoader {

  override def referenceTime: CantonTimestamp = parent.timestamp
  override def allKeys(owner: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[KeyCollection] =
    parent.allKeys(owner)
  override def allKeys(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Member, KeyCollection]] = parent.allKeys(members)
  override def loadParticipantStates(
      participants: Seq[ParticipantId]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, ParticipantAttributes]] =
    parent.loadParticipantStates(participants)

  override private[client] def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => FutureUnlessShutdown[
        Map[ParticipantId, ParticipantAttributes]
      ],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[PartyInfo] =
    parent.loadActiveParticipantsOf(party, participantStates)

  override def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[MemberCode],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[Member, KeyCollection]] =
    parent.inspectKeys(filterOwner, filterOwnerType, limit)
  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]] =
    parent.inspectKnownParties(filterParty, filterParticipant, limit)

  override def loadVettedPackages(participant: ParticipantId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PackageId, VettedPackage]] = parent.loadVettedPackages(participant)

  override def loadVettedPackages(participants: Set[ParticipantId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, Map[PackageId, VettedPackage]]] =
    parent.loadVettedPackages(participants)

  override private[client] def findUnvettedPackagesOrDependencies(
      participant: ParticipantId,
      packages: Set[PackageId],
      ledgerTime: CantonTimestamp,
      vettedPackages: Map[PackageId, VettedPackage],
  )(implicit traceContext: TraceContext): UnknownOrUnvettedPackages =
    parent.findUnvettedPackagesOrDependencies(participant, packages, ledgerTime, vettedPackages)

  /** returns the list of currently known mediators */
  override def mediatorGroups()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[MediatorGroup]] =
    parent.mediatorGroups()

  /** returns the sequencer group if known */
  override def sequencerGroup()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[SequencerGroup]] = parent.sequencerGroup()

  override def allMembers()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[Member]] =
    parent.allMembers()

  override def isMemberKnown(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    parent.isMemberKnown(member)

  override def areMembersKnown(members: Set[Member])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[Member]] = parent.areMembersKnown(members)

  override def memberFirstKnownAt(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] =
    parent.memberFirstKnownAt(member)

  override def findDynamicSynchronizerParameters()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, DynamicSynchronizerParametersWithValidity]] =
    parent.findDynamicSynchronizerParameters()

  override def findDynamicSequencingParameters()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, DynamicSequencingParametersWithValidity]] =
    parent.findDynamicSequencingParameters()

  /** List all the dynamic synchronizer parameters (past and current) */
  override def listDynamicSynchronizerParametersChanges()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[DynamicSynchronizerParametersWithValidity]] =
    parent.listDynamicSynchronizerParametersChanges()

  override private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => FutureUnlessShutdown[
        Map[ParticipantId, ParticipantAttributes]
      ],
  )(implicit traceContext: TraceContext) =
    parent.loadBatchActiveParticipantsOf(parties, loadParticipantStates)

  override def signingKeysWithThreshold(party: PartyId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[SigningKeysWithThreshold]] =
    parent.signingKeysWithThreshold(party)

  override def synchronizerUpgradeOngoing()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SynchronizerSuccessor, EffectiveTime)]] =
    parent.synchronizerUpgradeOngoing()

  override def sequencerConnectionSuccessors()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SequencerId, SequencerConnectionSuccessor]] =
    parent.sequencerConnectionSuccessors()
}

class CachingTopologySnapshot(
    parent: TopologySnapshotLoader,
    cachingConfigs: CachingConfigs,
    batchingConfig: BatchingConfig,
    val loggerFactory: NamedLoggerFactory,
    val futureSupervisor: FutureSupervisor,
)(implicit
    val executionContext: ExecutionContext
) extends TopologySnapshotLoader
    with NamedLogging {

  override def timestamp: CantonTimestamp = parent.timestamp

  private val partyCache: TracedAsyncLoadingCache[FutureUnlessShutdown, PartyId, PartyInfo] =
    ScaffeineCache.buildTracedAsync[FutureUnlessShutdown, PartyId, PartyInfo](
      cache = cachingConfigs.partyCache.buildScaffeine(loggerFactory),
      loader = implicit traceContext =>
        party => parent.loadActiveParticipantsOf(party, loadParticipantStates(_)),
      allLoader = Some(implicit traceContext =>
        parties => parent.loadBatchActiveParticipantsOf(parties.toSeq, loadParticipantStates(_))
      ),
    )(logger, "partyCache")

  private val participantCache: TracedAsyncLoadingCache[FutureUnlessShutdown, ParticipantId, Option[
    ParticipantAttributes
  ]] =
    ScaffeineCache
      .buildTracedAsync[FutureUnlessShutdown, ParticipantId, Option[ParticipantAttributes]](
        cache = cachingConfigs.participantCache.buildScaffeine(loggerFactory),
        loader = implicit traceContext => pid => parent.findParticipantState(pid),
        allLoader = Some { implicit traceContext => pids =>
          parent
            .loadParticipantStates(pids.toSeq)
            .map(attributes =>
              // make sure that the returned map contains an entry for each input element
              pids.map(pid => pid -> attributes.get(pid)).toMap
            )
        },
      )(logger, "participantCache")

  private val keyCache: TracedAsyncLoadingCache[FutureUnlessShutdown, Member, KeyCollection] =
    ScaffeineCache.buildTracedAsync[FutureUnlessShutdown, Member, KeyCollection](
      cache = cachingConfigs.keyCache.buildScaffeine(loggerFactory),
      loader = implicit traceContext => member => parent.allKeys(member),
      allLoader = Some(implicit traceContext =>
        members =>
          parent
            .allKeys(members.toSeq)
            .map(foundKeys =>
              // make sure that the returned map contains an entry for each input element
              members
                .map(member => member -> foundKeys.getOrElse(member, KeyCollection.empty))
                .toMap
            )
      ),
    )(logger, "keyCache")

  private val packageVettingCache: TracedAsyncLoadingCache[
    FutureUnlessShutdown,
    ParticipantId,
    Map[PackageId, VettedPackage],
  ] = ScaffeineCache
    .buildTracedAsync[FutureUnlessShutdown, ParticipantId, Map[PackageId, VettedPackage]](
      cache = cachingConfigs.packageVettingCache.buildScaffeine(loggerFactory),
      loader = implicit traceContext => x => parent.loadVettedPackages(x),
      allLoader =
        Some(implicit traceContext => participants => parent.loadVettedPackages(participants.toSet)),
    )(logger, "packageVettingCache")

  private val mediatorsCache =
    new AtomicReference[Option[FutureUnlessShutdown[Seq[MediatorGroup]]]](None)

  private val sequencerGroupCache =
    new AtomicReference[Option[FutureUnlessShutdown[Option[SequencerGroup]]]](None)

  private val allMembersCache = new AtomicReference[Option[FutureUnlessShutdown[Set[Member]]]](None)
  private val memberCache: TracedAsyncLoadingCache[FutureUnlessShutdown, Member, Boolean] =
    ScaffeineCache.buildTracedAsync[FutureUnlessShutdown, Member, Boolean](
      cache = cachingConfigs.memberCache.buildScaffeine(loggerFactory),
      loader = implicit traceContext => member => parent.isMemberKnown(member),
      allLoader = Some(implicit traceContext =>
        members =>
          parent
            .areMembersKnown(members.toSet)
            .map(knownMembers => members.map(m => m -> knownMembers.contains(m)).toMap)
      ),
    )(logger, "memberCache")

  private val synchronizerParametersCache =
    new AtomicReference[
      Option[FutureUnlessShutdown[Either[String, DynamicSynchronizerParametersWithValidity]]]
    ](None)

  private val sequencingDynamicParametersCache =
    new AtomicReference[
      Option[FutureUnlessShutdown[Either[String, DynamicSequencingParametersWithValidity]]]
    ](
      None
    )

  private val synchronizerParametersChangesCache =
    new AtomicReference[
      Option[FutureUnlessShutdown[Seq[DynamicSynchronizerParametersWithValidity]]]
    ](None)

  private val signingKeysWithThresholdCache: TracedAsyncLoadingCache[
    FutureUnlessShutdown,
    PartyId,
    Option[SigningKeysWithThreshold],
  ] = ScaffeineCache.buildTracedAsync[
    FutureUnlessShutdown,
    PartyId,
    Option[SigningKeysWithThreshold],
  ](
    cache = cachingConfigs.partyCache.buildScaffeine(loggerFactory),
    loader = implicit traceContext => party => parent.signingKeysWithThreshold(party),
  )(logger, "signingKeysWithThresholdCache")

  private val synchronizerUpgradeCache =
    new AtomicReference[
      Option[FutureUnlessShutdown[Option[(SynchronizerSuccessor, EffectiveTime)]]]
    ](None)

  override def allKeys(owner: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[KeyCollection] =
    keyCache.get(owner)

  override def allKeys(
      members: Seq[Member]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[Member, KeyCollection]] =
    keyCache.getAll(members)

  override def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => FutureUnlessShutdown[
        Map[ParticipantId, ParticipantAttributes]
      ],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[PartyInfo] =
    partyCache.get(party)

  override private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => FutureUnlessShutdown[
        Map[ParticipantId, ParticipantAttributes]
      ],
  )(implicit traceContext: TraceContext) =
    getAllBatched(parties)(partyCache.getAll(_)(traceContext))

  override def loadParticipantStates(
      participants: Seq[ParticipantId]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, ParticipantAttributes]] =
    participantCache.getAll(participants).map(_.collect { case (k, Some(v)) => (k, v) })

  override def loadVettedPackages(participant: ParticipantId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PackageId, VettedPackage]] =
    packageVettingCache.get(participant)

  override def loadVettedPackages(participants: Set[ParticipantId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, Map[PackageId, VettedPackage]]] =
    getAllBatched(participants.toSeq)(packageVettingCache.getAll(_)(traceContext))

  private[client] override def findUnvettedPackagesOrDependencies(
      participant: ParticipantId,
      packages: Set[PackageId],
      ledgerTime: CantonTimestamp,
      vettedPackages: Map[PackageId, VettedPackage],
  )(implicit traceContext: TraceContext): UnknownOrUnvettedPackages =
    parent.findUnvettedPackagesOrDependencies(participant, packages, ledgerTime, vettedPackages)

  override def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[MemberCode],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[Member, KeyCollection]] =
    parent.inspectKeys(filterOwner, filterOwnerType, limit)

  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]] =
    parent.inspectKnownParties(filterParty, filterParticipant, limit)

  /** returns the list of currently known mediators */
  override def mediatorGroups()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[MediatorGroup]] =
    getAndCache(mediatorsCache, parent.mediatorGroups())

  /** returns the sequencer group if known */
  override def sequencerGroup()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[SequencerGroup]] =
    getAndCache(sequencerGroupCache, parent.sequencerGroup())

  /** returns the set of all known members */
  override def allMembers()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[Member]] =
    getAndCache(allMembersCache, parent.allMembers())

  override def isMemberKnown(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    memberCache.get(member)

  override def areMembersKnown(members: Set[Member])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[Member]] =
    memberCache.getAll(members).map(_.collect { case (member, _isKnown @ true) => member }.toSet)

  override def memberFirstKnownAt(
      member: Member
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] =
    parent.memberFirstKnownAt(member)

  /** Returns the value if it is present in the cache. Otherwise, use the `getter` to fetch it and
    * cache the result.
    */
  private def getAndCache[T](
      cache: AtomicReference[Option[FutureUnlessShutdown[T]]],
      getter: => FutureUnlessShutdown[T],
  )(implicit errorLoggingContext: ErrorLoggingContext): FutureUnlessShutdown[T] = {
    val promise = PromiseUnlessShutdown.supervised[T]("getAndCache", futureSupervisor)
    val previousO = cache.getAndSet(Some(promise.futureUS))
    promise.completeWithUS(previousO.getOrElse(getter)).discard
    promise.futureUS
  }

  override def findDynamicSynchronizerParameters()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, DynamicSynchronizerParametersWithValidity]] =
    getAndCache(synchronizerParametersCache, parent.findDynamicSynchronizerParameters())

  override def findDynamicSequencingParameters()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, DynamicSequencingParametersWithValidity]] =
    getAndCache(sequencingDynamicParametersCache, parent.findDynamicSequencingParameters())

  /** List all the dynamic synchronizer parameters (past and current) */
  override def listDynamicSynchronizerParametersChanges()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[DynamicSynchronizerParametersWithValidity]] =
    getAndCache(
      synchronizerParametersChangesCache,
      parent.listDynamicSynchronizerParametersChanges(),
    )

  override def signingKeysWithThreshold(party: PartyId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[SigningKeysWithThreshold]] =
    signingKeysWithThresholdCache.get(party)

  private def getAllBatched[K, V](
      keys: Seq[K]
  )(fetchAll: Seq[K] => FutureUnlessShutdown[Map[K, V]]): FutureUnlessShutdown[Map[K, V]] =
    // split up the request into separate chunks so that we don't block the cache for too long
    // when loading very large batches
    MonadUtil
      .batchedSequentialTraverse(batchingConfig.parallelism, batchingConfig.maxItemsInBatch)(keys)(
        fetchAll(_).map(_.toSeq)
      )
      .map(_.toMap)

  override def synchronizerUpgradeOngoing()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SynchronizerSuccessor, EffectiveTime)]] =
    getAndCache(synchronizerUpgradeCache, parent.synchronizerUpgradeOngoing())

  override def sequencerConnectionSuccessors()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SequencerId, SequencerConnectionSuccessor]] =
    parent.sequencerConnectionSuccessors()
}
