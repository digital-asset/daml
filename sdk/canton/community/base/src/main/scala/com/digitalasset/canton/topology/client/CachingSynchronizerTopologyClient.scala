// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.caching.ScaffeineCache.TracedAsyncLoadingCache
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{BatchingConfig, CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle, PromiseUnlessShutdown}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{
  DynamicSequencingParametersWithValidity,
  DynamicSynchronizerParametersWithValidity,
}
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.processing.*
import com.digitalasset.canton.topology.store.{
  PackageDependencyResolverUS,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.daml.lf.data.Ref.PackageId

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

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

  override def updateHead(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      approximateTimestamp: ApproximateTime,
      potentialTopologyChange: Boolean,
  )(implicit traceContext: TraceContext): Unit = {
    if (potentialTopologyChange)
      appendSnapshotForInclusive(effectiveTimestamp)
    delegate.updateHead(
      sequencedTimestamp,
      effectiveTimestamp,
      approximateTimestamp,
      potentialTopologyChange,
    )
  }

  private val maxTimestampCache: TracedAsyncLoadingCache[
    FutureUnlessShutdown,
    SequencedTime,
    Option[(SequencedTime, EffectiveTime)],
  ] = ScaffeineCache.buildTracedAsync[
    FutureUnlessShutdown,
    SequencedTime,
    Option[(SequencedTime, EffectiveTime)],
  ](
    cache = cachingConfigs.synchronizerClientMaxTimestamp.buildScaffeine(),
    loader = traceContext => delegate.awaitMaxTimestamp(_)(traceContext),
  )(logger, "maxTimestampCache")

  /** An entry with a given `timestamp` refers to the topology snapshot at the same `timestamp`.
    * This is the snapshot that covers all committed topology transactions with `validFrom <
    * timestamp` and `validUntil.forall(timestamp <= _)`, following the topology snapshot and
    * effective time semantics.
    */
  protected class SnapshotEntry(val timestamp: CantonTimestamp) {
    def get(): CachingTopologySnapshot = pointwise.get(timestamp)
  }

  /** List of snapshot timestamps for which snapshots are cached. Invariants:
    *   - Entries are sorted descending by timestamp.
    *   - For every entry, a snapshot at `entry.timestamp` must be available.
    *   - If it contains entries with timestamps `ts1` and `ts3`, if there is a valid topology
    *     transaction at timestamp `ts2`, if `ts1 < ts2 < ts3`, then there must be an entry with
    *     `ts2` as well.
    */
  protected val snapshots = new AtomicReference[List[SnapshotEntry]](List.empty)

  /** Cache of snapshots. We want to avoid loading redundant data from the database. Now, we know
    * that if there was no topology transaction between tx and ty, then snapshot(ty) ==
    * snapshot(tx). Therefore, we remember the list of timestamps when updates happened (in
    * `snapshots`) and use that list in order to figure out which snapshot we can use instead of
    * loading the same data again and again. So we use `snapshots` to figure out the update
    * timestamp and then we use the `pointwise` cache to load the corresponding snapshot.
    */
  private val pointwise = cachingConfigs.topologySnapshot
    .buildScaffeine()
    .build[CantonTimestamp, CachingTopologySnapshot] { (ts: CantonTimestamp) =>
      new CachingTopologySnapshot(
        delegate.trySnapshot(ts)(TraceContext.empty),
        cachingConfigs,
        batchingConfig,
        loggerFactory,
        futureSupervisor,
      )
    }

  // note that this function is inclusive on effective time as opposed to other topology client (and snapshot) functions
  private def appendSnapshotForInclusive(effectiveTime: EffectiveTime): Unit = {
    // topology snapshots are exclusive on effective time, the below "emulates" inclusivity for the given effective time,
    // as we want to make topology changes observable as part of the topology snapshot for the given time
    val snapshotTimestamp = effectiveTime.value.immediateSuccessor
    val _ = snapshots.updateAndGet { cur =>
      if (cur.headOption.exists(_.timestamp >= snapshotTimestamp))
        cur
      else {
        val entry = new SnapshotEntry(snapshotTimestamp)
        val unexpiredEntries = cur.filter(
          _.timestamp.plusMillis(
            cachingConfigs.topologySnapshot.expireAfterAccess.duration.toMillis
          ) >= snapshotTimestamp
        )
        entry :: unexpiredEntries
      }
    }
  }

  override def trySnapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): TopologySnapshotLoader = {
    ErrorUtil.requireArgument(
      timestamp <= topologyKnownUntilTimestamp,
      s"requested snapshot=$timestamp, available snapshot=$topologyKnownUntilTimestamp",
    )
    // find a matching existing snapshot
    // including `<` is safe as it's guarded by the `topologyKnownUntilTimestamp` check,
    //  i.e., there will be no other snapshots in between, and the snapshot timestamp can be safely "overridden"
    val cur = snapshots.get().find(_.timestamp <= timestamp)
    cur match {
      // we'll use the cached snapshot client which defines the time-period this timestamp is in
      case Some(snapshotEntry) =>
        new ForwardingTopologySnapshotClient(timestamp, snapshotEntry.get(), loggerFactory)
      // this timestamp is outside of the window where we have tracked the timestamps of changes.
      // so let's do this pointwise
      case None =>
        pointwise.get(timestamp)
    }
  }

  override def synchronizerId: SynchronizerId = delegate.synchronizerId

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
  ): TopologySnapshotLoader = trySnapshot(approximateTimestamp)

  override def topologyKnownUntilTimestamp: CantonTimestamp = delegate.topologyKnownUntilTimestamp

  override def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Boolean
  ] = // we use our implementation such that we can benefit from cached data
    delegate.scheduleAwait(
      FutureUnlessShutdown.outcomeF(condition(currentSnapshotApproximation)),
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
    delegate.scheduleAwait(condition(currentSnapshotApproximation), timeout)

  override private[topology] def scheduleAwait(
      condition: => FutureUnlessShutdown[Boolean],
      timeout: Duration,
  ) =
    delegate.scheduleAwait(condition, timeout)

  override def close(): Unit =
    LifeCycle.close(delegate)(logger)

  override def numPendingChanges: Int = delegate.numPendingChanges

  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    if (transactions.nonEmpty) {
      // if there is a transaction, we insert the effective timestamp as a snapshot
      appendSnapshotForInclusive(effectiveTimestamp)
    } else if (snapshots.get().isEmpty) {
      // if we haven't seen any snapshot yet, we use the sequencer time to seed the first snapshot
      appendSnapshotForInclusive(EffectiveTime(sequencedTimestamp.value))
    }
    delegate.observed(sequencedTimestamp, effectiveTimestamp, sequencerCounter, transactions)
  }

  override def setSynchronizerTimeTracker(tracker: SynchronizerTimeTracker): Unit = {
    delegate.setSynchronizerTimeTracker(tracker)
    super.setSynchronizerTimeTracker(tracker)
  }

  override def awaitMaxTimestamp(sequencedTime: SequencedTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] =
    maxTimestampCache.get(sequencedTime)
}

object CachingSynchronizerTopologyClient {

  def create(
      clock: Clock,
      synchronizerId: SynchronizerId,
      store: TopologyStore[TopologyStoreId.SynchronizerStore],
      packageDependenciesResolver: PackageDependencyResolverUS,
      cachingConfigs: CachingConfigs,
      batchingConfig: BatchingConfig,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(
      headStateInitializer: SynchronizerTopologyClientHeadStateInitializer =
        new DefaultHeadStateInitializer(store)
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[SynchronizerTopologyClientWithInit] = {
    val dbClient =
      new StoreBasedSynchronizerTopologyClient(
        clock,
        synchronizerId,
        store,
        packageDependenciesResolver,
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
    headStateInitializer.initialize(caching)
  }
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
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]] =
    parent.inspectKnownParties(filterParty, filterParticipant)

  override def loadVettedPackages(participant: ParticipantId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PackageId, VettedPackage]] = parent.loadVettedPackages(participant)

  override private[client] def loadUnvettedPackagesOrDependenciesUsingLoader(
      participant: ParticipantId,
      packageId: PackageId,
      ledgerTime: CantonTimestamp,
      vettedPackagesLoader: VettedPackagesLoader,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PackageId]] =
    parent.loadUnvettedPackagesOrDependenciesUsingLoader(
      participant,
      packageId,
      ledgerTime,
      vettedPackagesLoader,
    )

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

  override def partyAuthorization(party: PartyId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[PartyKeyTopologySnapshotClient.PartyAuthorizationInfo]] =
    parent.partyAuthorization(party)
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
      cache = cachingConfigs.partyCache.buildScaffeine(),
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
        cache = cachingConfigs.participantCache.buildScaffeine(),
        loader = implicit traceContext => pid => parent.findParticipantState(pid),
        allLoader = Some(implicit traceContext =>
          pids =>
            parent.loadParticipantStates(pids.toSeq).map { attributes =>
              // make sure that the returned map contains an entry for each input element
              pids.map(pid => pid -> attributes.get(pid)).toMap
            }
        ),
      )(logger, "participantCache")
  private val keyCache: TracedAsyncLoadingCache[FutureUnlessShutdown, Member, KeyCollection] =
    ScaffeineCache.buildTracedAsync[FutureUnlessShutdown, Member, KeyCollection](
      cache = cachingConfigs.keyCache.buildScaffeine(),
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
      cache = cachingConfigs.packageVettingCache.buildScaffeine(),
      loader = implicit traceContext => x => parent.loadVettedPackages(x),
    )(logger, "packageVettingCache")

  private val mediatorsCache =
    new AtomicReference[Option[FutureUnlessShutdown[Seq[MediatorGroup]]]](None)

  private val sequencerGroupCache =
    new AtomicReference[Option[FutureUnlessShutdown[Option[SequencerGroup]]]](None)

  private val allMembersCache = new AtomicReference[Option[FutureUnlessShutdown[Set[Member]]]](None)
  private val memberCache: TracedAsyncLoadingCache[FutureUnlessShutdown, Member, Boolean] =
    ScaffeineCache.buildTracedAsync[FutureUnlessShutdown, Member, Boolean](
      cache = cachingConfigs.memberCache.buildScaffeine(),
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

  private val partyAuthorizationsCache: TracedAsyncLoadingCache[
    FutureUnlessShutdown,
    PartyId,
    Option[PartyKeyTopologySnapshotClient.PartyAuthorizationInfo],
  ] = ScaffeineCache.buildTracedAsync[
    FutureUnlessShutdown,
    PartyId,
    Option[PartyKeyTopologySnapshotClient.PartyAuthorizationInfo],
  ](
    cache = cachingConfigs.partyCache.buildScaffeine(),
    loader = implicit traceContext => party => parent.partyAuthorization(party),
  )(logger, "partyAuthorizationsCache")

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
    // split up the request into separate chunks so that we don't block the cache for too long
    // when loading very large batches
    MonadUtil
      .batchedSequentialTraverse(batchingConfig.parallelism, batchingConfig.maxItemsInBatch)(
        parties
      )(parties => partyCache.getAll(parties)(traceContext).map(_.toSeq))
      .map(_.toMap)

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

  private[client] def loadUnvettedPackagesOrDependenciesUsingLoader(
      participant: ParticipantId,
      packageId: PackageId,
      ledgerTime: CantonTimestamp,
      vettedPackagesLoader: VettedPackagesLoader,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PackageId]] =
    parent.loadUnvettedPackagesOrDependenciesUsingLoader(
      participant,
      packageId,
      ledgerTime,
      // use the caching vetted package loader
      this,
    )

  override def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[MemberCode],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[Member, KeyCollection]] =
    parent.inspectKeys(filterOwner, filterOwnerType, limit)

  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]] =
    parent.inspectKnownParties(filterParty, filterParticipant)

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

  override def partyAuthorization(party: PartyId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[PartyKeyTopologySnapshotClient.PartyAuthorizationInfo]] =
    partyAuthorizationsCache.get(party)
}
