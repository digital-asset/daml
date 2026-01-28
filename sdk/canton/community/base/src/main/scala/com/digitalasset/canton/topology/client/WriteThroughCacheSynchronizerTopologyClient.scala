// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.caching.ScaffeineCache.TracedAsyncLoadingCache
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{CachingConfigs, ProcessingTimeout, TopologyConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.cache.TopologyStateLookup
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{
  PackageDependencyResolver,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

/** A topology client that produces topology snapshots backed by the topology state write through
  * cache.
  *
  * Since the cache transparently loads data on demand into the cache and can inspect the topology
  * state at any time, there is no need to manage snapshots at certain timestamps. We do return new
  * instances of the snapshot to get the timestamps correct, but the actual underlying data is
  * shared through the `stateLookup`.
  */
class WriteThroughCacheSynchronizerTopologyClient(
    delegate: StoreBasedSynchronizerTopologyClient,
    stateLookup: TopologyStateLookup,
    store: TopologyStore[TopologyStoreId.SynchronizerStore],
    packageDependencyResolver: PackageDependencyResolver,
    cachingConfigs: CachingConfigs,
    val timeouts: ProcessingTimeout,
    override protected val futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext
) extends SynchronizerTopologyClientWithInit
    with NamedLogging {

  /** Returns a snapshot with the state of [[latestTopologyChangeTimestamp]], but using
    * [[topologyKnownUntilTimestamp]] as the timestamp for the snapshot.
    *
    * If these timestamps are the same, the event most recently processed by the topology processor
    * was a topology transaction.
    */
  override def headSnapshot(implicit traceContext: TraceContext): TopologySnapshot =
    new ForwardingTopologySnapshot(
      topologyKnownUntilTimestamp,
      new WriteThroughCacheTopologySnapshot(
        delegate.psid,
        stateLookup,
        store,
        packageDependencyResolver,
        latestTopologyChangeTimestamp,
        loggerFactory,
      ),
      loggerFactory,
    )

  override def hypotheticalSnapshot(timestamp: CantonTimestamp, desiredTimestamp: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshot] =
    snapshot(timestamp).map(
      new ForwardingTopologySnapshot(
        desiredTimestamp,
        _,
        loggerFactory,
      )
    )

  override def snapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[TopologySnapshotLoader] =
    waitForTimestampWithLogging(timestamp).map(_ =>
      new WriteThroughCacheTopologySnapshot(
        delegate.psid,
        stateLookup,
        store,
        packageDependencyResolver,
        timestamp,
        loggerFactory,
      )
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

  override def awaitMaxTimestamp(sequencedTime: SequencedTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] =
    maxTimestampCache.get(sequencedTime)

  override def latestTopologyChangeTimestamp: CantonTimestamp =
    delegate.latestTopologyChangeTimestamp

  override def initialize(
      sequencerSnapshotTimestamp: Option[EffectiveTime],
      synchronizerUpgradeTime: Option[SequencedTime],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    delegate.initialize(sequencerSnapshotTimestamp, synchronizerUpgradeTime)

  override def staticSynchronizerParameters: StaticSynchronizerParameters =
    delegate.staticSynchronizerParameters

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

  override def setSynchronizerTimeTracker(tracker: SynchronizerTimeTracker): Unit = {
    delegate.setSynchronizerTimeTracker(tracker)
    super.setSynchronizerTimeTracker(tracker)
  }

  override def close(): Unit = {
    LifeCycle.close(delegate)(logger)
    maxTimestampCache.invalidateAll()
    maxTimestampCache.cleanUp()
  }
}

object WriteThroughCacheSynchronizerTopologyClient {
  def create(
      clock: Clock,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      store: TopologyStore[TopologyStoreId.SynchronizerStore],
      stateLookup: TopologyStateLookup,
      synchronizerUpgradeTime: Option[CantonTimestamp],
      packageDependencyResolver: PackageDependencyResolver,
      cachingConfigs: CachingConfigs,
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
      new WriteThroughCacheSynchronizerTopologyClient(
        dbClient,
        stateLookup,
        store,
        packageDependencyResolver,
        cachingConfigs,
        timeouts,
        futureSupervisor,
        loggerFactory,
      )
    caching
      .initialize(sequencerSnapshotTimestamp, synchronizerUpgradeTime.map(SequencedTime(_)))
      .map(_ => caching)
  }

}
