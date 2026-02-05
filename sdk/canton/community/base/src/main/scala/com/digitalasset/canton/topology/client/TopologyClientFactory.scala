// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{CachingConfigs, ProcessingTimeout, TopologyConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.cache.TopologyStateLookup
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.{
  PackageDependencyResolver,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

object TopologyClientFactory {
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
  ): FutureUnlessShutdown[SynchronizerTopologyClientWithInit] =
    WriteThroughCacheSynchronizerTopologyClient.create(
      clock,
      staticSynchronizerParameters,
      store,
      stateLookup,
      synchronizerUpgradeTime,
      packageDependencyResolver,
      cachingConfigs,
      topologyConfig,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )(sequencerSnapshotTimestamp)

}
