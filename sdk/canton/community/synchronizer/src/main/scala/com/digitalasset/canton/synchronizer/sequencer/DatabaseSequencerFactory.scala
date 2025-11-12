// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import com.digitalasset.canton.config.{BatchingConfig, CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerStore
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

abstract class DatabaseSequencerFactory(
    config: DatabaseSequencerConfig,
    storage: Storage,
    cachingConfigs: CachingConfigs,
    batchingConfig: BatchingConfig,
    override val timeouts: ProcessingTimeout,
    protocolVersion: ProtocolVersion,
    sequencerId: SequencerId,
    blockSequencerMode: Boolean,
    metrics: SequencerMetrics,
)(implicit ec: ExecutionContext)
    extends SequencerFactory
    with NamedLogging {

  val sequencerStore: SequencerStore =
    SequencerStore(
      storage = storage,
      protocolVersion = protocolVersion,
      bufferedEventsMaxMemory = config.writer.bufferedEventsMaxMemory,
      bufferedEventsPreloadBatchSize = config.writer.bufferedEventsPreloadBatchSize,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
      sequencerMember = sequencerId,
      blockSequencerMode = blockSequencerMode,
      cachingConfigs = cachingConfigs,
      batchingConfig = batchingConfig,
      // Overriding the store's close context with the writers, so that when the writer gets closed, the store
      // stops retrying forever
      overrideCloseContext = Some(this.closeContext),
      sequencerMetrics = metrics,
    )

  override def initialize(
      initialState: SequencerInitialState,
      sequencerId: SequencerId,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    sequencerStore.initializeFromSnapshot(initialState)
}
