// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import com.digitalasset.canton.config.{BatchingConfig, CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.DbStorageMulti
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.SequencerWriterConfig
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.util.BytesUnit
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

/** Sequencer writer storage intended to use a [[resource.DbStorageMulti]]. */
class HASequencerWriterStore(
    override val instanceIndex: Int,
    storage: DbStorageMulti,
    protocolVersion: ProtocolVersion,
    sequencerMember: Member,
    cachingConfigs: CachingConfigs,
    batchingConfig: BatchingConfig,
    sequencerMetrics: SequencerMetrics,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
    closeContext: CloseContext,
)(implicit executionContext: ExecutionContext)
    extends SequencerWriterStore
    with FlagCloseable
    with NamedLogging {
  override def isActive: Boolean = storage.isActive
  override protected[store] val store: SequencerStore =
    SequencerStore(
      storage,
      protocolVersion,
      BytesUnit.zero, // Events cache is not currently supported in HA mode
      SequencerWriterConfig.DefaultBufferedEventsPreloadBatchSize,
      timeouts,
      loggerFactory,
      sequencerMember,
      // TODO(#15837): support HA in unified sequencer
      blockSequencerMode = false,
      cachingConfigs = cachingConfigs,
      batchingConfig = batchingConfig,
      sequencerMetrics = sequencerMetrics,
      Some(closeContext),
    )
  override protected def onClosed(): Unit =
    // ensure this dedicated storage instance is shutdown
    LifeCycle.close(store, storage)(logger)
}
