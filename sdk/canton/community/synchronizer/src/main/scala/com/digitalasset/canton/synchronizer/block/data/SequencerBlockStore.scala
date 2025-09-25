// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.data

import cats.data.EitherT
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.synchronizer.block.data.db.DbSequencerBlockStore
import com.digitalasset.canton.synchronizer.block.data.memory.InMemorySequencerBlockStore
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.sequencer.store.{
  DbSequencerStore,
  InMemorySequencerStore,
  SequencerStore,
}
import com.digitalasset.canton.synchronizer.sequencer.{
  InFlightAggregationUpdates,
  SequencerInitialState,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

trait SequencerBlockStore extends AutoCloseable {
  this: NamedLogging =>

  protected def executionContext: ExecutionContext

  /** Set initial state of the sequencer node from which it supports serving requests. This should
    * be called at most once. If not called, it means this sequencer node can server requests from
    * genesis.
    */
  def setInitialState(
      initial: SequencerInitialState,
      maybeOnboardingTopologyEffectiveTimestamp: Option[CantonTimestamp] = None,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** The current state of the sequencer, which can be used when the node is restarted to
    * deterministically derive the following counters and timestamps.
    *
    * The state excludes updates of unfinalized blocks added with [[storeInflightAggregations]].
    */
  def readHead(implicit traceContext: TraceContext): FutureUnlessShutdown[BlockEphemeralState]

  /** The block information for the block that contains the requested timestamp. */
  def findBlockContainingTimestamp(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SequencerError, BlockInfo]

  /** The state at the end of the block that contains the given timestamp. This will typically be
    * used to inform other sequencer nodes being initialized of the initial state they should use
    * based on the timestamp they provide which is typically the timestamp of their signing key.
    *
    * @param timestamp
    *   timestamp within the block being requested (i.e. BlockInfo.lastTs)
    * @param maxSequencingTimeBound
    *   optional bound for requesting the state for the sequencer snapshot, that may be far in the
    *   past, thus needing to bound the db io. Can be computed with
    *   `SequencerUtils.maxSequencingTimeBoundAt`. For requesting the latest state during the
    *   sequencer startup, this can be set to `CantonTimestamp.MaxValue`.
    */
  def readStateForBlockContainingTimestamp(
      timestamp: CantonTimestamp,
      maxSequencingTimeBound: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, BlockEphemeralState]

  def prune(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[String]

  /** Stores some updates that happen in a single block. May be called several times for the same
    * block and the same update may be contained in several of the calls. Before adding updates of a
    * subsequent block, [[finalizeBlockUpdates]] must be called to wrap up the current block.
    *
    * This method must not be called concurrently with itself or [[finalizeBlockUpdates]].
    */
  def storeInflightAggregations(
      inFlightAggregationUpdates: InFlightAggregationUpdates
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** Finalizes the current block whose updates have been added in the calls to
    * [[storeInflightAggregations]] since the last call to [[finalizeBlockUpdates]].
    *
    * This method must not be called concurrently with itself or [[storeInflightAggregations]], and
    * must be called for the blocks in monotonically increasing order of height.
    *
    * @param blocks
    *   The block information about the current block. It is the responsibility of the caller to
    *   ensure that the height increases monotonically by one
    */
  def finalizeBlockUpdates(blocks: Seq[BlockInfo])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]
}

object SequencerBlockStore {
  def apply(
      storage: Storage,
      protocolVersion: ProtocolVersion,
      sequencerStore: SequencerStore,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      batchingConfig: BatchingConfig,
  )(implicit
      executionContext: ExecutionContext
  ): SequencerBlockStore =
    (storage, sequencerStore) match {
      case (_: MemoryStorage, inMemorySequencerStore: InMemorySequencerStore) =>
        new InMemorySequencerBlockStore(inMemorySequencerStore, loggerFactory)
      case (dbStorage: DbStorage, _: DbSequencerStore) =>
        new DbSequencerBlockStore(
          dbStorage,
          protocolVersion,
          timeouts,
          loggerFactory,
          batchingConfig,
        )
      case otherwise =>
        sys.error(s"Invalid combination of stores: $otherwise")
    }
}
