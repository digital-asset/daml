// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.data

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
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
    * The state excludes updates of unfinalized blocks added with [[partialBlockUpdate]].
    */
  def readHead(implicit traceContext: TraceContext): FutureUnlessShutdown[BlockEphemeralState]

  /** The state at the end of the block that contains the given timestamp. This will typically be
    * used to inform other sequencer nodes being initialized of the initial state they should use
    * based on the timestamp they provide which is typically the timestamp of their signing key.
    */
  def readStateForBlockContainingTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, BlockEphemeralState]

  def prune(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[String]

  /** Stores some updates that happen in a single block. May be called several times for the same
    * block and the same update may be contained in several of the calls. Before adding updates of a
    * subsequent block, [[finalizeBlockUpdate]] must be called to wrap up the current block.
    *
    * This method must not be called concurrently with itself or [[finalizeBlockUpdate]].
    */
  def partialBlockUpdate(
      inFlightAggregationUpdates: InFlightAggregationUpdates
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** Finalizes the current block whose updates have been added in the calls to
    * [[partialBlockUpdate]] since the last call to [[finalizeBlockUpdate]].
    *
    * This method must not be called concurrently with itself or [[partialBlockUpdate]], and must be
    * called for the blocks in monotonically increasing order of height.
    *
    * @param block
    *   The block information about the current block. It is the responsibility of the caller to
    *   ensure that the height increases monotonically by one
    */
  def finalizeBlockUpdate(block: BlockInfo)(implicit
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
        )
      case otherwise =>
        sys.error(s"Invalid combination of stores: $otherwise")
    }
}
