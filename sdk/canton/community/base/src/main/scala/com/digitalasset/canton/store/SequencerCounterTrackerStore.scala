// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import com.digitalasset.canton.SequencerCounterDiscriminator
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.CursorPrehead.SequencerCounterCursorPrehead
import com.digitalasset.canton.store.db.DbSequencerCounterTrackerStore
import com.digitalasset.canton.store.memory.InMemorySequencerCounterTrackerStore
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** Store for keeping track of the prehead for clean sequencer counters.
  * A [[com.digitalasset.canton.SequencerCounter]] becomes clean
  * when the corresponding [[com.digitalasset.canton.sequencing.protocol.SequencedEvent]] has been processed
  * completely and successfully.
  * The prehead of the cursor is advanced only so far that all sequencer counters up to the prehead are clean.
  */
trait SequencerCounterTrackerStore extends FlagCloseable {
  protected[store] val cursorStore: CursorPreheadStore[SequencerCounterDiscriminator]

  /** Gets the prehead clean sequencer counter. This sequencer counter and all the ones below are assumed to be clean. */
  def preheadSequencerCounter(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[SequencerCounterCursorPrehead]] =
    cursorStore.prehead

  /** Sets the prehead clean sequencer counter to `sequencerCounter` unless it has previously been set to a higher value. */
  def advancePreheadSequencerCounterTo(
      sequencerCounter: SequencerCounterCursorPrehead
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] =
    cursorStore.advancePreheadTo(sequencerCounter)

  /** Rewinds the prehead clean sequencer counter to `newPrehead` unless the prehead is already at or before the new `preHead`. */
  def rewindPreheadSequencerCounter(
      newPreheadO: Option[SequencerCounterCursorPrehead]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    cursorStore.rewindPreheadTo(newPreheadO)
}

object SequencerCounterTrackerStore {
  def apply(
      storage: Storage,
      indexedSynchronizer: IndexedSynchronizer,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): SequencerCounterTrackerStore = storage match {
    case _: MemoryStorage => new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
    case dbStorage: DbStorage =>
      new DbSequencerCounterTrackerStore(indexedSynchronizer, dbStorage, timeouts, loggerFactory)
  }
}
