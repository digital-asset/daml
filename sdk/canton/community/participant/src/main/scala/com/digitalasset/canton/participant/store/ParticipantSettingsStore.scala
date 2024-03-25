// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.ResourceLimits
import com.digitalasset.canton.participant.store.ParticipantSettingsStore.Settings
import com.digitalasset.canton.participant.store.db.DbParticipantSettingsStore
import com.digitalasset.canton.participant.store.memory.InMemoryParticipantSettingsStore
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

/** Read-only interface for [[ParticipantSettingsStore]] */
trait ParticipantSettingsLookup {

  /** @return the current settings
    * @throws java.lang.IllegalStateException if [[ParticipantSettingsStore.refreshCache]] has not previously been run successfully
    */
  def settings: Settings
}

/** Stores misc settings for a participant.
  * Allows clients to read settings without accessing the database.
  * In turn, a client needs to call `refreshCache` before reading settings.
  */
trait ParticipantSettingsStore extends ParticipantSettingsLookup with AutoCloseable {

  /** A cache for the max number of dirty requests.
    * It is updated in the following situations:
    * - Before and after a write (successful or not).
    * - Through [[refreshCache]]. (Clients are requested to call [[refreshCache]] before reading any value.)
    */
  protected val cache: AtomicReference[Option[Settings]] = new AtomicReference(None)

  override final def settings: Settings =
    cache
      .get()
      .getOrElse(
        throw new IllegalStateException(
          "You need to initialize the cache before querying settings."
        )
      )

  def writeResourceLimits(resourceLimits: ResourceLimits)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Insert the given max deduplication duration provided unless a max deduplication duration has been set previously. */
  def insertMaxDeduplicationDuration(maxDeduplicationDuration: NonNegativeFiniteDuration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  def refreshCache()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]
}

object ParticipantSettingsStore {
  def apply(
      storage: Storage,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): ParticipantSettingsStore = {
    storage match {
      case _: MemoryStorage => new InMemoryParticipantSettingsStore(loggerFactory)
      case storage: DbStorage =>
        new DbParticipantSettingsStore(storage, timeouts, futureSupervisor, loggerFactory)
    }
  }

  final case class Settings(
      resourceLimits: ResourceLimits = ResourceLimits.default,
      maxDeduplicationDuration: Option[NonNegativeFiniteDuration] = None,
  )
}
