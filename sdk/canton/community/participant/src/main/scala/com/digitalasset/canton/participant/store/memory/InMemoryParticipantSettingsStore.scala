// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.ResourceLimits
import com.digitalasset.canton.participant.store.ParticipantSettingsStore
import com.digitalasset.canton.participant.store.ParticipantSettingsStore.Settings
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext
import monocle.Lens
import monocle.macros.GenLens

class InMemoryParticipantSettingsStore(override protected val loggerFactory: NamedLoggerFactory)
    extends ParticipantSettingsStore
    with NamedLogging {

  def writeResourceLimits(resourceLimits: ResourceLimits)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    updateCache(_.copy(resourceLimits = resourceLimits))

  override def insertMaxDeduplicationDuration(maxDeduplicationDuration: NonNegativeFiniteDuration)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = updateCache(
    setIfEmpty[NonNegativeFiniteDuration](
      GenLens[Settings](_.maxDeduplicationDuration),
      maxDeduplicationDuration,
    )
  )

  private def setIfEmpty[A](lens: Lens[Settings, Option[A]], newValue: A): Settings => Settings = {
    lens.modify {
      case None => Some(newValue)
      case alreadySet => alreadySet
    }
  }

  private def updateCache(f: Settings => Settings): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure {
      cache
        .getAndUpdate {
          case Some(settings) => Some(f(settings))
          case None => Some(f(Settings()))
        }
        .discard[Option[Settings]]
    }

  override def refreshCache()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    cache.updateAndGet(_.orElse(Some(Settings())))
    FutureUnlessShutdown.unit
  }

  override def close(): Unit = ()
}
