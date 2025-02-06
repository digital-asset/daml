// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.SynchronizerParameterStore
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference

class InMemorySynchronizerParameterStore extends SynchronizerParameterStore {

  private val currentParameters: AtomicReference[Option[StaticSynchronizerParameters]] =
    new AtomicReference[Option[StaticSynchronizerParameters]](None)

  override def setParameters(
      newParameters: StaticSynchronizerParameters
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val previous = currentParameters.getAndUpdate { old =>
      if (old.forall(_ == newParameters)) Some(newParameters) else old
    }
    if (previous.exists(_ != newParameters))
      FutureUnlessShutdown.failed(
        new IllegalArgumentException(
          s"Cannot overwrite old synchronizer parameters with $newParameters."
        )
      )
    else FutureUnlessShutdown.unit
  }

  override def lastParameters(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[StaticSynchronizerParameters]] =
    FutureUnlessShutdown.pure(currentParameters.get)
}
