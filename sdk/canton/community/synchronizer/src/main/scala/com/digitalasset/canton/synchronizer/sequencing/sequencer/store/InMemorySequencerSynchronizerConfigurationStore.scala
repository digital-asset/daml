// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.store

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

class InMemorySequencerSynchronizerConfigurationStore(implicit executionContext: ExecutionContext)
    extends SequencerSynchronizerConfigurationStore {
  private val currentConfiguration =
    new AtomicReference[Option[SequencerSynchronizerConfiguration]](None)

  override def fetchConfiguration(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerSynchronizerConfigurationStoreError, Option[
    SequencerSynchronizerConfiguration
  ]] =
    EitherT.pure(currentConfiguration.get())

  override def saveConfiguration(configuration: SequencerSynchronizerConfiguration)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerSynchronizerConfigurationStoreError, Unit] = {
    currentConfiguration.set(Some(configuration))
    EitherT.pure(())
  }
}
