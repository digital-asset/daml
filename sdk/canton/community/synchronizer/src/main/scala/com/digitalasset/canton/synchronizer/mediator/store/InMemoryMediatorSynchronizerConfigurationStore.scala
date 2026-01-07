// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator.store

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

class InMemoryMediatorSynchronizerConfigurationStore
    extends MediatorSynchronizerConfigurationStore {
  private val currentConfiguration =
    new AtomicReference[Option[MediatorSynchronizerConfiguration]](None)

  private val isTopologyInitialized_ : AtomicBoolean = new AtomicBoolean(false)

  override def fetchConfiguration()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[MediatorSynchronizerConfiguration]] =
    FutureUnlessShutdown.pure(currentConfiguration.get())

  override def saveConfiguration(configuration: MediatorSynchronizerConfiguration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    currentConfiguration.set(Some(configuration))
    FutureUnlessShutdown.unit
  }

  override def setTopologyInitialized()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    isTopologyInitialized_.set(true)
    FutureUnlessShutdown.unit
  }

  override def isTopologyInitialized()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    FutureUnlessShutdown.pure(isTopologyInitialized_.get())

  override def close(): Unit = ()
}
