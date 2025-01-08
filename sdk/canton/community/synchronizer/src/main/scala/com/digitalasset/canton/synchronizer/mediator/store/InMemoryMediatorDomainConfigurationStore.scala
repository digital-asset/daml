// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator.store

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference

class InMemoryMediatorDomainConfigurationStore extends MediatorDomainConfigurationStore {
  private val currentConfiguration = new AtomicReference[Option[MediatorDomainConfiguration]](None)

  override def fetchConfiguration(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[MediatorDomainConfiguration]] =
    FutureUnlessShutdown.pure(currentConfiguration.get())

  override def saveConfiguration(configuration: MediatorDomainConfiguration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    currentConfiguration.set(Some(configuration))
    FutureUnlessShutdown.unit
  }

  override def close(): Unit = ()
}
