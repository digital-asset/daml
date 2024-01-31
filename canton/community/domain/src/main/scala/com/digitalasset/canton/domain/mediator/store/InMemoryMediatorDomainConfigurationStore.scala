// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.store

import cats.data.EitherT
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class InMemoryMediatorDomainConfigurationStore(implicit executionContext: ExecutionContext)
    extends MediatorDomainConfigurationStore {
  private val currentConfiguration = new AtomicReference[Option[MediatorDomainConfiguration]](None)

  override def fetchConfiguration(implicit
      traceContext: TraceContext
  ): EitherT[Future, MediatorDomainConfigurationStoreError, Option[MediatorDomainConfiguration]] =
    EitherT.pure(currentConfiguration.get())

  override def saveConfiguration(configuration: MediatorDomainConfiguration)(implicit
      traceContext: TraceContext
  ): EitherT[Future, MediatorDomainConfigurationStoreError, Unit] = {
    currentConfiguration.set(Some(configuration))
    EitherT.pure(())
  }

  override def close(): Unit = ()
}
