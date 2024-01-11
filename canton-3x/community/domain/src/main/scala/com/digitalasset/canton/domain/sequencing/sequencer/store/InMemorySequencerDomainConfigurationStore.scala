// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import cats.data.EitherT
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class InMemorySequencerDomainConfigurationStore(implicit executionContext: ExecutionContext)
    extends SequencerDomainConfigurationStore {
  private val currentConfiguration = new AtomicReference[Option[SequencerDomainConfiguration]](None)

  override def fetchConfiguration(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerDomainConfigurationStoreError, Option[SequencerDomainConfiguration]] =
    EitherT.pure(currentConfiguration.get())

  override def saveConfiguration(configuration: SequencerDomainConfiguration)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerDomainConfigurationStoreError, Unit] = {
    currentConfiguration.set(Some(configuration))
    EitherT.pure(())
  }
}
