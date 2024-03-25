// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.store.DomainParameterStore
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

class InMemoryDomainParameterStore extends DomainParameterStore {

  private val currentParameters: AtomicReference[Option[StaticDomainParameters]] =
    new AtomicReference[Option[StaticDomainParameters]](None)

  override def setParameters(
      newParameters: StaticDomainParameters
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val previous = currentParameters.getAndUpdate { old =>
      if (old.forall(_ == newParameters)) Some(newParameters) else old
    }
    if (previous.exists(_ != newParameters))
      Future.failed(
        new IllegalArgumentException(s"Cannot overwrite old domain parameters with $newParameters.")
      )
    else Future.unit
  }

  override def lastParameters(implicit
      traceContext: TraceContext
  ): Future[Option[StaticDomainParameters]] = Future.successful(currentParameters.get)
}
