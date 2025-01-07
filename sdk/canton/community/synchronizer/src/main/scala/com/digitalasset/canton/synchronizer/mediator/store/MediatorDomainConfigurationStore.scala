// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator.store

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

final case class MediatorDomainConfiguration(
    synchronizerId: SynchronizerId,
    synchronizerParameters: StaticSynchronizerParameters,
    sequencerConnections: SequencerConnections,
)

trait MediatorDomainConfigurationStore extends AutoCloseable {
  def fetchConfiguration(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[MediatorDomainConfiguration]]
  def saveConfiguration(configuration: MediatorDomainConfiguration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]
}

object MediatorDomainConfigurationStore {
  def apply(storage: Storage, timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(
      implicit executionContext: ExecutionContext
  ): MediatorDomainConfigurationStore =
    storage match {
      case _: MemoryStorage => new InMemoryMediatorDomainConfigurationStore
      case storage: DbStorage =>
        new DbMediatorDomainConfigurationStore(storage, timeouts, loggerFactory)
    }
}
