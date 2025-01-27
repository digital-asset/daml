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

final case class MediatorSynchronizerConfiguration(
    synchronizerId: SynchronizerId,
    synchronizerParameters: StaticSynchronizerParameters,
    sequencerConnections: SequencerConnections,
)

trait MediatorSynchronizerConfigurationStore extends AutoCloseable {
  def fetchConfiguration(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[MediatorSynchronizerConfiguration]]
  def saveConfiguration(configuration: MediatorSynchronizerConfiguration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]
}

object MediatorSynchronizerConfigurationStore {
  def apply(storage: Storage, timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(
      implicit executionContext: ExecutionContext
  ): MediatorSynchronizerConfigurationStore =
    storage match {
      case _: MemoryStorage => new InMemoryMediatorSynchronizerConfigurationStore
      case storage: DbStorage =>
        new DbMediatorSynchronizerConfigurationStore(storage, timeouts, loggerFactory)
    }
}
