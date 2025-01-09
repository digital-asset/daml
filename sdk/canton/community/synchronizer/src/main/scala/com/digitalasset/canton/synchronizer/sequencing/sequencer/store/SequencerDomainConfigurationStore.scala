// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.store

import cats.data.EitherT
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

final case class SequencerDomainConfiguration(
    synchronizerId: SynchronizerId,
    synchronizerParameters: StaticSynchronizerParameters,
)

sealed trait SequencerDomainConfigurationStoreError

object SequencerDomainConfigurationStoreError {
  final case class DbError(exception: Throwable) extends SequencerDomainConfigurationStoreError
  final case class DeserializationError(deserializationError: ProtoDeserializationError)
      extends SequencerDomainConfigurationStoreError
}

trait SequencerDomainConfigurationStore {
  def fetchConfiguration(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerDomainConfigurationStoreError, Option[
    SequencerDomainConfiguration
  ]]
  def saveConfiguration(configuration: SequencerDomainConfiguration)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerDomainConfigurationStoreError, Unit]
}

object SequencerDomainConfigurationStore {
  def apply(storage: Storage, timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(
      implicit executionContext: ExecutionContext
  ): SequencerDomainConfigurationStore =
    storage match {
      case _: MemoryStorage => new InMemorySequencerDomainConfigurationStore
      case storage: DbStorage =>
        new DbSequencerDomainConfigurationStore(storage, timeouts, loggerFactory)
    }
}
