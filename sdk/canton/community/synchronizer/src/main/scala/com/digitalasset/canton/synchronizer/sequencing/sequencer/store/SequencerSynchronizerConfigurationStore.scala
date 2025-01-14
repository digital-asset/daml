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

final case class SequencerSynchronizerConfiguration(
    synchronizerId: SynchronizerId,
    synchronizerParameters: StaticSynchronizerParameters,
)

sealed trait SequencerSynchronizerConfigurationStoreError

object SequencerSynchronizerConfigurationStoreError {
  final case class DbError(exception: Throwable)
      extends SequencerSynchronizerConfigurationStoreError
  final case class DeserializationError(deserializationError: ProtoDeserializationError)
      extends SequencerSynchronizerConfigurationStoreError
}

trait SequencerSynchronizerConfigurationStore {
  def fetchConfiguration(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerSynchronizerConfigurationStoreError, Option[
    SequencerSynchronizerConfiguration
  ]]
  def saveConfiguration(configuration: SequencerSynchronizerConfiguration)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerSynchronizerConfigurationStoreError, Unit]
}

object SequencerSynchronizerConfigurationStore {
  def apply(storage: Storage, timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(
      implicit executionContext: ExecutionContext
  ): SequencerSynchronizerConfigurationStore =
    storage match {
      case _: MemoryStorage => new InMemorySequencerSynchronizerConfigurationStore
      case storage: DbStorage =>
        new DbSequencerSynchronizerConfigurationStore(storage, timeouts, loggerFactory)
    }
}
