// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import cats.data.EitherT
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

final case class SequencerDomainConfiguration(
    domainId: DomainId,
    domainParameters: StaticDomainParameters,
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
  ): EitherT[Future, SequencerDomainConfigurationStoreError, Option[SequencerDomainConfiguration]]
  def saveConfiguration(configuration: SequencerDomainConfiguration)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerDomainConfigurationStoreError, Unit]
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
