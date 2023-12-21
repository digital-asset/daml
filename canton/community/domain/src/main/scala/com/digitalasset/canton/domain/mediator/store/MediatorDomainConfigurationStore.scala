// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.store

import cats.data.EitherT
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

final case class MediatorDomainConfiguration(
    @Deprecated(since = "3.0.0, x-nodes do not need to return the initial key")
    initialKeyFingerprint: Fingerprint,
    domainId: DomainId,
    domainParameters: StaticDomainParameters,
    sequencerConnections: SequencerConnections,
)

sealed trait MediatorDomainConfigurationStoreError

object MediatorDomainConfigurationStoreError {
  final case class DbError(exception: Throwable) extends MediatorDomainConfigurationStoreError
  final case class DeserializationError(deserializationError: ProtoDeserializationError)
      extends MediatorDomainConfigurationStoreError
}

trait MediatorDomainConfigurationStore extends AutoCloseable {
  def fetchConfiguration(implicit
      traceContext: TraceContext
  ): EitherT[Future, MediatorDomainConfigurationStoreError, Option[MediatorDomainConfiguration]]
  def saveConfiguration(configuration: MediatorDomainConfiguration)(implicit
      traceContext: TraceContext
  ): EitherT[Future, MediatorDomainConfigurationStoreError, Unit]
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
