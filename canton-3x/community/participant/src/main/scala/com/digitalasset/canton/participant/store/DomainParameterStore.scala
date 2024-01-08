// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.db.DbDomainParameterStore
import com.digitalasset.canton.participant.store.memory.InMemoryDomainParameterStore
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

trait DomainParameterStore {

  /** Sets new domain parameters. Calls with the same argument are idempotent.
    *
    * @return The future fails with an [[java.lang.IllegalArgumentException]] if different domain parameters have been stored before.
    */
  def setParameters(newParameters: StaticDomainParameters)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Returns the last set domain parameters, if any. */
  def lastParameters(implicit traceContext: TraceContext): Future[Option[StaticDomainParameters]]
}

object DomainParameterStore {
  def apply(
      storage: Storage,
      domainId: DomainId,
      processingTimeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): DomainParameterStore = {
    storage match {
      case _: MemoryStorage =>
        new InMemoryDomainParameterStore(
        )
      case db: DbStorage =>
        new DbDomainParameterStore(
          domainId,
          db,
          processingTimeouts,
          loggerFactory,
        )
    }
  }
}
