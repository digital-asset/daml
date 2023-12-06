// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.service.store

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.service.ServiceAgreementAcceptance
import com.digitalasset.canton.domain.service.store.db.DbServiceAgreementAcceptanceStore
import com.digitalasset.canton.domain.service.store.memory.InMemoryServiceAgreementAcceptanceStore
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

/** Stores the accepted service agreements of participants for audit/legal purposes. */
trait ServiceAgreementAcceptanceStore {

  /** Stores the acceptance of a participant of an agreement. */
  def insertAcceptance(acceptance: ServiceAgreementAcceptance)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ServiceAgreementAcceptanceStoreError, Unit]

  def listAcceptances()(implicit
      traceContext: TraceContext
  ): EitherT[Future, ServiceAgreementAcceptanceStoreError, Seq[ServiceAgreementAcceptance]]

}

object ServiceAgreementAcceptanceStore {

  def create(
      storage: Storage,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): ServiceAgreementAcceptanceStore =
    storage match {
      case _: MemoryStorage => new InMemoryServiceAgreementAcceptanceStore(loggerFactory)
      case dbStorage: DbStorage =>
        new DbServiceAgreementAcceptanceStore(dbStorage, protocolVersion, timeouts, loggerFactory)
    }

}

sealed trait ServiceAgreementAcceptanceStoreError

object ServiceAgreementAcceptanceStoreError {

  final case class FailedToStoreAcceptance(reason: String)
      extends ServiceAgreementAcceptanceStoreError

  final case class FailedToListAcceptances(reason: String)
      extends ServiceAgreementAcceptanceStoreError

}
