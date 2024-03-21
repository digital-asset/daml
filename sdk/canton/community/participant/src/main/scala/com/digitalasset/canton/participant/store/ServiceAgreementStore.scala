// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import com.digitalasset.canton.common.domain.{ServiceAgreement, ServiceAgreementId}
import com.digitalasset.canton.config.CantonRequireTypes.String256M
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.db.DbServiceAgreementStore
import com.digitalasset.canton.participant.store.memory.InMemoryServiceAgreementStore
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

trait ServiceAgreementStore extends AutoCloseable {

  import ServiceAgreementStore.ServiceAgreementStoreError

  type ServiceAgreementStoreT[A] = EitherT[Future, ServiceAgreementStoreError, A]

  /** Stores the agreement for the domain with the agreement text.
    *
    * Fails if the agreement has been stored already with a different text.
    */
  def storeAgreement(
      domainId: DomainId,
      agreementId: ServiceAgreementId,
      agreementText: String256M,
  )(implicit
      traceContext: TraceContext
  ): ServiceAgreementStoreT[Unit]

  /** List all stored agreements. */
  def listAgreements(implicit traceContext: TraceContext): Future[Seq[(DomainId, ServiceAgreement)]]

  /** Get the agreement text of a stored agreement. */
  def getAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(implicit
      traceContext: TraceContext
  ): ServiceAgreementStoreT[String256M]

  /** Check if the agreement has been stored already. */
  def containsAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(implicit
      traceContext: TraceContext
  ): Future[Boolean]

  /** Store the acceptance of a previously stored agreement. */
  def insertAcceptedAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(implicit
      traceContext: TraceContext
  ): ServiceAgreementStoreT[Unit]

  /** List all accepted agreements for the domain. */
  def listAcceptedAgreements(domainId: DomainId)(implicit
      traceContext: TraceContext
  ): Future[Seq[ServiceAgreementId]]

  /** Check if the given agreement has been accepted for the domain. */
  def containsAcceptedAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(implicit
      traceContext: TraceContext
  ): Future[Boolean]
}

object ServiceAgreementStore {

  def apply(
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): ServiceAgreementStore =
    storage match {
      case dbStorage: DbStorage => new DbServiceAgreementStore(dbStorage, timeouts, loggerFactory)
      case _: MemoryStorage => new InMemoryServiceAgreementStore(loggerFactory)
    }

  sealed trait ServiceAgreementStoreError extends Product with Serializable {
    def description = toString
  }

  final case class UnknownServiceAgreement(domainId: DomainId, agreementId: ServiceAgreementId)
      extends ServiceAgreementStoreError {
    override def toString = s"The agreement '$agreementId' is not known at domain '$domainId'."
  }

  final case class ServiceAgreementAlreadyExists(
      domainId: DomainId,
      existingAgreement: ServiceAgreement,
  ) extends ServiceAgreementStoreError

  final case class FailedToStoreAgreement(
      domainId: DomainId,
      agreementId: ServiceAgreementId,
      override val description: String,
  ) extends ServiceAgreementStoreError

  final case class FailedToAcceptAgreement(
      domainId: DomainId,
      agreementId: ServiceAgreementId,
      override val description: String,
  ) extends ServiceAgreementStoreError

}
