// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.EitherT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.common.domain.{ServiceAgreement, ServiceAgreementId}
import com.digitalasset.canton.config.CantonRequireTypes.String256M
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.store.ServiceAgreementStore
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

class DbServiceAgreementStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends ServiceAgreementStore
    with DbStore {

  import ServiceAgreementStore.*
  import storage.api.*

  private val processingTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("service-agreement-store")

  private def getAgreementText(
      domainId: DomainId,
      agreementId: ServiceAgreementId,
  ): DbAction.ReadOnly[Option[String256M]] =
    sql"select agreement_text from service_agreements where domain_id = $domainId and agreement_id = $agreementId"
      .as[String256M]
      .headOption

  private def insertAgreement(
      domainId: DomainId,
      agreementId: ServiceAgreementId,
      agreementText: String256M,
  ): DbAction.WriteOnly[Unit] = {
    val insert = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        sqlu"""insert
               /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( service_agreements ( agreement_id, domain_id ) ) */
              into service_agreements values ($domainId, $agreementId, $agreementText)"""
      case _ =>
        sqlu"insert into service_agreements values ($domainId, $agreementId, $agreementText) on conflict do nothing"
    }
    insert.map(_ => ())
  }

  private def insertAcceptedAgreementQuery(
      domainId: DomainId,
      agreementId: ServiceAgreementId,
  ): DbAction.WriteOnly[Unit] = {
    val insertStatement = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        sqlu"""insert
               /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( accepted_agreements ( agreement_id, domain_id ) ) */
              into accepted_agreements values ($domainId, $agreementId)"""
      case _ =>
        sqlu"insert into accepted_agreements values ($domainId, $agreementId) on conflict do nothing"
    }
    insertStatement.map(_ => ())
  }

  private def getAcceptedAgreement(
      domainId: DomainId,
      agreementId: ServiceAgreementId,
  ): DbAction.ReadOnly[Option[ServiceAgreementId]] =
    sql"select agreement_id from accepted_agreements where domain_id = $domainId and agreement_id = $agreementId"
      .as[ServiceAgreementId]
      .headOption

  def containsAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(implicit
      traceContext: TraceContext
  ): Future[Boolean] = processingTime.event {
    storage.querySingle(getAgreementText(domainId, agreementId), functionFullName).isDefined
  }

  def getAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ServiceAgreementStoreError, String256M] =
    processingTime.eitherTEvent {
      storage
        .querySingle(getAgreementText(domainId, agreementId), functionFullName)
        .toRight(UnknownServiceAgreement(domainId, agreementId))
    }

  def storeAgreement(
      domainId: DomainId,
      agreementId: ServiceAgreementId,
      agreementText: String256M,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ServiceAgreementStoreError, Unit] =
    processingTime.eitherTEvent {
      EitherT.right(
        storage.update_(insertAgreement(domainId, agreementId, agreementText), functionFullName)
      )
    }

  def listAgreements(implicit
      traceContext: TraceContext
  ): Future[Seq[(DomainId, ServiceAgreement)]] =
    processingTime.event {
      storage.query(
        sql"select domain_id, agreement_id, agreement_text from service_agreements"
          .as[(DomainId, ServiceAgreement)],
        functionFullName,
      )
    }

  def insertAcceptedAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ServiceAgreementStoreError, Unit] =
    processingTime.eitherTEvent {
      for {
        contains <- EitherT.right(containsAgreement(domainId, agreementId))
        _ <- EitherT.cond[Future](contains, (), UnknownServiceAgreement(domainId, agreementId))
        _ <- EitherT.right(
          storage.update_(insertAcceptedAgreementQuery(domainId, agreementId), functionFullName)
        )
      } yield ()
    }

  def listAcceptedAgreements(
      domainId: DomainId
  )(implicit traceContext: TraceContext): Future[Seq[ServiceAgreementId]] =
    processingTime.event {
      storage.query(
        sql"select agreement_id from accepted_agreements where domain_id = $domainId"
          .as[ServiceAgreementId],
        functionFullName,
      )
    }

  def containsAcceptedAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(implicit
      traceContext: TraceContext
  ): Future[Boolean] = processingTime.event {
    storage.querySingle(getAcceptedAgreement(domainId, agreementId), functionFullName).isDefined
  }
}
