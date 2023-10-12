// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.service.store.memory

import cats.data.EitherT
import com.digitalasset.canton.common.domain.ServiceAgreementId
import com.digitalasset.canton.domain.service.ServiceAgreementAcceptance
import com.digitalasset.canton.domain.service.store.{
  ServiceAgreementAcceptanceStore,
  ServiceAgreementAcceptanceStoreError,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class InMemoryServiceAgreementAcceptanceStore(protected val loggerFactory: NamedLoggerFactory)(
    implicit ec: ExecutionContext
) extends ServiceAgreementAcceptanceStore
    with NamedLogging {

  private val acceptances =
    TrieMap.empty[(ServiceAgreementId, ParticipantId), ServiceAgreementAcceptance]

  override def insertAcceptance(acceptance: ServiceAgreementAcceptance)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ServiceAgreementAcceptanceStoreError, Unit] = {
    val agreementId = acceptance.agreementId
    val participantId = acceptance.participantId

    acceptances.putIfAbsent((agreementId, participantId), acceptance) match {
      case None =>
        logger.info(s"Participant $participantId accepted agreement $agreementId")
      case Some(currentAcceptance) =>
        logger.debug(
          s"Participant $participantId had already accepted agreement $agreementId at ${currentAcceptance.timestamp}"
        )
    }

    EitherT.rightT(())
  }

  override def listAcceptances()(implicit
      traceContext: TraceContext
  ): EitherT[Future, ServiceAgreementAcceptanceStoreError, Seq[ServiceAgreementAcceptance]] =
    EitherT.rightT[Future, ServiceAgreementAcceptanceStoreError](acceptances.values.toSeq)
}
