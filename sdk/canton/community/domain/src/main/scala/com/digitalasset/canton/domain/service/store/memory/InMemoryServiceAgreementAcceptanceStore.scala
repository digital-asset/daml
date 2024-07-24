// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.service.store.memory

import com.digitalasset.canton.common.domain.ServiceAgreementId
import com.digitalasset.canton.domain.service.ServiceAgreementAcceptance
import com.digitalasset.canton.domain.service.store.ServiceAgreementAcceptanceStore
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class InMemoryServiceAgreementAcceptanceStore(protected val loggerFactory: NamedLoggerFactory)
    extends ServiceAgreementAcceptanceStore
    with NamedLogging {

  private val acceptances =
    TrieMap.empty[(ServiceAgreementId, ParticipantId), ServiceAgreementAcceptance]

  override def insertAcceptance(acceptance: ServiceAgreementAcceptance)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
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

    Future.unit
  }

  override def listAcceptances()(implicit
      traceContext: TraceContext
  ): Future[Seq[ServiceAgreementAcceptance]] =
    Future.successful(acceptances.values.toSeq)
}
