// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.services

import com.daml.ledger.api.v2.state_service.{
  GetActiveContractsRequest,
  GetActiveContractsResponse,
  GetLedgerEndRequest,
  StateServiceGrpc,
}
import com.digitalasset.canton.ledger.api.benchtool.AuthorizationHelper
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig
import com.digitalasset.canton.ledger.api.benchtool.util.ObserverWithResult
import io.grpc.Channel
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
final class StateService(
    channel: Channel,
    authorizationToken: Option[String],
) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val service: StateServiceGrpc.StateServiceStub =
    AuthorizationHelper.maybeAuthedService(authorizationToken)(StateServiceGrpc.stub(channel))

  def getActiveContracts[Result](
      config: WorkflowConfig.StreamConfig.ActiveContractsStreamConfig,
      observer: ObserverWithResult[GetActiveContractsResponse, Result],
  )(implicit ec: ExecutionContext): Future[Result] =
    getLedgerEnd().flatMap { offset =>
      getActiveContractsRequest(config, offset) match {
        case Right(request) =>
          service.getActiveContracts(request, observer)
          logger.info("Started fetching active contracts")
          observer.result
        case Left(error) =>
          Future.failed(new RuntimeException(error))
      }
    }

  private def getActiveContractsRequest(
      config: WorkflowConfig.StreamConfig.ActiveContractsStreamConfig,
      activeAt: Long,
  ): Either[String, GetActiveContractsRequest] =
    StreamFilters.eventFormat(config.filters).map { eventFormat =>
      GetActiveContractsRequest.defaultInstance
        .withEventFormat(eventFormat)
        .withActiveAtOffset(activeAt)
    }

  def getLedgerEnd()(implicit ec: ExecutionContext): Future[Long] = for {
    response <- service.getLedgerEnd(new GetLedgerEndRequest())
  } yield response.offset
}
