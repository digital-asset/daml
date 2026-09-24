// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.services

import com.daml.ledger.api.v2.transaction_filter.TransactionShape.{
  TRANSACTION_SHAPE_ACS_DELTA,
  TRANSACTION_SHAPE_LEDGER_EFFECTS,
}
import com.daml.ledger.api.v2.transaction_filter.{TransactionFormat, TransactionShape, UpdateFormat}
import com.daml.ledger.api.v2.update_service.{
  GetUpdatesRequest,
  GetUpdatesResponse,
  UpdateServiceGrpc,
}
import com.digitalasset.canton.ledger.api.benchtool.AuthorizationHelper
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig
import com.digitalasset.canton.ledger.api.benchtool.util.ObserverWithResult
import io.grpc.Channel
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

final class UpdateService(
    channel: Channel,
    authorizationToken: Option[String],
) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val service: UpdateServiceGrpc.UpdateServiceStub =
    AuthorizationHelper.maybeAuthedService(authorizationToken)(UpdateServiceGrpc.stub(channel))

  def transactions[Result](
      config: WorkflowConfig.StreamConfig.TransactionsStreamConfig,
      observer: ObserverWithResult[GetUpdatesResponse, Result],
  ): Future[Result] =
    transactionsWithoutResult(config, observer) match {
      case Failure(exception) => Future.failed(exception)
      case Success(()) => observer.result
    }

  def transactionsWithoutResult(
      config: WorkflowConfig.StreamConfig.TransactionsStreamConfig,
      observer: StreamObserver[GetUpdatesResponse],
  ): Try[Unit] = getUpdatesRequest(
    filters = config.filters,
    transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
    beginOffsetExclusive = config.beginOffsetExclusive,
    endOffsetInclusive = config.endOffsetInclusive,
  ) match {
    case Right(request) =>
      service.getUpdates(request, observer)
      logger.info("Started fetching transactions")
      Success(())
    case Left(error) =>
      Failure(new RuntimeException(error))
  }

  def transactionsLedgerEffects[Result](
      config: WorkflowConfig.StreamConfig.TransactionLedgerEffectsStreamConfig,
      observer: ObserverWithResult[
        GetUpdatesResponse,
        Result,
      ],
  ): Future[Result] =
    getUpdatesRequest(
      filters = config.filters,
      beginOffsetExclusive = config.beginOffsetExclusive,
      endOffsetInclusive = config.endOffsetInclusive,
      transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
    ) match {
      case Right(request) =>
        service.getUpdates(request, observer)
        logger.info("Started fetching ledger effects transactions")
        observer.result
      case Left(error) =>
        Future.failed(new RuntimeException(error))
    }

  private def getUpdatesRequest(
      filters: List[WorkflowConfig.StreamConfig.PartyFilter],
      transactionShape: TransactionShape,
      beginOffsetExclusive: Long,
      endOffsetInclusive: Option[Long],
  ): Either[String, GetUpdatesRequest] =
    StreamFilters
      .eventFormat(filters)
      .map { eventFormat =>
        GetUpdatesRequest.defaultInstance
          .withBeginExclusive(beginOffsetExclusive)
          .withUpdateFormat(
            UpdateFormat(
              includeTransactions = Some(
                TransactionFormat(
                  eventFormat = Some(eventFormat),
                  transactionShape = transactionShape,
                )
              ),
              includeReassignments = None,
              includeTopologyEvents = None,
            )
          )
          .update(
            _.optionalEndInclusive := endOffsetInclusive
          )
      }

}
