// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import com.daml.ledger.api.v1.admin.metering_report_service.MeteringReportServiceGrpc.MeteringReportService
import com.daml.ledger.api.v1.admin.metering_report_service._
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiMeteringReportService()(implicit
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends MeteringReportService
    with GrpcApiService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  override def bindService(): ServerServiceDefinition =
    MeteringReportServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()

  override def getMeteringReport(
      request: GetMeteringReportRequest
  ): Future[GetMeteringReportResponse] = {
    logger.info(s"Received metering report request: $request")
    val now = java.time.Instant.now()
    val generationTime =
      com.google.protobuf.timestamp.Timestamp.of(now.getEpochSecond, now.getNano)
    val participantReport = ParticipantMeteringReport(
      participantId = "participant1",
      request.to.orElse(Some(generationTime)),
      Seq(
        ApplicationMeteringReport("app1", 100),
        ApplicationMeteringReport("app2", 200),
      ),
    )
    val response =
      GetMeteringReportResponse(Some(request), Some(participantReport), Some(generationTime))
    Future.successful(response).andThen(logger.logErrorsOnCall[GetMeteringReportResponse])
  }

}
