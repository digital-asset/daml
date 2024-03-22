// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.v1.admin.metering_report_service.MeteringReportServiceGrpc.MeteringReportService
import com.daml.ledger.api.v1.admin.metering_report_service.{
  GetMeteringReportRequest,
  GetMeteringReportResponse,
  MeteringReportServiceGrpc,
}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

private[daml] final class MeteringReportServiceAuthorization(
    protected val service: MeteringReportService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends MeteringReportService
    with ProxyCloseable
    with GrpcApiService {

  override def getMeteringReport(
      request: GetMeteringReportRequest
  ): Future[GetMeteringReportResponse] = {
    authorizer.requireAdminClaims(service.getMeteringReport)(request)
  }

  override def bindService(): ServerServiceDefinition =
    MeteringReportServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()

}
