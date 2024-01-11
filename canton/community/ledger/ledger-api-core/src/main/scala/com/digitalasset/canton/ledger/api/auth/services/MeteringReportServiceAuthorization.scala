// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v1.admin.metering_report_service.MeteringReportServiceGrpc.MeteringReportService
import com.daml.ledger.api.v1.admin.metering_report_service.{
  GetMeteringReportRequest,
  GetMeteringReportResponse,
  MeteringReportServiceGrpc,
}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class MeteringReportServiceAuthorization(
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
