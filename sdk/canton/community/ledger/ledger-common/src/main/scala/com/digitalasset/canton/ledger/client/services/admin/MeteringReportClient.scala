// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.admin

import com.daml.ledger.api.v2.admin.metering_report_service.MeteringReportServiceGrpc.MeteringReportServiceStub
import com.daml.ledger.api.v2.admin.metering_report_service.{
  GetMeteringReportRequest,
  GetMeteringReportResponse,
}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

final class MeteringReportClient(service: MeteringReportServiceStub) {

  def getMeteringReport(
      request: GetMeteringReportRequest,
      token: Option[String] = None,
  )(implicit traceContext: TraceContext): Future[GetMeteringReportResponse] =
    LedgerClient
      .stubWithTracing(service, token)
      .getMeteringReport(request)

}
