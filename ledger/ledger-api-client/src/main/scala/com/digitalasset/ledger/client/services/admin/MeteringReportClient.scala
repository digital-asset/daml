// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.admin

import com.daml.ledger.api.v1.admin.metering_report_service.MeteringReportServiceGrpc.MeteringReportServiceStub
import com.daml.ledger.api.v1.admin.metering_report_service.{
  GetMeteringReportRequest,
  GetMeteringReportResponse,
}
import com.daml.ledger.client.LedgerClient

import scala.concurrent.Future

final class MeteringReportClient(service: MeteringReportServiceStub) {

  def getMeteringReport(
      request: GetMeteringReportRequest,
      token: Option[String] = None,
  ): Future[GetMeteringReportResponse] =
    LedgerClient
      .stub(service, token)
      .getMeteringReport(request)

}
