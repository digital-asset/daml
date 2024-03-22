// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.http.util.Logging.{InstanceUUID, RequestID}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.v1.admin.metering_report_service.{
  GetMeteringReportRequest,
  GetMeteringReportResponse,
}
import com.daml.logging.LoggingContextOf

import scala.concurrent.Future

class MeteringReportService(getMeteringReportFn: LedgerClientJwt.GetMeteringReport) {

  def getMeteringReport(jwt: Jwt, request: GetMeteringReportRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[GetMeteringReportResponse] = {
    getMeteringReportFn(jwt, request)(lc)
  }

}
