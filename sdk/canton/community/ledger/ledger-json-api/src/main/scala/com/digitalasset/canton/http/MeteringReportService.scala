// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.v2.admin.metering_report_service.{GetMeteringReportRequest, GetMeteringReportResponse}
import com.daml.logging.LoggingContextOf
import com.digitalasset.canton.http.util.Logging.{InstanceUUID, RequestID}

import scala.concurrent.Future

class MeteringReportService(getMeteringReportFn: LedgerClientJwt.GetMeteringReport) {

  def getMeteringReport(jwt: Jwt, request: GetMeteringReportRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[GetMeteringReportResponse] = {
    getMeteringReportFn(jwt, request)(lc)
  }

}
