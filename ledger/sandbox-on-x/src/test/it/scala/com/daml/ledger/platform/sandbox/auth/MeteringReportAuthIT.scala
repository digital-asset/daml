// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package platform.sandbox.auth

import com.daml.ledger.api.v1.admin.metering_report_service.{
  GetMeteringReportRequest,
  MeteringReportServiceGrpc,
}
import com.google.protobuf.timestamp.Timestamp

import scala.concurrent.Future

final class MeteringReportAuthIT extends AdminServiceCallAuthTests {

  override def serviceCallName: String = "MeteringReportService#GetMeteringReport"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    stub(MeteringReportServiceGrpc.stub(channel), token)
      .getMeteringReport(
        GetMeteringReportRequest.defaultInstance.withFrom(Timestamp.defaultInstance)
      )

}
