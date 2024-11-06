// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.admin.metering_report_service
import com.digitalasset.canton.ledger.client.services.admin.MeteringReportClient
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.jsonBody
import com.google.protobuf

import scala.concurrent.ExecutionContext

class JsMeteringService(
    meteringReportClient: MeteringReportClient,
    val loggerFactory: NamedLoggerFactory,
) extends Endpoints {

  import JsMeteringService.*

  private val report = withServerLogic(
    reportEndpoint,
    (caller: Endpoints.CallerContext) =>
      (input: Endpoints.TracedInput[
        (String, protobuf.timestamp.Timestamp, Option[protobuf.timestamp.Timestamp])
      ]) => {
        val (applicationId, from, to) = input.in
        val req = metering_report_service.GetMeteringReportRequest(
          from = Some(from),
          to = to,
          applicationId = applicationId,
        )
        val resp = meteringReportClient
          .getMeteringReport(req, caller.token())(input.traceContext)
        resp.resultToRight.transform(handleErrorResponse(input.traceContext))(
          ExecutionContext.parasitic
        )
      },
  )

  def endpoints() = List(
    report
  )

}

object JsMeteringService {
  import Endpoints.*
  import JsMeteringServiceCodecs.*

  private lazy val metering = v2Endpoint.in("metering")
  val reportEndpoint =
    metering
      .in("report")
      .in(path[String]("application-id"))
      .in(query[protobuf.timestamp.Timestamp]("from"))
      .in(query[Option[protobuf.timestamp.Timestamp]]("to"))
      .out(jsonBody[metering_report_service.GetMeteringReportResponse])
      .description(
        "Metering report that provides information necessary for billing participant and application operators."
      )

}
object JsMeteringServiceCodecs {
  implicit val getMeteringReportRequest: Codec[metering_report_service.GetMeteringReportRequest] =
    deriveCodec

  implicit val getMeteringReportResponse: Codec[metering_report_service.GetMeteringReportResponse] =
    deriveCodec
}
