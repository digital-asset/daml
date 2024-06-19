// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json2

import com.daml.ledger.api.v2.admin.metering_report_service
import com.digitalasset.canton.ledger.client.services.admin.{MeteringReportClient}
import com.digitalasset.canton.http.json2.JsSchema.DirectScalaPbRwImplicits.*
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.jsonBody
import com.google.protobuf

import scala.concurrent.{ExecutionContext, Future}

class JsMeteringService(meteringReportClient: MeteringReportClient) extends Endpoints {
  import JsMeteringServiceCodecs.*
  private lazy val metering = baseEndpoint.in("metering")

  private val report =
    metering
      .in("report")
      .in(path[String]("application-id"))
      .in(headers)
      .description(
        "Metering report that provides information necessary for billing participant and application operators."
      )
      .mapIn(traceHeadersMapping[String]())
      .in(query[protobuf.timestamp.Timestamp]("from"))
      .in(query[Option[protobuf.timestamp.Timestamp]]("to"))
      .out(jsonBody[metering_report_service.GetMeteringReportResponse])
      .serverSecurityLogicSuccess(Future.successful)
      .serverLogic {
        caller =>
          { input =>
            val from = Some(input._2)
            val to = input._3
            val req = metering_report_service.GetMeteringReportRequest(
              from = from,
              to = to,
              applicationId = input._1.in,
            )

            val resp = meteringReportClient
              .getMeteringReport(req, caller.token())(input._1.traceContext)
            resp.toRight.transform(handleErrorResponse)(ExecutionContext.parasitic)
          }
      }

  def endpoints() = List(
    report
  )

}

object JsMeteringServiceCodecs {
  implicit val getMeteringReportRequest: Codec[metering_report_service.GetMeteringReportRequest] =
    deriveCodec

  implicit val getMeteringReportResponse: Codec[metering_report_service.GetMeteringReportResponse] =
    deriveCodec
}
