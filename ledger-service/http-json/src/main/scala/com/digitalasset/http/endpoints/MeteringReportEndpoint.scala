// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package endpoints

import akka.http.scaladsl.model.HttpRequest
import com.daml.http.Endpoints.ET
import com.daml.http.EndpointsCompanion.{Error, ServerError}
import com.daml.http.endpoints.MeteringReportEndpoint.{MeteringReportDateRequest, toPbRequest}
import com.daml.http.util.Logging.{InstanceUUID, RequestID}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.v1.admin.metering_report_service
import com.daml.lf.data.Ref.ApplicationId
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContextOf
import com.google.protobuf
import com.google.protobuf.struct.Struct
import scalaz.\/
import scalaz.std.scalaFuture._
import spray.json._

import java.time.{Instant, LocalDate, LocalTime, ZoneOffset}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

private[http] object MeteringReportEndpoint {

  case class MeteringReportDateRequest(
      from: LocalDate,
      to: Option[LocalDate],
      application: Option[ApplicationId],
  )

  import DefaultJsonProtocol._
  import json.JsonProtocol.xemapStringJsonFormat

  implicit val LocalDateFormat: RootJsonFormat[LocalDate] =
    xemapStringJsonFormat(s => Try(LocalDate.parse(s)).toEither.left.map(_.getMessage))(_.toString)

  implicit val ApplicationIdFormat: RootJsonFormat[ApplicationId] =
    xemapStringJsonFormat(ApplicationId.fromString)(identity)

  implicit val MeteringReportDateRequestFormat: RootJsonFormat[MeteringReportDateRequest] =
    jsonFormat3(MeteringReportDateRequest.apply)

  private val startOfDay = LocalTime.of(0, 0, 0)

  private[endpoints] def toTimestamp(ts: LocalDate): Timestamp = {
    Timestamp.assertFromInstant(Instant.ofEpochSecond(ts.toEpochSecond(startOfDay, ZoneOffset.UTC)))
  }

  private[endpoints] def toPbTimestamp(ts: Timestamp): protobuf.timestamp.Timestamp = {
    val instant = ts.toInstant
    protobuf.timestamp.Timestamp(instant.getEpochSecond, instant.getNano)
  }

  private[endpoints] def toPbRequest(
      request: MeteringReportDateRequest
  ): metering_report_service.GetMeteringReportRequest = {
    import request._
    metering_report_service.GetMeteringReportRequest(
      from = Some(toPbTimestamp(toTimestamp(request.from))),
      to = to.map(toTimestamp).map(toPbTimestamp),
      applicationId = application.fold(
        metering_report_service.GetMeteringReportRequest.defaultInstance.applicationId
      )(identity),
    )
  }

  private def mustHave[T](option: Option[T], field: String): Either[String, T] = option match {
    case Some(t) => Right(t)
    case None => Left(s"GetMeteringReportResponse missing field, expected $field")
  }

  private[endpoints] def toJsonMeteringReport(
      pbResponse: metering_report_service.GetMeteringReportResponse
  ): Error \/ Struct = {
    val jsonReport = mustHave(pbResponse.meteringReportJson, "meteringReportJson")
    import scalaz.syntax.std.either._
    jsonReport.disjunction.leftMap(ServerError.fromMsg)
  }

}

class MeteringReportEndpoint(routeSetup: RouteSetup, service: MeteringReportService)(implicit
    ec: ExecutionContext
) {

  import routeSetup._

  def generateReportResponse(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[Struct]] = {
    proxyWithCommand(generateReport)(req)
      .map[domain.SyncResponse[Struct]](domain.OkResponse(_))
  }

  def generateReport(jwt: Jwt, dateRequest: MeteringReportDateRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[Error \/ Struct] = {
    service
      .getMeteringReport(jwt, toPbRequest(dateRequest))
      .map(MeteringReportEndpoint.toJsonMeteringReport)
  }

}
