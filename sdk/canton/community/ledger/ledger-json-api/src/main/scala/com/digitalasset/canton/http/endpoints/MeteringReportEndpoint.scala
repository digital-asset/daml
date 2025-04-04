// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.endpoints

import com.daml.jwt.Jwt
import com.daml.ledger.api.v2.admin.metering_report_service
import com.daml.logging.LoggingContextOf
import com.digitalasset.canton.http.Endpoints.ET
import com.digitalasset.canton.http.EndpointsCompanion.{Error, ServerError}
import com.digitalasset.canton.http.endpoints.MeteringReportEndpoint.{
  MeteringReportDateRequest,
  toPbRequest,
}
import com.digitalasset.canton.http.util.Logging.{InstanceUUID, RequestID}
import com.digitalasset.canton.http.{MeteringReportService, OkResponse, SyncResponse}
import com.digitalasset.daml.lf.data.Ref.UserId
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.google.protobuf
import com.google.protobuf.struct.Struct
import scalaz.EitherT.eitherT
import scalaz.\/
import scalaz.std.scalaFuture.*
import spray.json.*

import java.time.{Instant, LocalDate, LocalTime, ZoneOffset}
import scala.concurrent.ExecutionContext
import scala.util.Try

object MeteringReportEndpoint {

  final case class MeteringReportDateRequest(
      from: LocalDate,
      to: Option[LocalDate],
      user: Option[UserId],
  )

  import DefaultJsonProtocol.*
  import com.digitalasset.canton.http.json.JsonProtocol.xemapStringJsonFormat

  implicit val LocalDateFormat: RootJsonFormat[LocalDate] =
    xemapStringJsonFormat(s => Try(LocalDate.parse(s)).toEither.left.map(_.getMessage))(_.toString)

  implicit val UserIdFormat: RootJsonFormat[UserId] =
    xemapStringJsonFormat(UserId.fromString)(identity)

  implicit val MeteringReportDateRequestFormat: RootJsonFormat[MeteringReportDateRequest] =
    jsonFormat3(MeteringReportDateRequest.apply)

  private val startOfDay = LocalTime.of(0, 0, 0)

  private[endpoints] def toTimestamp(ts: LocalDate): Timestamp =
    Timestamp.assertFromInstant(Instant.ofEpochSecond(ts.toEpochSecond(startOfDay, ZoneOffset.UTC)))

  private[endpoints] def toPbTimestamp(ts: Timestamp): protobuf.timestamp.Timestamp = {
    val instant = ts.toInstant
    protobuf.timestamp.Timestamp(instant.getEpochSecond, instant.getNano)
  }

  private[endpoints] def toPbRequest(
      request: MeteringReportDateRequest
  ): metering_report_service.GetMeteringReportRequest = {
    import request.*
    metering_report_service.GetMeteringReportRequest(
      from = Some(toPbTimestamp(toTimestamp(request.from))),
      to = to.map(toTimestamp).map(toPbTimestamp),
      userId = user.fold(
        metering_report_service.GetMeteringReportRequest.defaultInstance.userId
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
    import scalaz.syntax.std.either.*
    jsonReport.disjunction.leftMap(ServerError.fromMsg)
  }

}

class MeteringReportEndpoint(service: MeteringReportService)(implicit
    ec: ExecutionContext
) {

  def generateReport(jwt: Jwt, dateRequest: MeteringReportDateRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[SyncResponse[Struct]] = for {
    s <- eitherT(
      service
        .getMeteringReport(jwt, toPbRequest(dateRequest))
        .map(MeteringReportEndpoint.toJsonMeteringReport)
    )
  } yield OkResponse(s)

}
