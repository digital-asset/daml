// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package endpoints

import akka.http.scaladsl.model.HttpRequest
import cats.implicits.toTraverseOps
import com.daml.http.Endpoints.ET
import com.daml.http.EndpointsCompanion.{Error, ServerError}
import com.daml.http.endpoints.MeteringReportEndpoint.{
  MeteringReport,
  MeteringReportDateRequest,
  toMeteringReport,
  toPbRequest,
}
import com.daml.http.util.Logging.{InstanceUUID, RequestID}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.v1.admin.metering_report_service
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{ApplicationId, ParticipantId}
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContextOf
import com.google.protobuf
import scalaz.std.scalaFuture._
import scalaz.\/
import spray.json.{JsValue, JsonFormat, RootJsonFormat, deserializationError, DefaultJsonProtocol}

import java.time.{Instant, LocalDate, LocalTime, ZoneOffset}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

private[http] object MeteringReportEndpoint {

  // These classes must use field names that match the Json fields described at
  // https://docs.daml.com/2.0.0/ops/metering.html

  case class MeteringReportDateRequest(
      from: LocalDate,
      to: Option[LocalDate],
      application: Option[ApplicationId],
  ) {
    def request: MeteringReportRequest = {
      MeteringReportRequest(toTimestamp(from), to.map(toTimestamp), application)
    }
  }

  case class MeteringReportRequest(
      from: Timestamp,
      to: Option[Timestamp],
      application: Option[ApplicationId],
  )

  case class MeteringReport(
      participant: ParticipantId,
      request: MeteringReportRequest,
      `final`: Boolean,
      applications: Seq[ApplicationMeteringReport],
  )

  case class ApplicationMeteringReport(
      application: ApplicationId,
      events: Long,
  )

  import DefaultJsonProtocol._
  val JFS: JsonFormat[String] = implicitly[JsonFormat[String]]

  def stringJsonFormat[A](
      writeFn: A => String,
      readFn: String => Either[String, A],
  ): RootJsonFormat[A] = new RootJsonFormat[A] {
    override def write(obj: A): JsValue = JFS.write(writeFn(obj))
    override def read(json: JsValue): A =
      readFn(JFS.read(json)).fold(e => deserializationError(e), identity)
  }

  implicit val TimestampFormat: RootJsonFormat[Timestamp] =
    stringJsonFormat(_.toString, Timestamp.fromString)

  implicit val LocalDateFormat: RootJsonFormat[LocalDate] = stringJsonFormat(
    _.toString,
    s => Try(LocalDate.parse(s)).toEither.left.map(_.getMessage),
  )

  implicit val ParticipantIdFormat: RootJsonFormat[ParticipantId] =
    stringJsonFormat(_.toString, ParticipantId.fromString)

  implicit val ApplicationIdFormat: RootJsonFormat[ApplicationId] =
    stringJsonFormat(_.toString, ApplicationId.fromString)

  implicit val MeteringReportDateRequestFormat: RootJsonFormat[MeteringReportDateRequest] =
    jsonFormat3(MeteringReportDateRequest.apply)

  implicit val MeteringReportRequestFormat: RootJsonFormat[MeteringReportRequest] =
    jsonFormat3(MeteringReportRequest.apply)

  implicit val ApplicationMeteringReportFormat: RootJsonFormat[ApplicationMeteringReport] =
    jsonFormat2(ApplicationMeteringReport.apply)

  implicit val MeteringReportFormat: RootJsonFormat[MeteringReport] =
    jsonFormat4(MeteringReport.apply)

  private[endpoints] def toTimestamp(pbTimestamp: protobuf.timestamp.Timestamp): Timestamp = {
    Timestamp.assertFromInstant(
      Instant.ofEpochSecond(pbTimestamp.seconds, pbTimestamp.nanos.toLong)
    )
  }

  private val startOfDay = LocalTime.of(0, 0, 0)
  private[endpoints] def toTimestamp(ts: LocalDate): Timestamp = {
    Timestamp.assertFromInstant(Instant.ofEpochSecond(ts.toEpochSecond(startOfDay, ZoneOffset.UTC)))
  }

  private[endpoints] def toPbTimestamp(ts: Timestamp): protobuf.timestamp.Timestamp = {
    val instant = ts.toInstant
    protobuf.timestamp.Timestamp(instant.getEpochSecond, instant.getNano)
  }

  private[endpoints] def toPbRequest(
      request: MeteringReportRequest
  ): metering_report_service.GetMeteringReportRequest = {
    import request._
    metering_report_service.GetMeteringReportRequest(
      from = Some(toPbTimestamp(from)),
      to = to.map(toPbTimestamp),
      applicationId = application.fold(
        metering_report_service.GetMeteringReportRequest.defaultInstance.applicationId
      )(identity),
    )
  }

  // With Protobuf optional string is represented as an empty strings
  private def pbOption(str: String): Option[String] = Option(str).filter(_.nonEmpty)

  private def mustHave[T](option: Option[T], field: String): Either[String, T] = option match {
    case Some(t) => Right(t)
    case None => Left(s"GetMeteringReportResponse missing field, expected $field")
  }

  private[endpoints] def toMeteringReport(
      pbResponse: metering_report_service.GetMeteringReportResponse
  ): Error \/ MeteringReport = {

    val report = for {
      pbParticipantReport <- mustHave(pbResponse.participantReport, "participantReport")
      participantId <- Ref.ParticipantId.fromString(pbParticipantReport.participantId)
      pbRequest <- mustHave(pbResponse.request, "request")
      applicationId <- pbOption(pbRequest.applicationId).traverse(
        Ref.ApplicationId.fromString
      )
      pbFrom <- mustHave(pbRequest.from, "from")
      request = MeteringReportRequest(
        from = toTimestamp(pbFrom),
        to = pbRequest.to.map(toTimestamp),
        application = applicationId,
      )
      applications <- pbParticipantReport.applicationReports
        .traverse { r =>
          ApplicationId.fromString(r.applicationId).map { app =>
            ApplicationMeteringReport(app, r.eventCount)
          }
        }
    } yield MeteringReport(
      participantId,
      request,
      pbParticipantReport.isFinal,
      applications,
    )

    import scalaz.syntax.std.either._
    report.disjunction.leftMap(ServerError.fromMsg)
  }

}

class MeteringReportEndpoint(routeSetup: RouteSetup, service: MeteringReportService)(implicit
    ec: ExecutionContext
) {

  import routeSetup._

  def generateReportResponse(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[MeteringReport]] = {
    proxyWithCommand(generateReport)(req)
      .map[domain.SyncResponse[MeteringReport]](domain.OkResponse(_))
  }

  def generateReport(jwt: Jwt, dateRequest: MeteringReportDateRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[Error \/ MeteringReport] = {
    service.getMeteringReport(jwt, toPbRequest(dateRequest.request)).map(toMeteringReport)
  }

}
