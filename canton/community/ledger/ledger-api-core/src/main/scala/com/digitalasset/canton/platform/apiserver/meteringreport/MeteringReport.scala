// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.meteringreport

import com.daml.lf.data.Ref.{ApplicationId, ParticipantId}
import com.daml.lf.data.Time.Timestamp
import spray.json.{DefaultJsonProtocol, RootJsonFormat, *}

import DefaultJsonProtocol.*

object MeteringReport {

  type Scheme = String

  // These classes must use field names that match the Json fields described at
  // https://docs.daml.com/2.0.0/ops/metering.html

  final case class Check(scheme: Scheme, digest: String)

  final case class ParticipantReport(
      participant: ParticipantId,
      request: Request,
      `final`: Boolean,
      applications: Seq[ApplicationReport],
      check: Option[Check],
  )

  final case class Request(
      from: Timestamp,
      to: Option[Timestamp],
      application: Option[ApplicationId],
  )

  final case class ApplicationReport(application: ApplicationId, events: Long)

  implicit val TimestampFormat: RootJsonFormat[Timestamp] =
    stringJsonFormat(Timestamp.fromString(_))(_.toString)

  implicit val ApplicationIdFormat: RootJsonFormat[ApplicationId] =
    stringJsonFormat(ApplicationId.fromString)(identity)

  implicit val ParticipantIdFormat: RootJsonFormat[ParticipantId] =
    stringJsonFormat(ParticipantId.fromString)(identity)

  implicit val RequestFormat: RootJsonFormat[Request] =
    jsonFormat3(Request.apply)

  implicit val ApplicationReportFormat: RootJsonFormat[ApplicationReport] =
    jsonFormat2(ApplicationReport.apply)

  implicit val CheckFormat: RootJsonFormat[Check] =
    jsonFormat2(Check.apply)

  implicit val ParticipantReportFormat: RootJsonFormat[ParticipantReport] =
    jsonFormat5(ParticipantReport.apply)

  private def stringJsonFormat[A](readFn: String => Either[String, A])(
      writeFn: A => String
  ): RootJsonFormat[A] = new RootJsonFormat[A] {
    private[this] val base = implicitly[JsonFormat[String]]
    override def write(obj: A): JsValue = base.write(writeFn(obj))
    override def read(json: JsValue): A =
      readFn(base.read(json)).fold(deserializationError(_), identity)
  }
}
