// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.meteringreport

import com.daml.ledger.api.v1.admin.metering_report_service._
import com.daml.ledger.participant.state.index.v2.MeteringStore.ReportData
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.ApplicationId
import com.daml.lf.data.Time.Timestamp
import com.daml.platform.apiserver.meteringreport.MeteringReport.{ApplicationReport, _}
import com.google.protobuf.struct.Struct
import com.google.protobuf.timestamp.{Timestamp => ProtoTimestamp}
import scalapb.json4s.JsonFormat
import spray.json.enrichAny
import HmacSha256.Key

class MeteringReportGenerator(participantId: Ref.ParticipantId, key: Key) {

  def generate(
      request: GetMeteringReportRequest,
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[Ref.ApplicationId],
      reportData: ReportData,
      generationTime: ProtoTimestamp,
  ): Either[String, GetMeteringReportResponse] = {

    genMeteringReportJson(from, to, applicationId, reportData).map { reportJson =>
      GetMeteringReportResponse(
        request = Some(request),
        participantReport = Some(genParticipantReport(reportData)),
        reportGenerationTime = Some(generationTime),
        meteringReportJson = Some(reportJson),
      )
    }

  }

  // Note that this will be removed once downstream consumers no longer need it
  private def genMeteringReportJson(
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[ApplicationId],
      reportData: ReportData,
  ): Either[String, Struct] = {

    val applicationReports = reportData.applicationData.toList
      .sortBy(_._1)
      .map((ApplicationReport.apply _).tupled)

    val report: ParticipantReport = ParticipantReport(
      participant = participantId,
      request = Request(from, to, applicationId),
      `final` = reportData.isFinal,
      applications = applicationReports,
      check = None,
    )

    JcsSigner.sign(report, key).map { signedReport =>
      JsonFormat.parser.fromJsonString[Struct](signedReport.toJson.compactPrint)
    }
  }

  private def genParticipantReport(reportData: ReportData) = {
    val applicationMeteringReports = reportData.applicationData.toList
      .sortBy(_._1)
      .map((ApplicationMeteringReport.apply _).tupled)

    ParticipantMeteringReport(
      participantId,
      isFinal = reportData.isFinal,
      applicationMeteringReports,
    )
  }
}
