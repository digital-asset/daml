// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.meteringreport

import com.daml.ledger.api.v2.admin.metering_report_service.*
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.ApplicationId
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.daml.struct.spray.StructJsonFormat
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.ReportData
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReport.*
import com.google.protobuf.struct.Struct
import com.google.protobuf.timestamp.Timestamp as ProtoTimestamp
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
      StructJsonFormat.read(signedReport.toJson)
    }
  }

}
