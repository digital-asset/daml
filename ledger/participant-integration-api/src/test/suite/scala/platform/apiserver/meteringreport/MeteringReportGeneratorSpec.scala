// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.meteringreport

import com.daml.ledger.api.v1.admin.metering_report_service.{
  ApplicationMeteringReport,
  GetMeteringReportRequest,
  GetMeteringReportResponse,
  ParticipantMeteringReport,
}
import com.daml.ledger.participant.state.index.v2.MeteringStore.ReportData
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.platform.apiserver.services.admin.ApiMeteringReportService.toProtoTimestamp
import com.google.protobuf.struct.Struct
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalapb.json4s.JsonFormat
import spray.json._

import java.time.Duration
import java.time.temporal.ChronoUnit

class MeteringReportGeneratorSpec extends AsyncWordSpec with Matchers {

  private val someParticipantId = Ref.ParticipantId.assertFromString("test-participant")

  private val appIdA = Ref.ApplicationId.assertFromString("AppA")
  private val appIdB = Ref.ApplicationId.assertFromString("AppB")
  private val appIdX = Ref.ApplicationId.assertFromString("AppX")

  private val from = Timestamp.now()
  private val to = from.add(Duration.of(-1, ChronoUnit.DAYS))

  private val reportData =
    ReportData(applicationData = Map(appIdB -> 2, appIdA -> 4), isFinal = false)

  private val reportJsonStruct = {
    import com.daml.platform.apiserver.meteringreport.MeteringReport._
    val report = ParticipantReport(
      participant = someParticipantId,
      request = Request(from, Some(to), Some(appIdX)),
      `final` = false,
      applications = Seq(ApplicationReport(appIdA, 4), ApplicationReport(appIdB, 2)),
      check = None, // TODO populate
    )
    val json = report.toJson.compactPrint
    val struct: Struct = JsonFormat.parser.fromJsonString[Struct](json)
    struct
  }

  "MeteringReportGenerator" should {
    "generate report" in {

      val underTest = new MeteringReportGenerator(someParticipantId)

      val request = GetMeteringReportRequest.defaultInstance

      val generationTime = toProtoTimestamp(Timestamp.now())

      val actual =
        underTest.generate(request, from, Some(to), Some(appIdX), reportData, generationTime)

      val expectedReport = ParticipantMeteringReport(
        participantId = someParticipantId,
        isFinal = false,
        applicationReports = Seq(
          ApplicationMeteringReport(appIdA, 4),
          ApplicationMeteringReport(appIdB, 2),
        ),
      )

      val expected = GetMeteringReportResponse(
        request = Some(request),
        participantReport = Some(expectedReport),
        reportGenerationTime = Some(generationTime),
        meteringReportJson = Some(reportJsonStruct),
      )

      actual shouldBe expected

    }

  }

}
