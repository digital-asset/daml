// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import com.daml.ledger.api.v1.admin.metering_report_service.{
  ApplicationMeteringReport,
  GetMeteringReportRequest,
  GetMeteringReportResponse,
  ParticipantMeteringReport,
}
import com.daml.ledger.participant.state.index.v2.MeteringStore
import com.daml.ledger.participant.state.index.v2.MeteringStore.ReportData
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.platform.apiserver.services.admin.ApiMeteringReportService.{
  MeteringReportGenerator,
  toProtoTimestamp,
}
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.time.temporal.ChronoUnit
import java.time.{OffsetDateTime, ZoneOffset}
import scala.concurrent.Future

class ApiMeteringReportServiceSpec extends AsyncWordSpec with Matchers with MockitoSugar {

  private val someParticipantId = Ref.ParticipantId.assertFromString("test-participant")
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val appIdA = Ref.ApplicationId.assertFromString("AppA")
  private val appIdB = Ref.ApplicationId.assertFromString("AppB")

  private val reportData =
    ReportData(applicationData = Map(appIdB -> 2, appIdA -> 4), isFinal = false)

  "the metering report generator" should {

    "generate report" in {

      val underTest = new MeteringReportGenerator(someParticipantId)

      val request = GetMeteringReportRequest.defaultInstance

      val generationTime = toProtoTimestamp(Timestamp.now())

      val actual = underTest.generate(request, reportData, generationTime)

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
      )

      actual shouldBe expected

    }

  }

  "the metering report service" should {

    val fromUtc = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS)
    val from = Timestamp.assertFromInstant(fromUtc.toInstant)
    val to = Timestamp.assertFromInstant(fromUtc.plusHours(1).toInstant)

    "generate report with optional parameters unset" in {

      val store = mock[MeteringStore]

      val expectedGenTime = toProtoTimestamp(Timestamp.now().addMicros(-1000))

      val underTest =
        new ApiMeteringReportService(someParticipantId, store, () => expectedGenTime)

      val request = GetMeteringReportRequest.defaultInstance.withFrom(toProtoTimestamp(from))

      val expected =
        new MeteringReportGenerator(someParticipantId).generate(
          request,
          reportData,
          expectedGenTime,
        )

      when(store.getMeteringReportData(from, None, None))
        .thenReturn(Future.successful(reportData))

      underTest.getMeteringReport(request).map { actual =>
        actual shouldBe expected
      }

    }

    "generate report with with optional parameters set" in {

      val store = mock[MeteringStore]

      val expectedGenTime = toProtoTimestamp(Timestamp.now().addMicros(-1000))

      val underTest =
        new ApiMeteringReportService(someParticipantId, store, () => expectedGenTime)

      val appId = Ref.ApplicationId.assertFromString("AppT")

      val request = GetMeteringReportRequest.defaultInstance
        .withFrom(toProtoTimestamp(from))
        .withTo(toProtoTimestamp(to))
        .withApplicationId(appId)

      val expected =
        new MeteringReportGenerator(someParticipantId).generate(
          request,
          reportData,
          expectedGenTime,
        )

      when(store.getMeteringReportData(from, Some(to), Some(appId)))
        .thenReturn(Future.successful(reportData))

      underTest.getMeteringReport(request).map { actual =>
        actual shouldBe expected
      }
    }

    "fail if the from timestamp is unset" in {
      val underTest = new ApiMeteringReportService(someParticipantId, mock[MeteringStore])
      val request = GetMeteringReportRequest.defaultInstance
      underTest.getMeteringReport(request).failed.map { _ => succeed }
    }

    "fail if the from timestamp is not aligned with an hour boundary" in {
      val underTest = new ApiMeteringReportService(someParticipantId, mock[MeteringStore])

      val nonBoundaryFrom = Timestamp.assertFromInstant(fromUtc.plusSeconds(1).toInstant)

      val request = GetMeteringReportRequest.defaultInstance
        .withFrom(toProtoTimestamp(nonBoundaryFrom))

      underTest.getMeteringReport(request).failed.map { _ => succeed }
    }

  }

}
