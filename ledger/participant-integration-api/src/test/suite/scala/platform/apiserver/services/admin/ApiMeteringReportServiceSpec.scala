// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import com.daml.error.ErrorCodesVersionSwitcher
import com.daml.ledger.api.v1.admin.metering_report_service.{
  ApplicationMeteringReport,
  GetMeteringReportRequest,
  GetMeteringReportResponse,
  ParticipantMeteringReport,
}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.MeteringStore
import com.daml.ledger.participant.state.index.v2.MeteringStore.TransactionMetering
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

import scala.concurrent.Future

class ApiMeteringReportServiceSpec extends AsyncWordSpec with Matchers with MockitoSugar {

  private val someParticipantId = Ref.ParticipantId.assertFromString("test-participant")
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val switcher = new ErrorCodesVersionSwitcher(true)

  private val appIdA = Ref.ApplicationId.assertFromString("AppA")
  private val appIdB = Ref.ApplicationId.assertFromString("AppB")

  private def build(actionCount: Int, appId: Ref.ApplicationId) = TransactionMetering(
    applicationId = appId,
    actionCount = actionCount,
    meteringTimestamp = Timestamp.Epoch,
    ledgerOffset = Offset.beforeBegin,
  )

  private val metering = Vector(
    build(1, appIdA),
    build(2, appIdB),
    build(3, appIdA),
  )

  "the metering report generator" should {

    "generate report" in {

      val underTest = new MeteringReportGenerator(someParticipantId)

      val request = GetMeteringReportRequest.defaultInstance

      val generationTime = toProtoTimestamp(Timestamp.now())

      val actual = underTest.generate(request, metering, generationTime)

      val expectedReport = ParticipantMeteringReport(
        participantId = someParticipantId,
        toActual = Some(generationTime),
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

    "generate report with optional parameters unset" in {

      val store = mock[MeteringStore]

      val expectedGenTime = toProtoTimestamp(Timestamp.now().addMicros(-1000))

      val underTest =
        new ApiMeteringReportService(someParticipantId, store, switcher, () => expectedGenTime)

      val from = Timestamp(10000)

      val request = GetMeteringReportRequest.defaultInstance.withFrom(toProtoTimestamp(from))

      val expected =
        new MeteringReportGenerator(someParticipantId).generate(request, metering, expectedGenTime)

      when(store.getTransactionMetering(from, None, None)).thenReturn(Future.successful(metering))

      underTest.getMeteringReport(request).map { actual =>
        actual shouldBe expected
      }

    }

    "generate report based with with optional parameters set" in {

      val store = mock[MeteringStore]

      val expectedGenTime = toProtoTimestamp(Timestamp.now().addMicros(-1000))

      val underTest =
        new ApiMeteringReportService(someParticipantId, store, switcher, () => expectedGenTime)

      val from = Timestamp(10000)
      val to = Timestamp(20000)
      val appId = Ref.ApplicationId.assertFromString("AppT")

      val request = GetMeteringReportRequest.defaultInstance
        .withFrom(toProtoTimestamp(from))
        .withTo(toProtoTimestamp(to))
        .withApplicationId(appId)

      val expected =
        new MeteringReportGenerator(someParticipantId).generate(request, metering, expectedGenTime)

      when(store.getTransactionMetering(from, Some(to), Some(appId)))
        .thenReturn(Future.successful(metering))

      underTest.getMeteringReport(request).map { actual =>
        actual shouldBe expected
      }
    }

    "fail if the from timestamp is unset" in {
      val underTest = new ApiMeteringReportService(someParticipantId, mock[MeteringStore], switcher)
      val request = GetMeteringReportRequest.defaultInstance
      underTest.getMeteringReport(request).failed.map { _ => succeed }
    }

  }

}
