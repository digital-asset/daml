// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.ledger.api.v2.admin.metering_report_service.GetMeteringReportRequest
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.tracing.NoOpTelemetry
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.ReportData
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.apiserver.meteringreport.HmacSha256.{Bytes, Key}
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportGenerator
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey.{
  CommunityKey,
  EnterpriseKey,
}
import com.digitalasset.canton.platform.apiserver.services.admin.ApiMeteringReportService.toProtoTimestamp
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.wordspec.AsyncWordSpec

import java.time.temporal.ChronoUnit
import java.time.{OffsetDateTime, ZoneOffset}
import scala.concurrent.Future

class ApiMeteringReportServiceSpec
    extends AsyncWordSpec
    with MockitoSugar
    with ArgumentMatchersSugar
    with BaseTest {

  private val someParticipantId = Ref.ParticipantId.assertFromString("test-participant")

  private val appIdA = Ref.ApplicationId.assertFromString("AppA")
  private val appIdB = Ref.ApplicationId.assertFromString("AppB")

  private val reportData =
    ReportData(applicationData = Map(appIdB -> 2, appIdA -> 4), isFinal = false)

  "the metering report service" should {

    val fromUtc = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS)
    val from = Timestamp.assertFromInstant(fromUtc.toInstant)
    val to = Timestamp.assertFromInstant(fromUtc.plusHours(1).toInstant)

    "generate report with optional parameters unset" in {

      val store = mock[MeteringStore]

      val expectedGenTime = toProtoTimestamp(Timestamp.now().addMicros(-1000))

      val underTest =
        new ApiMeteringReportService(
          someParticipantId,
          store,
          CommunityKey,
          NoOpTelemetry,
          loggerFactory,
          () => expectedGenTime,
        )

      val request = GetMeteringReportRequest.defaultInstance.withFrom(toProtoTimestamp(from))

      val expected =
        new MeteringReportGenerator(someParticipantId, CommunityKey.key).generate(
          request,
          from,
          None,
          None,
          reportData,
          expectedGenTime,
        )

      when(
        store.getMeteringReportData(eqTo(from), eqTo(None), eqTo(None))(
          any[LoggingContextWithTrace]
        )
      )
        .thenReturn(Future.successful(reportData))

      underTest.getMeteringReport(request).map { actual =>
        expected.fold(_ => fail(), actual shouldBe _)
      }

    }

    "generate report with with optional parameters set" in {

      val store = mock[MeteringStore]

      val expectedGenTime = toProtoTimestamp(Timestamp.now().addMicros(-1000))

      val underTest =
        new ApiMeteringReportService(
          someParticipantId,
          store,
          CommunityKey,
          NoOpTelemetry,
          loggerFactory,
          () => expectedGenTime,
        )

      val appId = Ref.ApplicationId.assertFromString("AppT")

      val request = GetMeteringReportRequest.defaultInstance
        .withFrom(toProtoTimestamp(from))
        .withTo(toProtoTimestamp(to))
        .withApplicationId(appId)

      val expected =
        new MeteringReportGenerator(someParticipantId, CommunityKey.key).generate(
          request,
          from,
          Some(to),
          Some(appId),
          reportData,
          expectedGenTime,
        )

      when(
        store.getMeteringReportData(eqTo(from), eqTo(Some(to)), eqTo(Some(appId)))(
          any[LoggingContextWithTrace]
        )
      )
        .thenReturn(Future.successful(reportData))

      underTest.getMeteringReport(request).map { actual =>
        expected.fold(_ => fail(), actual shouldBe _)
      }
    }

    "fail if the from timestamp is unset" in {
      val underTest =
        new ApiMeteringReportService(
          someParticipantId,
          mock[MeteringStore],
          CommunityKey,
          NoOpTelemetry,
          loggerFactory,
        )
      val request = GetMeteringReportRequest.defaultInstance
      underTest.getMeteringReport(request).failed.map { _ => succeed }
    }

    "fail if the from timestamp is not aligned with an hour boundary" in {
      val underTest =
        new ApiMeteringReportService(
          someParticipantId,
          mock[MeteringStore],
          CommunityKey,
          NoOpTelemetry,
          loggerFactory,
        )

      val nonBoundaryFrom = Timestamp.assertFromInstant(fromUtc.plusSeconds(1).toInstant)

      val request = GetMeteringReportRequest.defaultInstance
        .withFrom(toProtoTimestamp(nonBoundaryFrom))

      underTest.getMeteringReport(request).failed.map { _ => succeed }
    }

    "fail an invalid key is passed is provided" in {
      val underTest =
        new ApiMeteringReportService(
          someParticipantId,
          mock[MeteringStore],
          EnterpriseKey(Key("bad", Bytes(Array.empty[Byte]), "bad")),
          NoOpTelemetry,
          loggerFactory,
        )
      val request = GetMeteringReportRequest.defaultInstance
      underTest.getMeteringReport(request).failed.map { _ => succeed }
    }

  }

}
