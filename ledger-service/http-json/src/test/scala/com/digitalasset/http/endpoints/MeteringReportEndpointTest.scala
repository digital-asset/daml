// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package endpoints

import com.daml.ledger.api.v1.admin.metering_report_service
import com.daml.lf.data.Ref.{ApplicationId, ParticipantId}
import com.daml.lf.data.Time.Timestamp
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import scalaz.\/-
import spray.json.enrichAny

class MeteringReportEndpointTest extends AnyFreeSpec with Matchers {

  import MeteringReportEndpoint._

  "MeteringReportEndpoint" - {

    val appX = ApplicationId.assertFromString("appX")

    val request = MeteringReportRequest(
      Timestamp.assertFromString("2022-02-03T14:00:00Z"),
      Some(Timestamp.assertFromString("2022-02-03T15:00:00Z")),
      Some(appX),
    )

    val report = MeteringReport(
      ParticipantId.assertFromString("Part"),
      request.copy(to = None),
      `final` = true,
      Seq(ApplicationMeteringReport(appX, 63)),
    )

    "should read/write request" in {
      val actual = request.toJson.convertTo[MeteringReportRequest]
      actual shouldBe request
    }

    "should read/write report" in {
      val json = report.toJson
      val actual = json.convertTo[MeteringReport]
      actual shouldBe report
    }

    "should convert to timestamp to protobuf timestamp" in {
      val expected = Timestamp.assertFromString("2022-02-03T14:00:00Z")
      val actual = toTimestamp(toPbTimestamp(expected))
      actual shouldBe expected
    }

    "should convert to protobuf request" in {
      import request._
      val expected = metering_report_service.GetMeteringReportRequest(
        Some(toPbTimestamp(from)),
        to.map(toPbTimestamp),
        application.get,
      )
      val actual = MeteringReportEndpoint.toPbRequest(request)
      actual shouldBe expected
    }

    "should convert from protobuf response" in {
      val expected = \/-(report)
      val response = metering_report_service.GetMeteringReportResponse(
        request = Some(MeteringReportEndpoint.toPbRequest(report.request)),
        participantReport = Some(
          metering_report_service.ParticipantMeteringReport(
            participantId = report.participant,
            isFinal = report.`final`,
            applicationReports = report.applications.map(r =>
              metering_report_service.ApplicationMeteringReport(r.application, r.events)
            ),
          )
        ),
        reportGenerationTime = None,
      )
      val actual = MeteringReportEndpoint.toMeteringReport(response)
      actual shouldBe expected
    }

  }

}
