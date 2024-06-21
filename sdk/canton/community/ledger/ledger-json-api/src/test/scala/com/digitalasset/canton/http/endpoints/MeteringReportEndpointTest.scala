// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.endpoints

import com.daml.ledger.api.v2.admin.metering_report_service
import com.digitalasset.daml.lf.data.Ref.ApplicationId
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.google.protobuf.struct
import com.google.protobuf.struct.Struct
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import scalaz.\/-
import spray.json.enrichAny

import java.time.{Instant, LocalDate}

class MeteringReportEndpointTest extends AnyFreeSpec with Matchers {

  import MeteringReportEndpoint.*

  "MeteringReportEndpoint" - {

    val appX = ApplicationId.assertFromString("appX")

    val request = MeteringReportDateRequest(
      LocalDate.of(2022, 2, 3),
      Some(LocalDate.of(2022, 2, 4)),
      Some(appX),
    )

    "should read/write request" in {
      val actual = request.toJson.convertTo[MeteringReportDateRequest]
      actual shouldBe request
    }

    "should convert to timestamp to protobuf timestamp" in {
      val expected = Timestamp.assertFromInstant(Instant.parse("2022-02-03T00:00:00Z"))
      val actual = toTimestamp(LocalDate.of(2022, 2, 3))
      actual shouldBe expected
    }

    "should convert to protobuf request" in {
      import request.*
      val expected = metering_report_service.GetMeteringReportRequest(
        Some(toPbTimestamp(toTimestamp(from))),
        to.map(toTimestamp).map(toPbTimestamp),
        application.get,
      )
      val actual = MeteringReportEndpoint.toPbRequest(request)
      actual shouldBe expected
    }

    "should convert from protobuf response" in {
      val expected = Struct.of(Map("" -> struct.Value.of(struct.Value.Kind.StringValue("ok"))))
      val response = metering_report_service.GetMeteringReportResponse(
        meteringReportJson = Some(expected)
      )
      val actual = MeteringReportEndpoint.toJsonMeteringReport(response)
      actual shouldBe \/-(expected)
    }

  }

}
